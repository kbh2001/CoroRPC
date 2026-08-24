#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>

#include "task/task.h"

// Task pool with hazard-pointer reclamation.

namespace rpc::memory {
namespace detail {

template <class NodeT>
class HazardDomain {
public:
    static constexpr std::size_t kInactiveBucket = static_cast<std::size_t>(-1);

    struct alignas(64) Record {
        std::atomic<NodeT *> protected_node{nullptr};
        std::atomic<std::size_t> bucket{kInactiveBucket};
        Record *next = nullptr;
    };

    static HazardDomain &Instance() {
        static HazardDomain domain;
        return domain;
    }

    Record *AcquireRecord(std::size_t bucket_index) {
        for (Record *record = records_.load(std::memory_order_acquire);
             record != nullptr; record = record->next) {
            std::size_t inactive = kInactiveBucket;
            if (record->bucket.compare_exchange_strong(
                    inactive, bucket_index, std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                assert(record->protected_node.load(std::memory_order_relaxed) == nullptr);
                return record;
            }
        }

        Record *record = new Record;
        record->bucket.store(bucket_index, std::memory_order_relaxed);
        Record *old = records_.load(std::memory_order_relaxed);
        do {
            record->next = old;
        } while (!records_.compare_exchange_weak(
            old, record, std::memory_order_release, std::memory_order_relaxed));
        return record;
    }

    void ReleaseRecord(Record *record) noexcept {
        record->protected_node.store(nullptr, std::memory_order_seq_cst);
        record->bucket.store(kInactiveBucket, std::memory_order_release);
    }

    bool IsProtected(NodeT *node, std::size_t bucket_index) const noexcept {
        for (Record *record = records_.load(std::memory_order_acquire);
             record != nullptr; record = record->next) {
            if (record->bucket.load(std::memory_order_acquire) == bucket_index &&
                record->protected_node.load(std::memory_order_seq_cst) == node) {
                return true;
            }
        }
        return false;
    }

private:
    HazardDomain() = default;

    ~HazardDomain() {
        Record *record = records_.load(std::memory_order_relaxed);
        while (record != nullptr) {
            Record *next = record->next;
            delete record;
            record = next;
        }
    }

    std::atomic<Record *> records_{nullptr};
};

template <class NodeT>
class HazardOwner {
public:
    using Domain = HazardDomain<NodeT>;
    using Record = typename Domain::Record;

    explicit HazardOwner(std::size_t bucket_index)
        : bucket_index_(bucket_index), record_(Domain::Instance().AcquireRecord(bucket_index)) {}

    ~HazardOwner() {
        Domain::Instance().ReleaseRecord(record_);
    }

    HazardOwner(const HazardOwner &) = delete;
    HazardOwner &operator=(const HazardOwner &) = delete;

    Record *record(std::size_t bucket_index) noexcept {
        (void)bucket_index;
        assert(bucket_index == bucket_index_);
        return record_;
    }

private:
    std::size_t bucket_index_;
    Record *record_;
};

} // namespace detail

template <size_t BufSize = 128>
class TaskPool {
public:
    using TaskT = rpc::runtime::Task<BufSize>;

    TaskPool() = default;

    TaskPool(const TaskPool &) = delete;
    TaskPool &operator=(const TaskPool &) = delete;

    TaskT *Allocate() {
        const std::size_t bucket_index = LocalBucket();
        Bucket &bucket = buckets_[bucket_index];
        HazardRecord *hazard = LocalHazardRecord(bucket_index);

        for (;;) {
            TaskT *task = bucket.head.load(std::memory_order_seq_cst);
            if (task == nullptr) {
                hazard->protected_node.store(nullptr, std::memory_order_seq_cst);

                ReclaimRetired(bucket, bucket_index);
                task = bucket.head.load(std::memory_order_seq_cst);
                if (task == nullptr) {
                    task = new TaskT{};
                    task->pool_bucket = static_cast<std::uint16_t>(bucket_index);
                    return task;
                }
            }

            hazard->protected_node.store(task, std::memory_order_seq_cst);
            if (bucket.head.load(std::memory_order_seq_cst) != task) {
                continue;
            }

            TaskT *next = task->next.load(std::memory_order_acquire);
            if (bucket.head.compare_exchange_strong(
                    task, next, std::memory_order_seq_cst,
                    std::memory_order_seq_cst)) {
                hazard->protected_node.store(nullptr, std::memory_order_seq_cst);
                task->next.store(nullptr, std::memory_order_relaxed);
                return task;
            }
        }
    }

    void Free(TaskT *task) {
        task->Reset();
        const std::size_t bucket_index = task->pool_bucket;
        assert(bucket_index < kBucketCount);
        Bucket &bucket = buckets_[bucket_index];

        PushRetired(bucket, task);
        const std::uint64_t sequence =
            bucket.retire_sequence.fetch_add(1, std::memory_order_relaxed) + 1;
        if (sequence % kReclaimBatch == 0) {
            ReclaimRetired(bucket, bucket_index);
        }
    }

    ~TaskPool() {
        for (Bucket &bucket : buckets_) {
            DeleteList(bucket.head.exchange(nullptr, std::memory_order_relaxed));
            DeleteList(bucket.retired.exchange(nullptr, std::memory_order_relaxed));
        }
    }

private:
    static constexpr std::size_t kBucketCount = 64;
    static constexpr std::uint64_t kReclaimBatch = 32;

    using HazardDomain = detail::HazardDomain<TaskT>;
    using HazardRecord = typename HazardDomain::Record;

    struct alignas(64) Bucket {
        alignas(64) std::atomic<TaskT *> head{nullptr};
        alignas(64) std::atomic<TaskT *> retired{nullptr};
        alignas(64) std::atomic<std::uint64_t> retire_sequence{0};
    };

    static std::size_t LocalBucket() noexcept {
        static std::atomic<std::size_t> next_bucket{0};
        thread_local const std::size_t bucket =
            next_bucket.fetch_add(1, std::memory_order_relaxed) % kBucketCount;
        return bucket;
    }

    static HazardRecord *LocalHazardRecord(std::size_t bucket_index) {
        thread_local detail::HazardOwner<TaskT> owner(bucket_index);
        return owner.record(bucket_index);
    }

    static void PushList(std::atomic<TaskT *> &head, TaskT *task) noexcept {
        TaskT *old = head.load(std::memory_order_relaxed);
        do {
            task->next.store(old, std::memory_order_relaxed);
        } while (!head.compare_exchange_weak(
            old, task, std::memory_order_release, std::memory_order_relaxed));
    }

    static void PushRetired(Bucket &bucket, TaskT *task) noexcept {
        PushList(bucket.retired, task);
    }

    static void PushReusable(Bucket &bucket, TaskT *task) noexcept {
        PushList(bucket.head, task);
    }

    static void ReclaimRetired(Bucket &bucket, std::size_t bucket_index) noexcept {
        TaskT *batch = bucket.retired.exchange(nullptr, std::memory_order_seq_cst);
        while (batch != nullptr) {
            TaskT *next = batch->next.load(std::memory_order_relaxed);
            if (HazardDomain::Instance().IsProtected(batch, bucket_index)) {
                PushRetired(bucket, batch);
            } else {
                PushReusable(bucket, batch);
            }
            batch = next;
        }
    }

    static void DeleteList(TaskT *task) noexcept {
        while (task != nullptr) {
            TaskT *next = task->next.load(std::memory_order_relaxed);
            delete task;
            task = next;
        }
    }

    std::array<Bucket, kBucketCount> buckets_{};
};

template <size_t BufSize = 128>
inline TaskPool<BufSize> &GlobalTaskPool() {
    static TaskPool<BufSize> pool;
    return pool;
}

} // namespace rpc::memory
