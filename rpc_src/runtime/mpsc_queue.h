#pragma once

#include <atomic>
#include <cstddef>
#include <type_traits>

namespace rpc::runtime {

template <typename T>
class MpscQueue {
    static_assert(std::is_same_v<decltype(std::declval<T>().next), std::atomic<T *>>,
                  "T must have field: std::atomic<T*> next");

public:
    MpscQueue() = default;

    MpscQueue(const MpscQueue &) = delete;
    MpscQueue &operator=(const MpscQueue &) = delete;

    // Multi-producer, lock-free push.
    void Push(T *node) {
        T *old = head_.load(std::memory_order_relaxed);
        do {
            node->next.store(old, std::memory_order_relaxed);
        } while (!head_.compare_exchange_weak(old, node,
                                              std::memory_order_release,
                                              std::memory_order_relaxed));
    }

    bool TryPop(T *&out) {
        if (drained_ == nullptr) {
            drained_ = TakeAllReversed();
            if (drained_ == nullptr) {
                return false;
            }
        }
        out = drained_;
        drained_ = drained_->next.load(std::memory_order_relaxed);
        out->next.store(nullptr, std::memory_order_relaxed);
        return true;
    }

    bool Empty() const {
        return drained_ == nullptr && head_.load(std::memory_order_acquire) == nullptr;
    }

private:
    T *TakeAllReversed() {
        T *cur = head_.exchange(nullptr, std::memory_order_acquire);
        T *prev = nullptr;
        while (cur != nullptr) {
            T *next = cur->next.load(std::memory_order_relaxed);
            cur->next.store(prev, std::memory_order_relaxed);
            prev = cur;
            cur = next;
        }
        return prev;
    }

    alignas(64) std::atomic<T *> head_{nullptr};
    alignas(64) T *drained_ = nullptr;
};

} // namespace rpc::runtime
