#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <mutex>
#include <new>
#include <type_traits>
#include <utility>

// Task and Gather support submission from user threads to coroutine threads.

namespace rpc::runtime {

struct Gather;

// The closure is stored inline (SBO). Tasks are recycled by TaskPool, so the
// submission path does not allocate per task. BufSize must be a power of two >= 16.
template <size_t BufSize = 128>
struct Task {
    static_assert(BufSize >= 16 && (BufSize & (BufSize - 1)) == 0,
                  "BufSize must be a power of two >= 16");

    alignas(16) char storage[BufSize];
    void (*invoke)(void *ctx) = nullptr;
    void (*destroy)(void *ctx) = nullptr;

    std::exception_ptr exception;

    Gather *gather = nullptr;

    std::atomic<Task *> next{nullptr};
    std::uint16_t pool_bucket = 0;

    Task() = default;

    // Tasks move between the pool and queue by pointer.
    Task(const Task &) = delete;
    Task &operator=(const Task &) = delete;

    template <class F>
    void SetLambda(F &&f) {
        using FDecay = std::decay_t<F>;
        static_assert(sizeof(FDecay) <= BufSize,
                      "lambda capture too large for Task::storage; "
                      "put large state on the heap and capture a pointer");
        static_assert(alignof(FDecay) <= 16,
                      "lambda alignment exceeds Task::storage alignment");

        new (storage) FDecay(std::forward<F>(f));

        invoke = [](void *ctx) { (*static_cast<FDecay *>(ctx))(); };
        destroy = [](void *ctx) { static_cast<FDecay *>(ctx)->~FDecay(); };
    }

    void Run() {
        if (invoke != nullptr) {
            invoke(storage);
        }
    }

    // Clear state and destroy the closure before returning to the pool.
    void Reset() {
        if (destroy != nullptr) {
            destroy(storage);
            invoke = nullptr;
            destroy = nullptr;
        }
        exception = nullptr;
        gather = nullptr;
        next.store(nullptr, std::memory_order_relaxed);
    }
};

// Completion counter for a batch of tasks. The user thread waits on a condition
// variable while coroutine threads decrement the counter.
struct Gather {
    std::atomic<size_t> remaining{0};
    std::mutex mutex;
    std::condition_variable cv;
    // Keep the first exception for CoScope::Join(); protected by mutex.
    std::exception_ptr first_exception;
};

} // namespace rpc::runtime
