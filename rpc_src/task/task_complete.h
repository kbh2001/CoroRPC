#pragma once

#include <mutex>

#include "memory/task_pool.h"
#include "task/task.h"

// Complete a task on its coroutine thread.

namespace rpc::runtime {

// Propagate exceptions, decrement Gather, and return the task to the pool.
// The task pointer is invalid after this call.
template <size_t BufSize = 128>
inline void CompleteTask(Task<BufSize> *task, rpc::memory::TaskPool<BufSize> &pool) {
    Gather *gather = task->gather;

    // Keep only the first exception.
    if (task->exception != nullptr && gather != nullptr) {
        std::lock_guard<std::mutex> lock(gather->mutex);
        if (!gather->first_exception) {
            gather->first_exception = task->exception;
        }
    }

    // Return the task immediately.
    pool.Free(task);

    // The condition-variable path retains the original mutex protocol. The
    // semaphore path uses an atomic armed/signaled state, so successful task
    // completions do not contend on a mutex just to publish the final event.
    if (gather != nullptr) {
        if (gather->mode == NotifyMode::SpinSemaphore) {
            if (gather->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                BatchWaitState expected = BatchWaitState::Armed;
                if (gather->wait_state.compare_exchange_strong(
                        expected, BatchWaitState::Signaled,
                        std::memory_order_acq_rel, std::memory_order_acquire)) {
                    gather->waiter->Signal();
                }
            }
        } else {
            std::lock_guard<std::mutex> lock(gather->mutex);
            if (gather->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                gather->cv.notify_one();
            }
        }
    }
}

} // namespace rpc::runtime
