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

    // Hold the lock across decrement and notify to avoid a lost wakeup in Join().
    if (gather != nullptr) {
        std::lock_guard<std::mutex> lock(gather->mutex);
        if (gather->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            gather->cv.notify_one();
        }
    }
}

} // namespace rpc::runtime
