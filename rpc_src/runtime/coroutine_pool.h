#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "runtime/coroutine_routine.h"

namespace rpc::runtime {

class CoroutineScheduler;

class CoroutinePool {
public:
    friend class CoroutineScheduler;

    explicit CoroutinePool(std::size_t max_count);

    // The scheduler and this pool are owner-thread confined. A routine is
    // reusable only after it has finished and no public handle still owns it.
    std::shared_ptr<Coroutine_Routine> Acquire(std::size_t minimum_stack_size);

    // Returns true when the routine was retained by the pool.
    bool Release(std::shared_ptr<Coroutine_Routine> routine);

    std::size_t size() const noexcept { return pool_.size(); }
    std::size_t created_count() const noexcept { return created_count_; }

private:
    std::vector<std::shared_ptr<Coroutine_Routine>> pool_;
    std::size_t max_pool_size_;
    std::size_t created_count_ = 0;
};

} // namespace rpc::runtime
