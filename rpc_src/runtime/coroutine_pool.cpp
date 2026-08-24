#include "runtime/coroutine_pool.h"

#include <utility>

namespace rpc::runtime {

CoroutinePool::CoroutinePool(std::size_t max_count) : max_pool_size_(max_count)
{
    pool_.reserve(max_count);
}

std::shared_ptr<Coroutine_Routine> CoroutinePool::Acquire(std::size_t minimum_stack_size)
{
    for (std::size_t index = pool_.size(); index != 0; --index)
    {
        const std::size_t candidate = index - 1;
        const auto &routine = pool_[candidate];
        // One owner is routines_ and one is pool_. Any additional owner is a
        // public RoutineHandle whose identity must remain stable.
        if (routine.use_count() != 2)
        {
            continue;
        }
        if (routine->shared_stack_ == nullptr && routine->stack().usable_size() < minimum_stack_size)
        {
            continue;
        }

        auto acquired = std::move(pool_[candidate]);
        if (candidate + 1 != pool_.size())
        {
            pool_[candidate] = std::move(pool_.back());
        }
        pool_.pop_back();
        return acquired;
    }
    return nullptr;
}

bool CoroutinePool::Release(std::shared_ptr<Coroutine_Routine> routine)
{
    if (!routine || routine->state_ != CoroutineState::FINISHED || routine->queued_ ||
        routine->io_wait_ != nullptr)
    {
        std::terminate();
    }
    if (pool_.size() < max_pool_size_)
    {
        pool_.push_back(std::move(routine));
        return true;
    }
    return false;
}

} // namespace rpc::runtime
