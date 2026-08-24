#include "runtime/coroutine_routine.h"

#include <utility>

namespace rpc::runtime {

Coroutine_Routine::Coroutine_Routine(CoroutineScheduler *scheduler, std::uint64_t id, Entry entry,
                                     std::size_t stack_size, SharedStackSlot *shared_stack)
    : TimerNode(TimerNodeType::ROUTINE_WAIT), scheduler_(scheduler), id_(id), entry_(std::move(entry)),
      shared_stack_(shared_stack)
{
    if (shared_stack_ == nullptr)
    {
        owned_stack_ = std::make_unique<CoroutineStack>(stack_size);
    }
}

CoroutineStack &Coroutine_Routine::stack() noexcept
{
    return shared_stack_ == nullptr ? *owned_stack_ : *shared_stack_->stack;
}

const CoroutineStack &Coroutine_Routine::stack() const noexcept
{
    return shared_stack_ == nullptr ? *owned_stack_ : *shared_stack_->stack;
}

} // namespace rpc::runtime
