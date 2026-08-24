#include "runtime/coroutine_scheduler.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "runtime/coroutine_pool.h"

#if RPC_RUNTIME_HAS_ASAN
#include <sanitizer/asan_interface.h>
#include <sanitizer/common_interface_defs.h>
#endif

namespace rpc::runtime {
namespace {

thread_local CoroutineScheduler *tls_scheduler = nullptr;

#if RPC_RUNTIME_HAS_ASAN
void StartAsanSwitch(void **from_fake_stack, const void *to_stack_bottom, std::size_t to_stack_size,
                     CoroutineState next_state)
{
    // ASan associates a fake stack with the stack being left, not the stack
    // being entered. A finishing routine never resumes, so passing nullptr
    // releases its fake stack as required by the sanitizer interface.
    __sanitizer_start_switch_fiber(next_state == CoroutineState::FINISHED ? nullptr : from_fake_stack,
                                   to_stack_bottom, to_stack_size);
}

void FinishAsanSwitch(void *current_fake_stack, const void **from_stack_bottom, std::size_t *from_stack_size)
{
    __sanitizer_finish_switch_fiber(current_fake_stack, from_stack_bottom, from_stack_size);
}

void UnpoisonSharedStackRange(const void *address, std::size_t size)
{
    __asan_unpoison_memory_region(address, size);
}

void UnpoisonCoroutineStack(const CoroutineStack &stack)
{
    __asan_unpoison_memory_region(stack.usable_bottom(), stack.usable_size());
}
#endif

} // namespace

CoroutineScheduler::CoroutineScheduler() : CoroutineScheduler(Options{})
{
}

CoroutineScheduler::CoroutineScheduler(Options options) : owner_thread_(std::this_thread::get_id())
{
    if (tls_scheduler != nullptr)
    {
        throw std::logic_error("only one CoroutineScheduler is allowed per thread");
    }
    if (options.shared_stack_count != 0)
    {
        if (options.shared_stack_size == 0)
        {
            throw std::invalid_argument("shared coroutine stack size must be non-zero");
        }
        shared_stacks_.reserve(options.shared_stack_count);
        for (std::size_t index = 0; index < options.shared_stack_count; ++index)
        {
            SharedStackSlot slot;
            slot.stack = std::make_unique<CoroutineStack>(options.shared_stack_size);
            shared_stacks_.push_back(std::move(slot));
        }
    }
    if (options.pool_prewarm_count > 0 || options.pool_max_size > 0)
    {
        if (options.pool_stack_size == 0)
        {
            throw std::invalid_argument("pooled coroutine stack size must be non-zero");
        }
        const std::size_t max_size = options.pool_max_size > 0
                                         ? options.pool_max_size
                                         : options.pool_prewarm_count * 2;
        pool_ = std::make_unique<CoroutinePool>(max_size);

        const std::size_t prewarm_count = std::min(options.pool_prewarm_count, max_size);
        for (std::size_t index = 0; index < prewarm_count; ++index)
        {
            auto routine = CreateRaw([] {}, options.pool_stack_size);
            routine->state_ = CoroutineState::FINISHED;
            if (!pool_->Release(routine))
            {
                std::terminate();
            }
            ++pool_->created_count_;
        }
    }
    tls_scheduler = this;
}

CoroutineScheduler::~CoroutineScheduler()
{
    if (call_stack_.size() != 1)
    {
        std::terminate();
    }
    if (tls_scheduler == this)
    {
        tls_scheduler = nullptr;
    }
}

std::shared_ptr<Coroutine_Routine> CoroutineScheduler::Create(Coroutine_Routine::Entry entry, std::size_t stack_size)
{
    CheckOwnerThread();
    if (!entry)
    {
        throw std::invalid_argument("coroutine entry must not be empty");
    }

    std::shared_ptr<Coroutine_Routine> routine;
    if (pool_)
    {
        routine = pool_->Acquire(stack_size);
        if (routine)
        {
            routine->Reset(std::move(entry));
            return routine;
        }
    }

    SharedStackSlot *shared_stack = nullptr;
    if (!shared_stacks_.empty())
    {
        shared_stack = &shared_stacks_[next_shared_stack_++ % shared_stacks_.size()];
    }
    routine = std::make_shared<Coroutine_Routine>(this, next_id_++, std::move(entry), stack_size, shared_stack);
    routines_.emplace(routine->id_, routine);
    if (pool_)
    {
        ++pool_->created_count_;
    }

    return routine;
}

std::shared_ptr<Coroutine_Routine> CoroutineScheduler::CreateRaw(Coroutine_Routine::Entry entry, std::size_t stack_size)
{
    CheckOwnerThread();
    if (!entry)
    {
        throw std::invalid_argument("coroutine entry must not be empty");
    }

    SharedStackSlot *shared_stack = nullptr;
    if (!shared_stacks_.empty())
    {
        shared_stack = &shared_stacks_[next_shared_stack_++ % shared_stacks_.size()];
    }
    auto routine = std::make_shared<Coroutine_Routine>(this, next_id_++, std::move(entry), stack_size, shared_stack);
    routines_.emplace(routine->id_, routine);
    return routine;
}

std::shared_ptr<Coroutine_Routine> CoroutineScheduler::Spawn(Coroutine_Routine::Entry entry, std::size_t stack_size)
{
    auto routine = Create(std::move(entry), stack_size);
    // INIT routines first run in a later RunReady() pass from the root context.
    routine->state_ = CoroutineState::READY;
    EnqueueReady(routine);
    return routine;
}

std::size_t CoroutineScheduler::RunReady(std::size_t max_count)
{
    CheckOwnerThread();
    if (max_count == 0)
    {
        max_count = ready_queue_.size();
    }
    std::size_t executed = 0;
    while (executed < max_count && !ready_queue_.empty())
    {
        std::shared_ptr<Coroutine_Routine> routine = std::move(ready_queue_.front());
        ready_queue_.pop_front();
        routine->queued_ = false;
        Resume(routine);
        ++executed;
    }
    return executed;
}

void CoroutineScheduler::Resume(const std::shared_ptr<Coroutine_Routine> &routine)
{
    CheckOwnerThread();
    if (!routine || routine->scheduler_ != this)
    {
        throw std::invalid_argument("coroutine belongs to a different scheduler");
    }
    if (routine.get() == current_)
    {
        throw std::logic_error("a coroutine cannot resume itself");
    }
    if (routine->queued_)
    {
        throw std::logic_error("a queued coroutine must be resumed by RunReady");
    }
    if (routine->state_ != CoroutineState::INIT && routine->state_ != CoroutineState::READY &&
        routine->state_ != CoroutineState::WAIT_EVENT)
    {
        throw std::logic_error("coroutine is not resumable");
    }
    if (routine->context_.rip == 0)
    {
        InitializeContext(*routine);
    }

    Coroutine_Routine *caller = current_;
    CoroutineContext *caller_context = caller == nullptr ? &root_context_ : &caller->context_;
    const void *caller_stack_pointer = caller == nullptr ? nullptr : coroutine_stack_pointer();
    const SharedStackRestore restore = PrepareSharedStack(*routine, caller, caller_stack_pointer);
#if RPC_RUNTIME_HAS_ASAN
    void **caller_fake_stack = caller == nullptr ? &root_asan_fake_stack_ : &caller->asan_fake_stack_;
#endif
    call_stack_.push_back(routine.get());
    current_ = routine.get();
    routine->state_ = CoroutineState::RUNNING;

#if RPC_RUNTIME_HAS_ASAN
    StartAsanSwitch(caller_fake_stack, routine->stack().usable_bottom(), routine->stack().usable_size(),
                    CoroutineState::RUNNING);
#endif
    coroutine_context_switch(caller_context, &routine->context_, restore.begin, restore.data, restore.size);
#if RPC_RUNTIME_HAS_ASAN
    const void **routine_stack_bottom = &routine->asan_stack_bottom_;
    std::size_t *routine_stack_size = &routine->asan_stack_size_;
    FinishAsanSwitch(caller == nullptr ? root_asan_fake_stack_ : caller->asan_fake_stack_, routine_stack_bottom,
                     routine_stack_size);
    if (caller != nullptr && caller->shared_stack_ != nullptr)
    {
        UnpoisonCoroutineStack(caller->stack());
    }
#endif

    current_ = caller;
    if (routine->state_ == CoroutineState::FINISHED)
    {
        FinishRoutine(routine);
    }
    else if (routine->state_ == CoroutineState::READY)
    {
        EnqueueReady(routine);
    }
}

void CoroutineScheduler::Resume(Coroutine_Routine *routine)
{
    CheckOwnerThread();
    if (routine == nullptr || routine->scheduler_ != this)
    {
        throw std::invalid_argument("coroutine belongs to a different scheduler");
    }
    Resume(FindRoutine(routine));
}

bool CoroutineScheduler::MakeReady(Coroutine_Routine *routine)
{
    CheckOwnerThread();
    if (routine == nullptr || routine->scheduler_ != this)
    {
        return false;
    }
    switch (routine->state_)
    {
    case CoroutineState::WAIT_IO:
    case CoroutineState::WAIT_TIMER:
    case CoroutineState::WAIT_EVENT:
        routine->state_ = CoroutineState::READY;
        EnqueueReady(FindRoutine(routine));
        return true;
    default:
        return false;
    }
}

void CoroutineScheduler::SuspendCurrent(CoroutineState waiting_state)
{
    if (waiting_state != CoroutineState::WAIT_IO && waiting_state != CoroutineState::WAIT_TIMER &&
        waiting_state != CoroutineState::WAIT_EVENT)
    {
        throw std::invalid_argument("SuspendCurrent requires a waiting state");
    }
    CoroutineScheduler *scheduler = Current();
    if (scheduler == nullptr || scheduler->current_ == nullptr)
    {
        throw std::logic_error("SuspendCurrent must be called by a running coroutine");
    }
    scheduler->YieldCurrent(waiting_state);
}

CoroutineScheduler *CoroutineScheduler::Current() noexcept
{
    return tls_scheduler;
}

Coroutine_Routine *CoroutineScheduler::CurrentRoutine() noexcept
{
    return tls_scheduler == nullptr ? nullptr : tls_scheduler->current_;
}

void CoroutineScheduler::CoroutineTrampoline() noexcept
{
    CoroutineScheduler *scheduler = Current();
    Coroutine_Routine *routine = CurrentRoutine();
    if (scheduler == nullptr || routine == nullptr)
    {
        std::terminate();
    }
#if RPC_RUNTIME_HAS_ASAN
    Coroutine_Routine *caller = scheduler->call_stack_.size() < 2
                                    ? nullptr
                                    : scheduler->call_stack_[scheduler->call_stack_.size() - 2];
    const void **caller_stack_bottom = caller == nullptr ? &scheduler->root_asan_stack_bottom_
                                                          : &caller->asan_stack_bottom_;
    std::size_t *caller_stack_size = caller == nullptr ? &scheduler->root_asan_stack_size_
                                                        : &caller->asan_stack_size_;
    FinishAsanSwitch(routine->asan_fake_stack_, caller_stack_bottom, caller_stack_size);
    UnpoisonCoroutineStack(routine->stack());
#endif

    try
    {
        routine->entry_();
    }
    catch (...)
    {
        routine->exception_ = std::current_exception();
    }
    scheduler->YieldCurrent(CoroutineState::FINISHED);
#if defined(__GNUC__) || defined(__clang__)
    __builtin_unreachable();
#else
    std::terminate();
#endif
}

void CoroutineScheduler::CheckOwnerThread() const
{
    if (std::this_thread::get_id() != owner_thread_)
    {
        throw std::logic_error("CoroutineScheduler is thread-affine");
    }
}

void CoroutineScheduler::InitializeContext(Coroutine_Routine &routine)
{
    const std::uintptr_t top = reinterpret_cast<std::uintptr_t>(routine.stack().stack_top());
    routine.context_.rsp = top - sizeof(void *);
    routine.context_.rip = reinterpret_cast<std::uintptr_t>(&CoroutineScheduler::CoroutineTrampoline);
}

CoroutineScheduler::SharedStackRestore CoroutineScheduler::PrepareSharedStack(
    Coroutine_Routine &routine, Coroutine_Routine *current, const void *current_stack_pointer)
{
    SharedStackRestore restore;
    SharedStackSlot *slot = routine.shared_stack_;
    if (slot == nullptr || slot->occupant == &routine)
    {
        return restore;
    }
    if (slot->occupant != nullptr)
    {
        SaveSharedStack(*slot->occupant, slot->occupant == current ? current_stack_pointer : nullptr);
    }
    slot->occupant = &routine;

    if (!routine.stack_snapshot_.empty())
    {
        if (routine.stack_snapshot_begin_ == 0)
        {
            throw std::logic_error("shared stack snapshot has no restore address");
        }
        restore.begin = reinterpret_cast<void *>(routine.stack_snapshot_begin_);
        restore.data = routine.stack_snapshot_.data();
        restore.size = routine.stack_snapshot_.size();
#if RPC_RUNTIME_HAS_ASAN
        UnpoisonSharedStackRange(restore.begin, restore.size);
#endif
    }
    return restore;
}

void CoroutineScheduler::SaveSharedStack(Coroutine_Routine &routine, const void *stack_pointer)
{
    if (routine.shared_stack_ == nullptr || routine.state_ == CoroutineState::INIT ||
        routine.state_ == CoroutineState::FINISHED)
    {
        return;
    }

    const std::uintptr_t stack_top = reinterpret_cast<std::uintptr_t>(routine.stack().stack_top());
    const std::uintptr_t stack_bottom = reinterpret_cast<std::uintptr_t>(routine.stack().usable_bottom());
    const std::uintptr_t saved_rsp = stack_pointer == nullptr
                                         ? routine.context_.rsp
                                         : reinterpret_cast<std::uintptr_t>(stack_pointer);
    if (saved_rsp < stack_bottom || saved_rsp > stack_top)
    {
        throw std::logic_error("saved coroutine stack pointer is outside its stack");
    }
    const std::size_t used = static_cast<std::size_t>(stack_top - saved_rsp);
    routine.stack_snapshot_begin_ = saved_rsp;

    // resize() reuses the retained capacity after a pooled routine is reset.
    routine.stack_snapshot_.resize(used);
#if RPC_RUNTIME_HAS_ASAN
    UnpoisonSharedStackRange(reinterpret_cast<const void *>(saved_rsp), used);
#endif
    std::memcpy(routine.stack_snapshot_.data(), reinterpret_cast<const void *>(saved_rsp), used);
}

void CoroutineScheduler::EnqueueReady(const std::shared_ptr<Coroutine_Routine> &routine)
{
    if (!routine || routine->state_ != CoroutineState::READY || routine->queued_)
    {
        return;
    }
    routine->queued_ = true;
    ready_queue_.push_back(routine);
}

std::shared_ptr<Coroutine_Routine> CoroutineScheduler::FindRoutine(Coroutine_Routine *routine) const
{
    const auto found = routine == nullptr ? routines_.end() : routines_.find(routine->id_);
    if (found == routines_.end() || found->second.get() != routine)
    {
        throw std::logic_error("coroutine is no longer owned by this scheduler");
    }
    return found->second;
}

void CoroutineScheduler::YieldCurrent(CoroutineState next_state)
{
    Coroutine_Routine *routine = current_;
    if (routine == nullptr || routine->state_ != CoroutineState::RUNNING || call_stack_.size() < 2 ||
        call_stack_.back() != routine)
    {
        std::terminate();
    }

    Coroutine_Routine *caller = call_stack_[call_stack_.size() - 2];
    CoroutineContext *caller_context = caller == nullptr ? &root_context_ : &caller->context_;
#if RPC_RUNTIME_HAS_ASAN
    void **coroutine_fake_stack = &routine->asan_fake_stack_;
#endif
    routine->state_ = next_state;
    call_stack_.pop_back();
    current_ = caller;

#if RPC_RUNTIME_HAS_ASAN
    const void *caller_stack_bottom = caller == nullptr ? root_asan_stack_bottom_ : caller->asan_stack_bottom_;
    const std::size_t caller_stack_size = caller == nullptr ? root_asan_stack_size_ : caller->asan_stack_size_;
    StartAsanSwitch(coroutine_fake_stack, caller_stack_bottom, caller_stack_size, next_state);
#endif
    const void *routine_stack_pointer = coroutine_stack_pointer();
    SharedStackRestore restore;
    if (caller != nullptr)
    {
        restore = PrepareSharedStack(*caller, routine, routine_stack_pointer);
    }
    coroutine_context_switch(&routine->context_, caller_context, restore.begin, restore.data, restore.size);
#if RPC_RUNTIME_HAS_ASAN
    const void **caller_asan_stack_bottom = caller == nullptr ? &root_asan_stack_bottom_
                                                               : &caller->asan_stack_bottom_;
    std::size_t *caller_asan_stack_size = caller == nullptr ? &root_asan_stack_size_
                                                             : &caller->asan_stack_size_;
    FinishAsanSwitch(routine->asan_fake_stack_, caller_asan_stack_bottom, caller_asan_stack_size);
    UnpoisonCoroutineStack(routine->stack());
#endif
}

void CoroutineScheduler::FinishRoutine(const std::shared_ptr<Coroutine_Routine> &routine)
{
    if (routine->on_finish_)
    {
        routine->on_finish_(*routine);
    }
    if (routine->shared_stack_ != nullptr && routine->shared_stack_->occupant == routine.get())
    {
        routine->shared_stack_->occupant = nullptr;
    }
    routine->stack_snapshot_.clear();
    routine->stack_snapshot_begin_ = 0;

    const bool pooled = pool_ && pool_->Release(routine);
    if (!pooled)
    {
        routines_.erase(routine->id_);
    }
}

} // namespace rpc::runtime
