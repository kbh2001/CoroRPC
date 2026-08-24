#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <vector>

#include "runtime/coroutine_context.h"
#include "runtime/sanitizer_config.h"
#include "runtime/stack_allocator.h"
#include "runtime/timer_wheel.h"

namespace rpc::runtime {

class Coroutine;
class Coroutine_Routine;
class CoroutineScheduler;
class IoManager;
struct IoWaitOperation;

struct SharedStackSlot {
    std::unique_ptr<CoroutineStack> stack;
    Coroutine_Routine *occupant = nullptr;
};

enum class CoroutineState : std::uint8_t {
    INIT,
    READY,
    RUNNING,
    WAIT_IO,
    WAIT_TIMER,
    // A user-defined event wait used by RPC response and writer notifications.
    WAIT_EVENT,
    FINISHED,
};

class Coroutine_Routine : private TimerNode {
public:
    using Entry = std::function<void()>;
    using FinishCallback = std::function<void(Coroutine_Routine &)>;

    Coroutine_Routine(CoroutineScheduler *scheduler, std::uint64_t id, Entry entry, std::size_t stack_size,
                       SharedStackSlot *shared_stack = nullptr);

    std::uint64_t id() const noexcept { return id_; }
    CoroutineState state() const noexcept { return state_; }
    bool has_exception() const noexcept { return exception_ != nullptr; }

    // Reset a finished routine while retaining its physical stack and snapshot
    // capacity. CoroutinePool only calls this on the scheduler owner thread.
    void Reset(Entry new_entry) {
        if (state_ != CoroutineState::FINISHED || scheduled || list != nullptr ||
            deadline_heap_index != TimerNode::kNotInDeadlineHeap)
        {
            std::terminate();
        }
        entry_ = std::move(new_entry);
        state_ = CoroutineState::INIT;
        exception_ = nullptr;
        queued_ = false;
        flush_queued_ = false;
        io_wait_ = nullptr;
        on_finish_ = {};
        stack_snapshot_.clear();
        stack_snapshot_begin_ = 0;
        context_ = {};
        previous = nullptr;
        next = nullptr;
        deadline_ms = 0;
        scheduled = false;
        deadline_heap_index = TimerNode::kNotInDeadlineHeap;
#if RPC_RUNTIME_HAS_ASAN
        asan_fake_stack_ = nullptr;
#endif
    }

private:
    friend class Coroutine;
    friend class CoroutineScheduler;
    friend class CoroutinePool;
    friend class IoManager;

    CoroutineScheduler *scheduler_;
    std::uint64_t id_;
    Entry entry_;
    FinishCallback on_finish_;
    CoroutineContext context_;
    std::unique_ptr<CoroutineStack> owned_stack_;
    SharedStackSlot *shared_stack_ = nullptr;
    std::vector<char> stack_snapshot_;
    std::uintptr_t stack_snapshot_begin_ = 0;
    CoroutineState state_ = CoroutineState::INIT;
    bool queued_ = false;
    bool flush_queued_ = false;
    IoWaitOperation *io_wait_ = nullptr;
    std::exception_ptr exception_;
#if RPC_RUNTIME_HAS_ASAN
    void *asan_fake_stack_ = nullptr;
    const void *asan_stack_bottom_ = nullptr;
    std::size_t asan_stack_size_ = 0;
#endif

    CoroutineStack &stack() noexcept;
    const CoroutineStack &stack() const noexcept;
};

} // namespace rpc::runtime
