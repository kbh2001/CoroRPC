#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "runtime/io_manager.h"
#include "runtime/mpsc_queue.h"

namespace rpc::runtime {

class AsyncSocket;
class Coroutine;

// One Coroutine instance owns one thread's scheduler and I/O. Every coroutine
// primitive below is thread-affine: it must run on the owner thread. Work from
// other threads enters through the MPSC Task inbox, not through this class.
class Coroutine {
public:
    using Routine = std::function<void()>;
    using RoutineHandle = std::shared_ptr<Coroutine_Routine>;

    struct Options {
        std::size_t stack_size = CoroutineScheduler::kDefaultStackSize;
        std::size_t shared_stack_count = 1;
        std::size_t shared_stack_size = CoroutineScheduler::kDefaultStackSize;
        std::size_t timer_slots = TimerWheel::kDefaultSlotCount;
        std::uint64_t flush_interval_ms = 1;
        std::size_t pool_prewarm_count = 0; 
        std::size_t pool_max_size = 0;   
    };

    Coroutine();
    explicit Coroutine(Options options);
    ~Coroutine();

    Coroutine(const Coroutine &) = delete;
    Coroutine &operator=(const Coroutine &) = delete;

    // Create an INIT routine without running it. The owner thread may call
    // this from either the root context or a running routine.
    RoutineHandle CreateCo(Routine task);

    // Enter an INIT routine or resume a WAIT_EVENT routine immediately. An
    // event owner must detach its wait state before resuming WAIT_EVENT. Kernel
    // I/O and timer waits remain owned exclusively by IoManager.
    void Resume(RoutineHandle routine);

    // Queue a WAIT_EVENT routine for the next fixed flush boundary. Repeated
    // requests for the same routine within one interval are coalesced.
    bool ScheduleAtFlush(const RoutineHandle &routine);

    // Return the running routine on this owner thread, or an empty handle in
    // the root context.
    RoutineHandle Current() const;

    // Suspend until Resume() is called for a user-defined event.
    void Yield();

    // Suspend the running routine for at least milliseconds. Zero returns
    // immediately. The owner event loop determines the actual resume time.
    void YieldFor(std::uint64_t milliseconds);

    // Suspend the running routine until the next fixed runtime flush boundary.
    // This is the polling primitive used by owner-thread SPSC consumers.
    void YieldUntilFlush();

    WaitResult AwaitReadable(int fd, std::uint64_t timeout_ms = kNoTimeout);
    WaitResult AwaitWritable(int fd, std::uint64_t timeout_ms = kNoTimeout);

    // Convenience wrapper equivalent to Resume(CreateCo(task)).
    // Called from inside a coroutine this enters the child synchronously: the
    // caller stays parked on this line until the child yields or returns.
    void Go(Routine task);

    // Detached start: queue the routine as READY and return immediately. The
    // caller keeps running; the child first executes from the root context in a
    // later scheduling round, so it never nests on the caller's stack.
    //
    // Go() vs Spawn() is the parent/child contract:
    //   Go()    - parent parks here, child runs now (nested, join-like)
    //   Spawn() - parent continues, child runs later (detached)
    // Neither transfers values. A parent that needs the child's result must
    // fan in through CoroResult<T>, which lives on the heap and therefore
    // survives shared-stack snapshot restores.
    RoutineHandle Spawn(Routine task);

    // Cross-thread submit: the ONE primitive on this class callable from a
    // foreign thread. The task is queued on an MPSC inbox and later started by
    // the owner thread as if by Spawn(), from the root context.
    //
    // Returns false once the runtime has been asked to stop, so callers can
    // report unavailability instead of silently dropping work.
    //
    // Submission wakes an idle epoll wait through IoManager's eventfd.
    //
    // This allocates one small node per submit. Hot paths that dispatch per
    // RPC should use the pooled Task/TaskSink route instead.
    bool Submit(Routine task);

    void Wake() noexcept { io_.Wake(); }

    // Keep driving ready work, fd readiness, and timers until stop is requested.
    // Call Join() afterwards to drain routines already started.
    void RunUntil(const std::atomic_bool &stop_requested);

    // Drives fd readiness and timers until all routines started by Resume()/Go()
    // have completed. Unstarted CreateCo() handles are not joinable work.
    void Join();

    // Cancel the routine's current AwaitReadable/AwaitWritable operation.
    // This never closes the fd and must run on the owner thread.
    bool CancelWait(const RoutineHandle &routine);

    bool InOwnerThread() const noexcept;
    std::size_t live_routine_count() const noexcept { return live_routine_count_; }

    // Returns the Coroutine runtime installed on this thread. Current() is
    // non-empty only while the caller is executing inside one of its routines.
    static Coroutine *CurrentRuntime() noexcept;

private:
    friend class AsyncSocket;

    void CancelFd(int fd);

    void CheckOwnerThread() const;
    void DriveUntil(const std::function<bool()> &stop);
    void RethrowFirstException();
    void OnRoutineFinished(Coroutine_Routine &routine);

    // Cross-thread inbox. The owner thread deletes nodes while draining it.
    struct InboxNode {
        std::atomic<InboxNode *> next{nullptr};
        Routine task;
    };

    InboxNode *AcquireInboxNode();
    void ReleaseInboxNode(InboxNode *node) noexcept;

    std::size_t DrainInbox();

    std::thread::id owner_thread_;
    CoroutineScheduler scheduler_;
    IoManager io_;
    std::size_t default_stack_size_ = CoroutineScheduler::kDefaultStackSize;
    std::size_t live_routine_count_ = 0;
    bool driving_ = false;
    std::exception_ptr first_exception_;
    MpscQueue<InboxNode> inbox_;
    std::atomic_bool inbox_closed_{false};
    std::mutex inbox_pool_mutex_;
    InboxNode *inbox_pool_ = nullptr;
};

} // namespace rpc::runtime
