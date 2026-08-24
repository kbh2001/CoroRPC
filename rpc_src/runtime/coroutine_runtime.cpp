#include "runtime/coroutine.h"

#include <iostream>
#include <utility>

namespace rpc::runtime
{
    namespace
    {
        thread_local Coroutine *tls_runtime = nullptr;

        CoroutineScheduler::Options MakeSchedulerOptions(const Coroutine::Options &options)
        {
            CoroutineScheduler::Options scheduler_options;
            scheduler_options.shared_stack_count = options.shared_stack_count;
            scheduler_options.shared_stack_size = options.shared_stack_size;
            scheduler_options.pool_prewarm_count = options.pool_prewarm_count;
            scheduler_options.pool_max_size = options.pool_max_size;
            scheduler_options.pool_stack_size = options.stack_size;
            return scheduler_options;
        }
    }

    Coroutine::Coroutine() : Coroutine(Options{})
    {
    }

    Coroutine::Coroutine(Options options)
        : owner_thread_(std::this_thread::get_id()),
          scheduler_(MakeSchedulerOptions(options)),
          io_(scheduler_, options.timer_slots, options.flush_interval_ms),
          default_stack_size_(options.stack_size)
    {
        if (options.stack_size == 0)
        {
            throw std::invalid_argument("coroutine stack size must be non-zero");
        }
        tls_runtime = this;
    }

    Coroutine::~Coroutine()
    {
        if (live_routine_count_ != 0 || driving_)
        {
            std::terminate();
        }
        // Close the inbox before releasing remaining nodes. Concurrent pushes
        // during destruction are a caller lifetime error.
        inbox_closed_.store(true, std::memory_order_release);
        InboxNode *node = nullptr;
        while (inbox_.TryPop(node))
        {
            delete node;
        }
        while (inbox_pool_ != nullptr)
        {
            node = inbox_pool_;
            inbox_pool_ = node->next.load(std::memory_order_relaxed);
            delete node;
        }
        if (tls_runtime == this)
        {
            tls_runtime = nullptr;
        }
    }

    Coroutine::RoutineHandle Coroutine::CreateCo(Routine task)
    {
        CheckOwnerThread();
        if (!task)
        {
            throw std::invalid_argument("coroutine routine must not be empty");
        }
        RoutineHandle routine = scheduler_.Create(std::move(task), default_stack_size_);
        routine->on_finish_ = [this](Coroutine_Routine &finished)
        { OnRoutineFinished(finished); };
        return routine;
    }

    void Coroutine::Resume(RoutineHandle routine)
    {
        CheckOwnerThread();
        if (!routine || routine->scheduler_ != &scheduler_)
        {
            throw std::logic_error("Coroutine::Resume requires a routine created by this Coroutine");
        }
        if (routine->state_ == CoroutineState::READY && routine->queued_)
        {
            return;
        }
        if (routine->state_ == CoroutineState::WAIT_TIMER)
        {
            if (!io_.PrepareTimerResume(*routine))
            {
                throw std::logic_error("Coroutine::Resume could not detach timer wait");
            }
        }
        if (routine->state_ != CoroutineState::INIT && routine->state_ != CoroutineState::READY &&
            routine->state_ != CoroutineState::WAIT_EVENT)
        {
            throw std::logic_error("coroutine is not resumable");
        }
        const bool starting = routine->state_ == CoroutineState::INIT;
        if (starting)
        {
            ++live_routine_count_;
        }
        try
        {
            scheduler_.Resume(routine);
        }
        catch (...)
        {
            if (starting)
            {
                --live_routine_count_;
            }
            throw;
        }
    }

    bool Coroutine::ScheduleAtFlush(const RoutineHandle &routine)
    {
        CheckOwnerThread();
        if (!routine || routine->scheduler_ != &scheduler_)
        {
            return false;
        }
        return io_.ScheduleAtFlush(routine);
    }

    Coroutine::RoutineHandle Coroutine::Current() const
    {
        CheckOwnerThread();
        Coroutine_Routine *routine = CoroutineScheduler::CurrentRoutine();
        return routine == nullptr ? RoutineHandle{} : scheduler_.FindRoutine(routine);
    }

    void Coroutine::Yield()
    {
        CheckOwnerThread();
        CoroutineScheduler::SuspendCurrent(CoroutineState::WAIT_EVENT);
    }

    void Coroutine::YieldFor(std::uint64_t milliseconds)
    {
        CheckOwnerThread();
        io_.YieldFor(milliseconds);
    }

    void Coroutine::YieldUntilFlush()
    {
        CheckOwnerThread();
        io_.YieldUntilFlush();
    }

    void Coroutine::Go(Routine task)
    {
        Resume(CreateCo(std::move(task)));
    }

    Coroutine::RoutineHandle Coroutine::Spawn(Routine task)
    {
        // Install the completion callback before publishing the routine to the
        // scheduler. The owner routine cannot be preempted between these steps.
        RoutineHandle routine = CreateCo(std::move(task));
        // Account for a detached routine before enqueueing it so Join() also
        // waits for routines that have not started yet.
        ++live_routine_count_;
        routine->state_ = CoroutineState::READY;
        scheduler_.EnqueueReady(routine);
        return routine;
    }

    bool Coroutine::Submit(Routine task)
    {
        if (!task || inbox_closed_.load(std::memory_order_acquire))
        {
            return false;
        }
        // This method is callable from foreign threads and must not check owner affinity.
        InboxNode *node = AcquireInboxNode();
        node->task = std::move(task);
        inbox_.Push(node);
        io_.Wake();
        return true;
    }

    Coroutine::InboxNode *Coroutine::AcquireInboxNode()
    {
        std::lock_guard<std::mutex> lock(inbox_pool_mutex_);
        if (inbox_pool_ == nullptr)
        {
            return new InboxNode();
        }
        InboxNode *node = inbox_pool_;
        inbox_pool_ = node->next.load(std::memory_order_relaxed);
        node->next.store(nullptr, std::memory_order_relaxed);
        return node;
    }

    void Coroutine::ReleaseInboxNode(InboxNode *node) noexcept
    {
        node->task = {};
        std::lock_guard<std::mutex> lock(inbox_pool_mutex_);
        node->next.store(inbox_pool_, std::memory_order_relaxed);
        inbox_pool_ = node;
    }

    std::size_t Coroutine::DrainInbox()
    {
        std::size_t started = 0;
        InboxNode *node = nullptr;
        while (inbox_.TryPop(node))
        {
            Routine task = std::move(node->task);
            ReleaseInboxNode(node);
            // Spawn only queues the routine; user code does not run nested in root context.
            Spawn(std::move(task));
            ++started;
        }
        return started;
    }

    void Coroutine::RunUntil(const std::atomic_bool &stop_requested)
    {
        DriveUntil([&stop_requested] { return stop_requested.load(std::memory_order_acquire); });
    }

    void Coroutine::DriveUntil(const std::function<bool()> &stop)
    {
        CheckOwnerThread();
        if (driving_ || CoroutineScheduler::CurrentRoutine() != nullptr)
        {
            throw std::logic_error("Coroutine::RunUntil must be called once by the root context");
        }

        driving_ = true;
        try
        {
            while (!stop())
            {
                DrainInbox();
                scheduler_.RunReady();
                if (stop())
                {
                    break;
                }
                io_.Poll(scheduler_.HasReady() ? 0 : -1);
            }
            driving_ = false;
        }
        catch (...)
        {
            driving_ = false;
            throw;
        }
    }

    void Coroutine::Join()
    {
        DriveUntil([this] { return live_routine_count_ == 0; });
        RethrowFirstException();
    }

    void Coroutine::RethrowFirstException()
    {
        if (first_exception_ != nullptr)
        {
            std::exception_ptr exception = first_exception_;
            first_exception_ = nullptr;
            std::rethrow_exception(exception);
        }
    }

    bool Coroutine::CancelWait(const RoutineHandle &routine)
    {
        CheckOwnerThread();
        if (!routine || routine->scheduler_ != &scheduler_)
        {
            return false;
        }
        return io_.CancelWait(*routine);
    }

    bool Coroutine::InOwnerThread() const noexcept
    {
        return std::this_thread::get_id() == owner_thread_;
    }

    Coroutine *Coroutine::CurrentRuntime() noexcept
    {
        return tls_runtime;
    }

    WaitResult Coroutine::AwaitReadable(int fd, std::uint64_t timeout_ms)
    {
        return io_.AwaitReadable(fd, timeout_ms);
    }

    WaitResult Coroutine::AwaitWritable(int fd, std::uint64_t timeout_ms)
    {
        return io_.AwaitWritable(fd, timeout_ms);
    }

    void Coroutine::CancelFd(int fd)
    {
        io_.CancelFd(fd);
    }

    void Coroutine::CheckOwnerThread() const
    {
        if (!InOwnerThread())
        {
            throw std::logic_error("Coroutine is thread-affine");
        }
    }

    void Coroutine::OnRoutineFinished(Coroutine_Routine &routine)
    {
        if (live_routine_count_ == 0)
        {
            std::terminate();
        }
        --live_routine_count_;
        if (first_exception_ == nullptr && routine.exception_ != nullptr)
        {
            first_exception_ = routine.exception_;
        }
    }

} // namespace rpc::runtime
