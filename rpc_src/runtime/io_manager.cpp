#include "runtime/io_manager.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <climits>
#include <limits>
#include <stdexcept>
#include <system_error>

namespace rpc::runtime
{
    namespace
    {

        std::uint64_t DeadlineAfter(std::uint64_t now_ms, std::uint64_t timeout_ms) noexcept
        {
            return timeout_ms > std::numeric_limits<std::uint64_t>::max() - now_ms
                       ? std::numeric_limits<std::uint64_t>::max()
                       : now_ms + timeout_ms;
        }

    } // namespace

    struct IoWaitOperation : TimerNode
    {
        IoWaitOperation() : TimerNode(TimerNodeType::IO_WAIT) {}

        Coroutine_Routine *routine = nullptr;
        int fd = -1;
        WaitDirection direction = WaitDirection::READ;
        bool has_deadline = false;
        WaitResult result = WaitResult::ERROR;
    };

    struct IoManager::FdContext
    {
        int fd = -1;
        IoWaitOperation *read = nullptr;
        IoWaitOperation *write = nullptr;
        bool registered = false;
    };

    IoManager::IoManager(CoroutineScheduler &scheduler, std::size_t timer_slots, std::uint64_t flush_interval_ms)
        : scheduler_(scheduler), timer_wheel_(timer_slots), flush_interval_ms_(flush_interval_ms),
          next_flush_deadline_ms_(DeadlineAfter(TimerWheel::NowMs(), flush_interval_ms))
    {
        if (CoroutineScheduler::Current() != &scheduler_)
        {
            throw std::logic_error("IoManager must be created on its scheduler owner thread");
        }
        if (flush_interval_ms_ == 0)
        {
            throw std::invalid_argument("flush interval must be non-zero");
        }
        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0)
        {
            throw std::system_error(errno, std::generic_category(), "epoll_create1");
        }
        wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wake_fd_ < 0)
        {
            const int error = errno;
            close(epoll_fd_);
            epoll_fd_ = -1;
            throw std::system_error(error, std::generic_category(), "eventfd");
        }
        epoll_event wake_event{};
        wake_event.events = EPOLLIN;
        wake_event.data.ptr = nullptr;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &wake_event) != 0)
        {
            const int error = errno;
            close(wake_fd_);
            close(epoll_fd_);
            wake_fd_ = -1;
            epoll_fd_ = -1;
            throw std::system_error(error, std::generic_category(), "epoll_ctl wake fd");
        }
    }

    IoManager::~IoManager()
    {
        if (wake_fd_ >= 0)
        {
            close(wake_fd_);
        }
        if (epoll_fd_ >= 0)
        {
            close(epoll_fd_);
        }
    }

    void IoManager::Wake() noexcept
    {
        const std::uint64_t value = 1;
        const ssize_t ignored = write(wake_fd_, &value, sizeof(value));
        (void)ignored;
    }

    bool IoManager::ScheduleAtFlush(const std::shared_ptr<Coroutine_Routine> &routine)
    {
        CheckOwnerThread();
        if (!routine || routine->scheduler_ != &scheduler_ || routine->state_ != CoroutineState::WAIT_EVENT)
        {
            return false;
        }
        if (routine->flush_queued_)
        {
            return true;
        }
        routine->flush_queued_ = true;
        flush_queue_.push_back(routine);
        return true;
    }

    void IoManager::YieldUntilFlush()
    {
        CheckOwnerThread();
        Coroutine_Routine *routine = CoroutineScheduler::CurrentRoutine();
        if (routine == nullptr || routine->flush_queued_)
        {
            throw std::logic_error("YieldUntilFlush requires an unscheduled running coroutine");
        }
        routine->flush_queued_ = true;
        flush_queue_.push_back(scheduler_.FindRoutine(routine));
        CoroutineScheduler::SuspendCurrent(CoroutineState::WAIT_EVENT);
    }

    WaitResult IoManager::AwaitReadable(int fd, std::uint64_t timeout_ms)
    {
        return Await(fd, WaitDirection::READ, timeout_ms);
    }

    WaitResult IoManager::AwaitWritable(int fd, std::uint64_t timeout_ms)
    {
        return Await(fd, WaitDirection::WRITE, timeout_ms);
    }

    WaitResult IoManager::Await(int fd, WaitDirection direction, std::uint64_t timeout_ms)
    {
        CheckOwnerThread();
        Coroutine_Routine *routine = CoroutineScheduler::CurrentRoutine();
        if (routine == nullptr)
        {
            throw std::logic_error("Await must be called by a coroutine");
        }
        if (timeout_ms == 0)
        {
            return WaitResult::TIMEOUT;
        }
        const std::uint64_t now = TimerWheel::NowMs();

        auto operation = std::make_unique<IoWaitOperation>();
        operation->routine = routine;
        operation->fd = fd;
        operation->direction = direction;
        operation->has_deadline = timeout_ms != kNoTimeout;
        if (operation->has_deadline)
        {
            operation->deadline_ms = DeadlineAfter(now, timeout_ms);
        }
        IoWaitOperation *operation_ptr = operation.get();
        Register(*operation_ptr);
        routine->io_wait_ = operation_ptr;

        CoroutineScheduler::SuspendCurrent(CoroutineState::WAIT_IO);

        const WaitResult result = operation_ptr->result;
        return result;
    }

    void IoManager::YieldFor(std::uint64_t milliseconds)
    {
        CheckOwnerThread();
        Coroutine_Routine *routine = CoroutineScheduler::CurrentRoutine();
        if (routine == nullptr)
        {
            throw std::logic_error("YieldFor must be called by a coroutine");
        }
        if (milliseconds == 0)
        {
            return;
        }
        if (routine->scheduled || routine->list != nullptr)
        {
            throw std::logic_error("coroutine already has a timer wait");
        }

        routine->deadline_ms = DeadlineAfter(TimerWheel::NowMs(), milliseconds);
        timer_wheel_.Add(static_cast<TimerNode &>(*routine));
        CoroutineScheduler::SuspendCurrent(CoroutineState::WAIT_TIMER);
    }

    bool IoManager::PrepareTimerResume(Coroutine_Routine &routine)
    {
        CheckOwnerThread();
        if (routine.state_ != CoroutineState::WAIT_TIMER)
        {
            return false;
        }
        timer_wheel_.Remove(static_cast<TimerNode &>(routine));
        routine.state_ = CoroutineState::READY;
        return true;
    }

    bool IoManager::CancelWait(Coroutine_Routine &routine)
    {
        CheckOwnerThread();
        if (routine.state_ != CoroutineState::WAIT_IO || routine.io_wait_ == nullptr)
        {
            return false;
        }
        CompleteWait(*routine.io_wait_, WaitResult::CANCELLED);
        return true;
    }

    IoManager::FdContext &IoManager::GetFdContext(int fd)
    {
        auto found = fds_.find(fd);
        if (found == fds_.end())
        {
            auto context = std::make_unique<FdContext>();
            context->fd = fd;
            found = fds_.emplace(fd, std::move(context)).first;
        }
        return *found->second;
    }

    IoManager::FdContext *IoManager::FindFdContext(int fd) noexcept
    {
        const auto found = fds_.find(fd);
        return found == fds_.end() ? nullptr : found->second.get();
    }

    void IoManager::Register(IoWaitOperation &operation)
    {
        FdContext &context = GetFdContext(operation.fd);
        IoWaitOperation *&slot = operation.direction == WaitDirection::READ ? context.read : context.write;
        if (slot != nullptr)
        {
            throw std::logic_error("one fd direction may have only one waiting coroutine");
        }
        slot = &operation;
        try
        {
            if (operation.has_deadline)
            {
                timer_wheel_.Add(operation);
            }
            UpdateInterest(context);
        }
        catch (...)
        {
            if (operation.has_deadline)
            {
                timer_wheel_.Remove(operation);
            }
            if (slot == &operation)
            {
                slot = nullptr;
            }
            try
            {
                UpdateInterest(context);
            }
            catch (...)
            {
            }
            throw;
        }
    }

    void IoManager::UpdateInterest(FdContext &context)
    {
        // Keep a long-lived connection registered. With no waiter, MOD to an
        // empty mask so a persistent HUP cannot make epoll_wait spin. DEL is
        // reserved for CancelFd immediately before close(fd).
        std::uint32_t events = 0;
        if (context.read != nullptr || context.write != nullptr)
        {
            events = EPOLLERR | EPOLLHUP | EPOLLRDHUP;
            if (context.read != nullptr)
            {
                events |= EPOLLIN;
            }
            if (context.write != nullptr)
            {
                events |= EPOLLOUT;
            }
        }

        epoll_event event{};
        event.events = events;
        event.data.ptr = &context;
        if (!context.registered)
        {
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, context.fd, &event) != 0)
            {
                throw std::system_error(errno, std::generic_category(), "epoll_ctl ADD");
            }
            context.registered = true;
        }
        else
        {
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, context.fd, &event) != 0)
            {
                throw std::system_error(errno, std::generic_category(), "epoll_ctl MOD");
            }
        }
    }

    void IoManager::CompleteIo(FdContext &context, std::uint32_t events)
    {
        if (context.read != nullptr && (events & (EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0)
        {
            CompleteWait(*context.read, WaitResult::IO_READY);
        }
        if (context.write != nullptr && (events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) != 0)
        {
            CompleteWait(*context.write, WaitResult::IO_READY);
        }
        // Linux reports ERR/HUP even when the interest mask is zero. Once all
        // waiters have been collected, detach a terminal fd so the next Poll does
        // not spin on a persistent hangup. Its resumed connection owner will
        // decide whether to close or reconnect it.
        if ((events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0 && context.read == nullptr && context.write == nullptr &&
            context.registered)
        {
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, context.fd, nullptr);
            context.registered = false;
        }
    }

    void IoManager::CompleteWait(IoWaitOperation &operation, WaitResult result)
    {
        operation.result = result;
        if (operation.has_deadline)
        {
            timer_wheel_.Remove(operation);
        }
        if (FdContext *context = FindFdContext(operation.fd))
        {
            IoWaitOperation *&slot = operation.direction == WaitDirection::READ ? context->read : context->write;
            if (slot == &operation)
            {
                slot = nullptr;
                UpdateInterest(*context);
            }
        }
        if (operation.routine->io_wait_ == &operation)
        {
            operation.routine->io_wait_ = nullptr;
        }
        scheduler_.MakeReady(operation.routine);
    }

    void IoManager::CompleteTimeout(IoWaitOperation &operation, std::uint64_t now_ms)
    {
        if (now_ms < operation.deadline_ms)
        {
            timer_wheel_.Add(operation);
            return;
        }
        CompleteWait(operation, WaitResult::TIMEOUT);
    }

    void IoManager::CompleteTimer(Coroutine_Routine &routine, std::uint64_t now_ms)
    {
        if (now_ms < routine.deadline_ms)
        {
            timer_wheel_.Add(static_cast<TimerNode &>(routine));
            return;
        }
        scheduler_.MakeReady(&routine);
    }

    void IoManager::FlushScheduledEvents(std::uint64_t now_ms)
    {
        if (now_ms < next_flush_deadline_ms_)
        {
            return;
        }
        do
        {
            next_flush_deadline_ms_ += flush_interval_ms_;
        } while (next_flush_deadline_ms_ <= now_ms);

        for (const std::shared_ptr<Coroutine_Routine> &routine : flush_queue_)
        {
            routine->flush_queued_ = false;
            if (routine->state_ == CoroutineState::WAIT_EVENT)
            {
                scheduler_.MakeReady(routine.get());
            }
        }
        flush_queue_.clear();
    }

    int IoManager::Poll(int max_wait_ms)
    {
        CheckOwnerThread();
        int timeout_ms = max_wait_ms;
        const std::uint64_t before_poll_ms = TimerWheel::NowMs();
        if (!flush_queue_.empty())
        {
            const std::uint64_t flush_remaining_ms =
                next_flush_deadline_ms_ > before_poll_ms ? next_flush_deadline_ms_ - before_poll_ms : 0;
            const int flush_wait_ms = flush_remaining_ms > static_cast<std::uint64_t>(INT_MAX)
                                          ? INT_MAX
                                          : static_cast<int>(flush_remaining_ms);
            if (timeout_ms < 0 || flush_wait_ms < timeout_ms)
            {
                timeout_ms = flush_wait_ms;
            }
        }
        const std::uint64_t deadline_ms = timer_wheel_.NextDeadlineMs();
        if (deadline_ms != kNoTimeout)
        {
            const std::uint64_t remaining_ms = deadline_ms > before_poll_ms ? deadline_ms - before_poll_ms : 0;
            const int deadline_wait_ms = remaining_ms > static_cast<std::uint64_t>(INT_MAX)
                                             ? INT_MAX
                                             : static_cast<int>(remaining_ms);
            if (timeout_ms < 0 || deadline_wait_ms < timeout_ms)
            {
                timeout_ms = deadline_wait_ms;
            }
        }
        epoll_event events[256]{};
        const int count = epoll_wait(epoll_fd_, events, 256, timeout_ms);
        if (count < 0 && errno != EINTR)
        {
            throw std::system_error(errno, std::generic_category(), "epoll_wait");
        }
        for (int index = 0; index < std::max(count, 0); ++index)
        {
            auto *context = static_cast<FdContext *>(events[index].data.ptr);
            if (context == nullptr)
            {
                std::uint64_t value;
                while (read(wake_fd_, &value, sizeof(value)) > 0)
                {
                }
                continue;
            }
            CompleteIo(*context, events[index].events);
        }
        const std::uint64_t now = TimerWheel::NowMs();
        if (!flush_queue_.empty())
        {
            FlushScheduledEvents(now);
        }
        TimerList expired;
        timer_wheel_.Advance(now, expired);
        while (TimerNode *node = expired.PopFront())
        {
            switch (node->type)
            {
            case TimerNodeType::IO_WAIT:
            {
                auto *operation = static_cast<IoWaitOperation *>(node);
                CompleteTimeout(*operation, now);
                break;
            }
            case TimerNodeType::ROUTINE_WAIT:
            {
                auto *routine = static_cast<Coroutine_Routine *>(node);
                CompleteTimer(*routine, now);
                break;
            }
            case TimerNodeType::NONE:
                throw std::logic_error("untyped timer node reached IoManager");
            }
        }
        return count < 0 ? 0 : count;
    }

    void IoManager::CheckOwnerThread() const
    {
        if (CoroutineScheduler::Current() != &scheduler_)
        {
            throw std::logic_error("IoManager is thread-affine");
        }
    }

    void IoManager::CancelFd(int fd)
    {
        CheckOwnerThread();
        FdContext *context = FindFdContext(fd);
        if (context == nullptr)
        {
            return;
        }
        if (context->registered)
        {
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, context->fd, nullptr);
            context->registered = false;
        }
        IoWaitOperation *read = context->read;
        IoWaitOperation *write = context->write;
        context->read = nullptr;
        context->write = nullptr;
        if (read != nullptr)
        {
            CompleteWait(*read, WaitResult::CANCELLED);
        }
        if (write != nullptr)
        {
            CompleteWait(*write, WaitResult::CANCELLED);
        }
        fds_.erase(fd);
    }

} // namespace rpc::runtime
