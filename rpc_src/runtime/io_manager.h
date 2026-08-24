#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

#include "runtime/coroutine_scheduler.h"
#include "runtime/timer_wheel.h"

namespace rpc::runtime
{

    struct IoWaitOperation;

    enum class WaitDirection : std::uint8_t
    {
        READ,
        WRITE
    };
    enum class WaitResult : std::uint8_t
    {
        IO_READY,
        TIMEOUT,
        CANCELLED,
        ERROR
    };

    constexpr std::uint64_t kNoTimeout = std::numeric_limits<std::uint64_t>::max();

    class IoManager
    {
    public:
        explicit IoManager(CoroutineScheduler &scheduler,
                           std::size_t timer_slots = TimerWheel::kDefaultSlotCount,
                           std::uint64_t flush_interval_ms = 1);
        ~IoManager();

        IoManager(const IoManager &) = delete;
        IoManager &operator=(const IoManager &) = delete;

        WaitResult AwaitReadable(int fd, std::uint64_t timeout_ms);
        WaitResult AwaitWritable(int fd, std::uint64_t timeout_ms);
        void YieldFor(std::uint64_t milliseconds);
        void YieldUntilFlush();
        bool PrepareTimerResume(Coroutine_Routine &routine);
        bool CancelWait(Coroutine_Routine &routine);
        bool ScheduleAtFlush(const std::shared_ptr<Coroutine_Routine> &routine);
        // Drive one epoll/timer iteration. Completed I/O operations are made READY;
        // the scheduler restores them from its ready queue on the next loop turn.
        // max_wait_ms is an upper bound. -1 means wait until I/O or the nearest
        // timer deadline; the timer wheel always shortens a larger bound.
        int Poll(int max_wait_ms = -1);
        void Wake() noexcept;
        // The owner thread must call this before close(fd). It removes the
        // persistent epoll registration and wakes outstanding waiters once.
        void CancelFd(int fd);
        int epoll_fd() const noexcept { return epoll_fd_; }
        CoroutineScheduler &scheduler() noexcept { return scheduler_; }
        TimerWheel &timer_wheel() noexcept { return timer_wheel_; }
    private:
        struct FdContext;

        WaitResult Await(int fd, WaitDirection direction, std::uint64_t timeout_ms);
        void Register(IoWaitOperation &operation);
        void CompleteIo(FdContext &context, std::uint32_t events);
        void CompleteTimeout(IoWaitOperation &operation, std::uint64_t now_ms);
        void CompleteTimer(Coroutine_Routine &routine, std::uint64_t now_ms);
        void CompleteWait(IoWaitOperation &operation, WaitResult result);
        void UpdateInterest(FdContext &context);
        FdContext &GetFdContext(int fd);
        FdContext *FindFdContext(int fd) noexcept;
        void CheckOwnerThread() const;
        void FlushScheduledEvents(std::uint64_t now_ms);

        CoroutineScheduler &scheduler_;
        int epoll_fd_ = -1;
        int wake_fd_ = -1;
        TimerWheel timer_wheel_;
        std::uint64_t flush_interval_ms_;
        std::uint64_t next_flush_deadline_ms_;
        std::vector<std::shared_ptr<Coroutine_Routine>> flush_queue_;
        std::unordered_map<int, std::unique_ptr<FdContext>> fds_;
    };

} // namespace rpc::runtime
