#include "runtime/async_socket.h"

#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <unistd.h>

#include <limits>
#include <system_error>

namespace rpc::runtime {
namespace {

std::uint64_t DeadlineAfter(std::uint64_t now_ms, std::uint64_t timeout_ms) noexcept
{
    return timeout_ms > std::numeric_limits<std::uint64_t>::max() - now_ms
               ? std::numeric_limits<std::uint64_t>::max()
               : now_ms + timeout_ms;
}

} // namespace

std::uint64_t AsyncSocket::Remaining(std::uint64_t deadline_ms) noexcept
{
    if (deadline_ms == kNoTimeout)
    {
        return kNoTimeout;
    }
    const std::uint64_t now = TimerWheel::NowMs();
    return deadline_ms > now ? deadline_ms - now : 0;
}

WaitResult AsyncSocket::AwaitReadable(int fd, std::uint64_t timeout_ms)
{
    return coroutine_->AwaitReadable(fd, timeout_ms);
}

WaitResult AsyncSocket::AwaitWritable(int fd, std::uint64_t timeout_ms)
{
    return coroutine_->AwaitWritable(fd, timeout_ms);
}

void AsyncSocket::CancelFd(int fd)
{
    coroutine_->CancelFd(fd);
}

ssize_t AsyncSocket::Read(int fd, void *buffer, std::size_t size, std::uint64_t timeout_ms)
{
    const std::uint64_t deadline = timeout_ms == kNoTimeout ? kNoTimeout : DeadlineAfter(TimerWheel::NowMs(), timeout_ms);
    for (;;)
    {
        const ssize_t result = ::read(fd, buffer, size);
        if (result >= 0)
        {
            return result;
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        {
            return -1;
        }
        const WaitResult wait = AwaitReadable(fd, Remaining(deadline));
        if (wait != WaitResult::IO_READY)
        {
            errno = wait == WaitResult::TIMEOUT ? ETIMEDOUT : ECANCELED;
            return -1;
        }
    }
}

ssize_t AsyncSocket::Write(int fd, const void *buffer, std::size_t size, std::uint64_t timeout_ms)
{
    const std::uint64_t deadline = timeout_ms == kNoTimeout ? kNoTimeout : DeadlineAfter(TimerWheel::NowMs(), timeout_ms);
    for (;;)
    {
        const ssize_t result = ::write(fd, buffer, size);
        if (result >= 0)
        {
            return result;
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        {
            return -1;
        }
        const WaitResult wait = AwaitWritable(fd, Remaining(deadline));
        if (wait != WaitResult::IO_READY)
        {
            errno = wait == WaitResult::TIMEOUT ? ETIMEDOUT : ECANCELED;
            return -1;
        }
    }
}

ssize_t AsyncSocket::WriteAll(int fd, const void *buffer, std::size_t size, std::uint64_t timeout_ms)
{
    const std::uint64_t deadline = timeout_ms == kNoTimeout ? kNoTimeout : DeadlineAfter(TimerWheel::NowMs(), timeout_ms);
    std::size_t offset = 0;
    while (offset < size)
    {
        const ssize_t result = ::write(fd, static_cast<const char *>(buffer) + offset, size - offset);
        if (result > 0)
        {
            offset += static_cast<std::size_t>(result);
            continue;
        }
        if (result == 0)
        {
            return static_cast<ssize_t>(offset);
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        {
            return offset == 0 ? -1 : static_cast<ssize_t>(offset);
        }
        const WaitResult wait = AwaitWritable(fd, Remaining(deadline));
        if (wait != WaitResult::IO_READY)
        {
            errno = wait == WaitResult::TIMEOUT ? ETIMEDOUT : ECANCELED;
            return offset == 0 ? -1 : static_cast<ssize_t>(offset);
        }
    }
    return static_cast<ssize_t>(offset);
}

int AsyncSocket::Accept(int listen_fd, sockaddr *address, socklen_t *address_length, std::uint64_t timeout_ms)
{
    const std::uint64_t deadline = timeout_ms == kNoTimeout ? kNoTimeout : DeadlineAfter(TimerWheel::NowMs(), timeout_ms);
    for (;;)
    {
        const int result = ::accept4(listen_fd, address, address_length, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (result >= 0)
        {
            return result;
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        {
            return -1;
        }
        const WaitResult wait = AwaitReadable(listen_fd, Remaining(deadline));
        if (wait != WaitResult::IO_READY)
        {
            errno = wait == WaitResult::TIMEOUT ? ETIMEDOUT : ECANCELED;
            return -1;
        }
    }
}

int AsyncSocket::Connect(int fd, const sockaddr *address, socklen_t address_length, std::uint64_t timeout_ms)
{
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        return -1;
    }
    const int result = ::connect(fd, address, address_length);
    if (result == 0)
    {
        return 0;
    }
    if (errno != EINPROGRESS && errno != EALREADY && errno != EINTR)
    {
        return -1;
    }
    const WaitResult wait = AwaitWritable(fd, timeout_ms);
    if (wait != WaitResult::IO_READY)
    {
        errno = wait == WaitResult::TIMEOUT ? ETIMEDOUT : ECANCELED;
        return -1;
    }
    int socket_error = 0;
    socklen_t error_length = sizeof(socket_error);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_length) != 0)
    {
        return -1;
    }
    if (socket_error != 0)
    {
        errno = socket_error;
        return -1;
    }
    return 0;
}

int AsyncSocket::Close(int fd) noexcept
{
    try
    {
        CancelFd(fd);
    }
    catch (...)
    {
        errno = EINVAL;
        return -1;
    }
    return ::close(fd);
}

} // namespace rpc::runtime
