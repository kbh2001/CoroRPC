#pragma once

#include <cstddef>
#include <cstdint>
#include <sys/socket.h>

#include "runtime/coroutine.h"

namespace rpc::runtime {

class AsyncSocket {
public:
    // The public construction path. Socket operations keep synchronous call
    // syntax while Coroutine drives their EAGAIN waits.
    explicit AsyncSocket(Coroutine &coroutine) : coroutine_(&coroutine) {}

    ssize_t Read(int fd, void *buffer, std::size_t size, std::uint64_t timeout_ms);
    ssize_t Write(int fd, const void *buffer, std::size_t size, std::uint64_t timeout_ms);
    ssize_t WriteAll(int fd, const void *buffer, std::size_t size, std::uint64_t timeout_ms);
    int Accept(int listen_fd, sockaddr *address, socklen_t *address_length, std::uint64_t timeout_ms);
    int Connect(int fd, const sockaddr *address, socklen_t address_length, std::uint64_t timeout_ms);
    int Close(int fd) noexcept;

private:
    static std::uint64_t Remaining(std::uint64_t deadline_ms) noexcept;
    WaitResult AwaitReadable(int fd, std::uint64_t timeout_ms);
    WaitResult AwaitWritable(int fd, std::uint64_t timeout_ms);
    void CancelFd(int fd);

    Coroutine *coroutine_ = nullptr;
};

} // namespace rpc::runtime
