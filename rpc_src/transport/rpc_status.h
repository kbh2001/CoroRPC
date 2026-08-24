#pragma once

#include <cstdint>

#include "protocol/frame.h"

namespace rpc::transport {

enum class RpcStatus : std::uint32_t {
    OK = 0,
    CANCELLED = 1,
    INVALID_ARGUMENT = 2,
    DEADLINE_EXCEEDED = 3,
    NOT_FOUND = 4,
    ALREADY_EXISTS = 5,
    RESOURCE_EXHAUSTED = 6,
    FAILED_PRECONDITION = 7,
    UNAVAILABLE = 8,
    INTERNAL = 9,
    UNIMPLEMENTED = 10,
};

inline RpcStatus ResponseStatus(const protocol::Frame &frame) noexcept
{
    return static_cast<RpcStatus>(frame.header.flags);
}

inline void SetResponseStatus(protocol::Frame &frame, RpcStatus status) noexcept
{
    frame.header.flags = static_cast<std::uint32_t>(status);
}

} // namespace rpc::transport
