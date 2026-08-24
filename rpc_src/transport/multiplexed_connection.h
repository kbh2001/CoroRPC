#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <unordered_map>
#include <vector>

#include <sys/socket.h>

#include "protocol/frame.h"
#include "protocol/rpc_metadata.h"
#include "runtime/async_socket.h"
#include "transport/rpc_status.h"

namespace rpc::transport {

enum class ConnectionState : std::uint8_t { DISCONNECTED, CONNECTING, READY, CLOSING, CLOSED };

enum class CallStatus : std::uint8_t {
    OK,
    TIMEOUT,
    NOT_CONNECTED,
    INVALID_REQUEST,
    DUPLICATE_REQUEST_ID,
    ENCODE_ERROR,
    CONNECTION_CLOSED,
    PROTOCOL_ERROR,
    RESOURCE_EXHAUSTED,
    LOCAL_OVERLOADED,
    SERVICE_NOT_FOUND,
    DRAINING,
};

struct ConnectionLimits {
    std::size_t max_pending_calls = 4096;
    std::size_t max_outbound_bytes = 16 * 1024 * 1024;
};

struct ConnectionMetrics {
    std::uint64_t calls_started = 0;
    std::uint64_t calls_completed = 0;
    std::uint64_t calls_timed_out = 0;
    std::uint64_t calls_rejected = 0;
    std::uint64_t late_responses = 0;
    std::uint64_t connection_failures = 0;
};

struct CallResult {
    CallStatus status = CallStatus::NOT_CONNECTED;
    RpcStatus rpc_status = RpcStatus::OK;
    std::uint64_t request_id = 0;
    protocol::Frame response;
};

// One connected TCP stream, owned by one Coroutine runtime thread. It has exactly one
// reader coroutine and one writer coroutine; RPC calls wait by request_id.
class MultiplexedConnection : public std::enable_shared_from_this<MultiplexedConnection> {
public:
    static std::shared_ptr<MultiplexedConnection> Create(runtime::Coroutine &runtime, ConnectionLimits limits = {});
    ~MultiplexedConnection();

    MultiplexedConnection(const MultiplexedConnection &) = delete;
    MultiplexedConnection &operator=(const MultiplexedConnection &) = delete;

    // Takes ownership of an already connected stream and enables O_NONBLOCK.
    bool Attach(int fd);
    bool Connect(const sockaddr *address, socklen_t address_length, std::uint64_t timeout_ms);
    CallResult Call(protocol::Frame request, std::uint64_t timeout_ms);
    CallResult Call(const protocol::RequestMetadataView &metadata, std::string_view body,
                    std::uint64_t timeout_ms);
    // Rejects new calls but continues to drain already accepted responses.
    void BeginDrain();
    void Close();

    ConnectionState state() const noexcept { return state_; }
    int fd() const noexcept { return fd_; }
    std::size_t pending_count() const noexcept { return pending_calls_.size(); }
    std::size_t outbound_bytes() const noexcept { return outbound_bytes_; }
    const ConnectionMetrics &metrics() const noexcept { return metrics_; }

private:
    struct PendingCall;
    struct OutboundFrame;

    MultiplexedConnection(runtime::Coroutine &runtime, ConnectionLimits limits);

    bool StartFibers();
    void ReaderLoop();
    void WriterLoop();
    void FailConnection(CallStatus status);
    void CompletePending(PendingCall &pending, CallStatus status, protocol::Frame response = {});
    void CompleteAllPending(CallStatus status);
    void WakeWriter();
    void QueueCancel(std::uint64_t request_id);
    bool HasUnfinishedPending() const noexcept;
    void MaybeFinishDrain();
    std::uint64_t NextRequestId();
    bool CheckOwnerThread() const noexcept;
    CallResult CallEncoded(std::uint64_t request_id, std::string encoded, std::uint64_t timeout_ms);
    std::string AcquireEncodeBuffer();
    void ReleaseEncodeBuffer(std::string buffer);
    std::unique_ptr<PendingCall> AcquirePendingCall();
    void ReleasePendingCall(std::unique_ptr<PendingCall> pending);

    runtime::Coroutine &runtime_;
    runtime::AsyncSocket socket_;
    ConnectionState state_ = ConnectionState::DISCONNECTED;
    int fd_ = -1;
    std::uint64_t next_request_id_ = 1;
    protocol::FrameDecoder decoder_;
    runtime::Coroutine::RoutineHandle reader_;
    runtime::Coroutine::RoutineHandle writer_;
    std::unordered_map<std::uint64_t, std::unique_ptr<PendingCall>> pending_calls_;
    std::vector<std::unique_ptr<PendingCall>> pending_call_pool_;
    std::deque<OutboundFrame> outbound_;
    std::vector<std::string> encode_buffer_pool_;
    std::string write_batch_;
    ConnectionLimits limits_;
    ConnectionMetrics metrics_;
    std::size_t outbound_bytes_ = 0;
};

} // namespace rpc::transport
