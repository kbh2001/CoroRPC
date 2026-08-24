#include "transport/multiplexed_connection.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <utility>

namespace rpc::transport {
namespace {

constexpr std::size_t kWriteBatchBytes = 64 * 1024;

bool ConfigureTcpLatency(int fd)
{
    int domain = 0;
    socklen_t domain_length = sizeof(domain);
    if (getsockopt(fd, SOL_SOCKET, SO_DOMAIN, &domain, &domain_length) != 0)
    {
        return true;
    }
    if (domain != AF_INET && domain != AF_INET6)
    {
        return true;
    }
    const int enabled = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) == 0;
}

} // namespace

struct MultiplexedConnection::PendingCall {
    MultiplexedConnection *connection = nullptr;
    runtime::Coroutine::RoutineHandle coroutine;
    std::uint64_t request_id = 0;
    CallStatus status = CallStatus::NOT_CONNECTED;
    protocol::Frame response;
    bool completed = false;
    bool sent = false;

};

struct MultiplexedConnection::OutboundFrame {
    std::uint64_t request_id = 0;
    std::string bytes;
    bool control = false;
};

std::shared_ptr<MultiplexedConnection> MultiplexedConnection::Create(runtime::Coroutine &runtime,
                                                                     ConnectionLimits limits)
{
    if (!runtime.InOwnerThread())
    {
        throw std::logic_error("MultiplexedConnection must be created on its Coroutine owner thread");
    }
    return std::shared_ptr<MultiplexedConnection>(new MultiplexedConnection(runtime, limits));
}

MultiplexedConnection::MultiplexedConnection(runtime::Coroutine &runtime, ConnectionLimits limits)
    : runtime_(runtime), socket_(runtime), limits_(limits)
{
    if (limits_.max_pending_calls == 0 || limits_.max_outbound_bytes == 0)
    {
        throw std::invalid_argument("connection limits must be non-zero");
    }
    pending_calls_.reserve(std::min<std::size_t>(limits_.max_pending_calls, 256));
    pending_call_pool_.reserve(std::min<std::size_t>(limits_.max_pending_calls, 256));
    encode_buffer_pool_.reserve(std::min<std::size_t>(limits_.max_pending_calls, 64));
    write_batch_.reserve(kWriteBatchBytes);
}

MultiplexedConnection::~MultiplexedConnection()
{
    if (fd_ >= 0)
    {
        // A live connection is retained by its reader/writer coroutine. This
        // fallback only applies before fibers were started or after Close().
        close(fd_);
    }
}

bool MultiplexedConnection::Attach(int fd)
{
    if (!CheckOwnerThread() || state_ != ConnectionState::DISCONNECTED || fd < 0)
    {
        return false;
    }
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0 || !ConfigureTcpLatency(fd))
    {
        return false;
    }
    fd_ = fd;
    state_ = ConnectionState::READY;
    return StartFibers();
}

bool MultiplexedConnection::Connect(const sockaddr *address, socklen_t address_length, std::uint64_t timeout_ms)
{
    if (!CheckOwnerThread() || state_ != ConnectionState::DISCONNECTED || address == nullptr || address_length == 0)
    {
        return false;
    }
    const int fd = socket(address->sa_family, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0)
    {
        return false;
    }
    if (!ConfigureTcpLatency(fd))
    {
        close(fd);
        return false;
    }
    fd_ = fd;
    state_ = ConnectionState::CONNECTING;
    if (socket_.Connect(fd_, address, address_length, timeout_ms) != 0)
    {
        socket_.Close(fd_);
        fd_ = -1;
        state_ = ConnectionState::DISCONNECTED;
        return false;
    }
    state_ = ConnectionState::READY;
    return StartFibers();
}

CallResult MultiplexedConnection::Call(protocol::Frame request, std::uint64_t timeout_ms)
{
    CallResult result;
    if (!CheckOwnerThread() || runtime_.Current() == nullptr ||
        state_ != ConnectionState::READY)
    {
        return result;
    }
    if (request.header.message_type != protocol::MessageType::REQUEST)
    {
        result.status = CallStatus::INVALID_REQUEST;
        return result;
    }
    if (timeout_ms == 0)
    {
        result.status = CallStatus::TIMEOUT;
        return result;
    }
    if (pending_calls_.size() >= limits_.max_pending_calls)
    {
        result.status = CallStatus::LOCAL_OVERLOADED;
        ++metrics_.calls_rejected;
        return result;
    }
    if (request.header.request_id == 0)
    {
        request.header.request_id = NextRequestId();
    }
    result.request_id = request.header.request_id;
    std::string encoded = AcquireEncodeBuffer();
    if (!protocol::FrameEncoder::Encode(request, encoded))
    {
        ReleaseEncodeBuffer(std::move(encoded));
        result.status = CallStatus::ENCODE_ERROR;
        return result;
    }
    return CallEncoded(result.request_id, std::move(encoded), timeout_ms);
}

CallResult MultiplexedConnection::Call(const protocol::RequestMetadataView &metadata, std::string_view body,
                                       std::uint64_t timeout_ms)
{
    CallResult result;
    if (!CheckOwnerThread() || runtime_.Current() == nullptr || state_ != ConnectionState::READY)
    {
        return result;
    }
    if (timeout_ms == 0)
    {
        result.status = CallStatus::TIMEOUT;
        return result;
    }
    if (pending_calls_.size() >= limits_.max_pending_calls)
    {
        result.status = CallStatus::LOCAL_OVERLOADED;
        ++metrics_.calls_rejected;
        return result;
    }

    result.request_id = NextRequestId();
    std::string encoded = AcquireEncodeBuffer();
    if (!protocol::FrameEncoder::EncodeRequest(metadata, body, result.request_id, encoded))
    {
        ReleaseEncodeBuffer(std::move(encoded));
        result.status = CallStatus::ENCODE_ERROR;
        return result;
    }
    return CallEncoded(result.request_id, std::move(encoded), timeout_ms);
}

CallResult MultiplexedConnection::CallEncoded(std::uint64_t request_id, std::string encoded,
                                              std::uint64_t timeout_ms)
{
    CallResult result;
    result.request_id = request_id;
    if (pending_calls_.find(request_id) != pending_calls_.end())
    {
        ReleaseEncodeBuffer(std::move(encoded));
        result.status = CallStatus::DUPLICATE_REQUEST_ID;
        return result;
    }
    if (encoded.size() > limits_.max_outbound_bytes || outbound_bytes_ > limits_.max_outbound_bytes - encoded.size())
    {
        ReleaseEncodeBuffer(std::move(encoded));
        result.status = CallStatus::LOCAL_OVERLOADED;
        ++metrics_.calls_rejected;
        return result;
    }

    auto pending = AcquirePendingCall();
    pending->connection = this;
    pending->coroutine = runtime_.Current();
    pending->request_id = request_id;
    PendingCall *pending_ptr = pending.get();
    pending_calls_.emplace(request_id, std::move(pending));
    try
    {
        outbound_.push_back({request_id, std::move(encoded), false});
        outbound_bytes_ += outbound_.back().bytes.size();
        ++metrics_.calls_started;
    }
    catch (...)
    {
        auto found = pending_calls_.find(request_id);
        if (found != pending_calls_.end())
        {
            auto failed_pending = std::move(found->second);
            pending_calls_.erase(found);
            ReleasePendingCall(std::move(failed_pending));
        }
        throw;
    }
    WakeWriter();

    if (timeout_ms == runtime::kNoTimeout)
    {
        runtime_.Yield();
    }
    else
    {
        runtime_.YieldFor(timeout_ms);
    }

    if (!pending_ptr->completed)
    {
        pending_ptr->completed = true;
        pending_ptr->status = CallStatus::TIMEOUT;
        ++metrics_.calls_timed_out;
        if (pending_ptr->sent)
        {
            QueueCancel(pending_ptr->request_id);
        }
        MaybeFinishDrain();
    }

    result.status = pending_ptr->status;
    result.rpc_status = ResponseStatus(pending_ptr->response);
    result.response = std::move(pending_ptr->response);
    auto found = pending_calls_.find(request_id);
    auto completed_pending = std::move(found->second);
    pending_calls_.erase(found);
    ReleasePendingCall(std::move(completed_pending));
    return result;
}

void MultiplexedConnection::BeginDrain()
{
    if (!CheckOwnerThread() || state_ != ConnectionState::READY)
    {
        return;
    }
    state_ = ConnectionState::CLOSING;
    WakeWriter();
    MaybeFinishDrain();
}

void MultiplexedConnection::Close()
{
    if (!CheckOwnerThread() || state_ == ConnectionState::CLOSED || state_ == ConnectionState::DISCONNECTED)
    {
        return;
    }
    state_ = ConnectionState::CLOSING;
    if (fd_ >= 0)
    {
        socket_.Close(fd_);
        fd_ = -1;
    }
    outbound_.clear();
    outbound_bytes_ = 0;
    CompleteAllPending(CallStatus::CONNECTION_CLOSED);
    state_ = ConnectionState::CLOSED;
    WakeWriter();
}

bool MultiplexedConnection::StartFibers()
{
    try
    {
        const std::shared_ptr<MultiplexedConnection> self = shared_from_this();
        reader_ = runtime_.CreateCo([self] { self->ReaderLoop(); });
        writer_ = runtime_.CreateCo([self] { self->WriterLoop(); });
        runtime_.Resume(reader_);
        runtime_.Resume(writer_);
        return true;
    }
    catch (...)
    {
        Close();
        return false;
    }
}

void MultiplexedConnection::ReaderLoop()
{
    while (state_ == ConnectionState::READY || state_ == ConnectionState::CLOSING)
    {
        constexpr std::size_t kReadSize = 16 * 1024;
        char *buffer = decoder_.PrepareWritable(kReadSize);
        if (buffer == nullptr)
        {
            FailConnection(CallStatus::PROTOCOL_ERROR);
            break;
        }
        const ssize_t received = socket_.Read(fd_, buffer, kReadSize, runtime::kNoTimeout);
        if (received <= 0)
        {
            (void)decoder_.CommitWritable(0);
            FailConnection(CallStatus::CONNECTION_CLOSED);
            break;
        }
        if (decoder_.CommitWritable(static_cast<std::size_t>(received)) == protocol::DecodeStatus::PROTOCOL_ERROR)
        {
            FailConnection(CallStatus::PROTOCOL_ERROR);
            break;
        }
        for (;;)
        {
            protocol::Frame frame;
            const protocol::DecodeStatus decoded = decoder_.Next(frame);
            if (decoded == protocol::DecodeStatus::NEED_MORE)
            {
                break;
            }
            if (decoded == protocol::DecodeStatus::PROTOCOL_ERROR ||
                frame.header.message_type != protocol::MessageType::RESPONSE)
            {
                FailConnection(CallStatus::PROTOCOL_ERROR);
                break;
            }
            const auto found = pending_calls_.find(frame.header.request_id);
            if (found != pending_calls_.end())
            {
                CompletePending(*found->second, CallStatus::OK, std::move(frame));
            }
            else
            {
                ++metrics_.late_responses;
            }
        }
        if (state_ == ConnectionState::CLOSED || state_ == ConnectionState::DISCONNECTED)
        {
            break;
        }
    }
    reader_.reset();
}

void MultiplexedConnection::WriterLoop()
{
    while (state_ == ConnectionState::READY || state_ == ConnectionState::CLOSING)
    {
        if (outbound_.empty())
        {
            runtime_.Yield();
            continue;
        }
        write_batch_.clear();
        while (!outbound_.empty() &&
               (write_batch_.empty() || write_batch_.size() + outbound_.front().bytes.size() <= kWriteBatchBytes))
        {
            OutboundFrame frame = std::move(outbound_.front());
            outbound_.pop_front();
            outbound_bytes_ -= frame.bytes.size();
            if (!frame.control)
            {
                const auto pending = pending_calls_.find(frame.request_id);
                if (pending == pending_calls_.end() || pending->second->completed)
                {
                    ReleaseEncodeBuffer(std::move(frame.bytes));
                    continue;
                }
                pending->second->sent = true;
            }
            write_batch_.append(frame.bytes);
            ReleaseEncodeBuffer(std::move(frame.bytes));
        }
        if (write_batch_.empty())
        {
            continue;
        }
        const ssize_t written = socket_.WriteAll(fd_, write_batch_.data(), write_batch_.size(), runtime::kNoTimeout);
        if (written != static_cast<ssize_t>(write_batch_.size()))
        {
            FailConnection(CallStatus::CONNECTION_CLOSED);
            break;
        }
        MaybeFinishDrain();
    }
    writer_.reset();
}

void MultiplexedConnection::FailConnection(CallStatus status)
{
    if (state_ == ConnectionState::CLOSED || state_ == ConnectionState::DISCONNECTED)
    {
        return;
    }
    if (fd_ >= 0)
    {
        socket_.Close(fd_);
        fd_ = -1;
    }
    outbound_.clear();
    outbound_bytes_ = 0;
    ++metrics_.connection_failures;
    // Directly resumed callers may retry immediately. Publish the terminal
    // state before waking any of them so they cannot re-enter this connection.
    state_ = ConnectionState::CLOSED;
    CompleteAllPending(status);
    WakeWriter();
}

void MultiplexedConnection::CompletePending(PendingCall &pending, CallStatus status, protocol::Frame response)
{
    if (pending.completed)
    {
        return;
    }
    pending.completed = true;
    pending.status = status;
    if (status == CallStatus::TIMEOUT)
    {
        ++metrics_.calls_timed_out;
    }
    else if (status == CallStatus::OK)
    {
        ++metrics_.calls_completed;
    }
    pending.response = std::move(response);
    MaybeFinishDrain();
    runtime_.Resume(pending.coroutine);
}

void MultiplexedConnection::CompleteAllPending(CallStatus status)
{
    std::vector<std::uint64_t> request_ids;
    request_ids.reserve(pending_calls_.size());
    for (const auto &entry : pending_calls_)
    {
        if (!entry.second->completed)
        {
            request_ids.push_back(entry.first);
        }
    }
    for (const std::uint64_t request_id : request_ids)
    {
        const auto found = pending_calls_.find(request_id);
        if (found != pending_calls_.end() && !found->second->completed)
        {
            CompletePending(*found->second, status);
        }
    }
}

void MultiplexedConnection::WakeWriter()
{
    if (writer_ != nullptr && writer_->state() == runtime::CoroutineState::WAIT_EVENT)
    {
        runtime_.Resume(writer_);
    }
}

void MultiplexedConnection::QueueCancel(std::uint64_t request_id)
{
    if (state_ != ConnectionState::READY || request_id == 0)
    {
        return;
    }
    protocol::Frame cancel;
    cancel.header.message_type = protocol::MessageType::CANCEL;
    cancel.header.request_id = request_id;
    std::string encoded = AcquireEncodeBuffer();
    if (!protocol::FrameEncoder::Encode(cancel, encoded) || encoded.size() > limits_.max_outbound_bytes ||
        outbound_bytes_ > limits_.max_outbound_bytes - encoded.size())
    {
        ReleaseEncodeBuffer(std::move(encoded));
        return;
    }
    outbound_bytes_ += encoded.size();
    outbound_.push_back({request_id, std::move(encoded), true});
    WakeWriter();
}

std::string MultiplexedConnection::AcquireEncodeBuffer()
{
    if (encode_buffer_pool_.empty())
    {
        return {};
    }
    std::string buffer = std::move(encode_buffer_pool_.back());
    encode_buffer_pool_.pop_back();
    buffer.clear();
    return buffer;
}

void MultiplexedConnection::ReleaseEncodeBuffer(std::string buffer)
{
    constexpr std::size_t kMaxRetainedCapacity = 256 * 1024;
    constexpr std::size_t kMaxRetainedBuffers = 64;
    if (buffer.capacity() > kMaxRetainedCapacity || encode_buffer_pool_.size() >= kMaxRetainedBuffers)
    {
        return;
    }
    buffer.clear();
    encode_buffer_pool_.push_back(std::move(buffer));
}

std::unique_ptr<MultiplexedConnection::PendingCall> MultiplexedConnection::AcquirePendingCall()
{
    if (pending_call_pool_.empty())
    {
        return std::make_unique<PendingCall>();
    }
    auto pending = std::move(pending_call_pool_.back());
    pending_call_pool_.pop_back();
    return pending;
}

void MultiplexedConnection::ReleasePendingCall(std::unique_ptr<PendingCall> pending)
{
    if (!pending || pending_call_pool_.size() >= 256)
    {
        return;
    }
    pending->connection = nullptr;
    pending->coroutine.reset();
    pending->request_id = 0;
    pending->status = CallStatus::NOT_CONNECTED;
    pending->response = {};
    pending->completed = false;
    pending->sent = false;
    pending_call_pool_.push_back(std::move(pending));
}

bool MultiplexedConnection::HasUnfinishedPending() const noexcept
{
    for (const auto &entry : pending_calls_)
    {
        if (!entry.second->completed)
        {
            return true;
        }
    }
    return false;
}

void MultiplexedConnection::MaybeFinishDrain()
{
    if (state_ == ConnectionState::CLOSING && outbound_.empty() && !HasUnfinishedPending())
    {
        Close();
    }
}

std::uint64_t MultiplexedConnection::NextRequestId()
{
    const std::uint64_t request_id = next_request_id_++;
    if (next_request_id_ == 0)
    {
        next_request_id_ = 1;
    }
    return request_id;
}

bool MultiplexedConnection::CheckOwnerThread() const noexcept
{
    return runtime_.InOwnerThread();
}

} // namespace rpc::transport
