#include "transport/rpc_server.h"

#include "discovery/zookeeper_service_registry.h"
#include "transport/service_dispatcher.h"
#include "transport/rpc_status.h"

#include <arpa/inet.h>
#include <unistd.h>

#include <array>
#include <algorithm>
#include <cerrno>
#include <condition_variable>
#include <fcntl.h>
#include <limits>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>

namespace rpc::transport {
namespace {

constexpr std::size_t kWriteBatchBytes = 64 * 1024;

protocol::Frame MakeStatusResponse(std::uint64_t request_id, RpcStatus status, std::string body = {})
{
    protocol::Frame response;
    response.header.message_type = protocol::MessageType::RESPONSE;
    response.header.request_id = request_id;
    response.body = std::move(body);
    SetResponseStatus(response, status);
    return response;
}

std::uint64_t AddDeadline(std::uint64_t now_ms, std::uint64_t timeout_ms)
{
    if (timeout_ms == 0)
    {
        return runtime::kNoTimeout;
    }
    return timeout_ms > std::numeric_limits<std::uint64_t>::max() - now_ms ? std::numeric_limits<std::uint64_t>::max()
                                                                              : now_ms + timeout_ms;
}

std::string PeerScope(int fd)
{
    sockaddr_storage address{};
    socklen_t length = sizeof(address);
    if (getpeername(fd, reinterpret_cast<sockaddr *>(&address), &length) == 0)
    {
        char host[INET6_ADDRSTRLEN]{};
        if (address.ss_family == AF_INET)
        {
            const auto *ipv4 = reinterpret_cast<const sockaddr_in *>(&address);
            if (inet_ntop(AF_INET, &ipv4->sin_addr, host, sizeof(host)) != nullptr)
            {
                return host;
            }
        }
        else if (address.ss_family == AF_INET6)
        {
            const auto *ipv6 = reinterpret_cast<const sockaddr_in6 *>(&address);
            if (inet_ntop(AF_INET6, &ipv6->sin6_addr, host, sizeof(host)) != nullptr)
            {
                return host;
            }
        }
    }
    return "connection:" + std::to_string(fd);
}

std::uint64_t BodyFingerprint(std::string_view body) noexcept
{
    std::uint64_t hash = 1469598103934665603ULL;
    for (const unsigned char byte : body)
    {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

void AppendKeyPart(std::string &key, std::string_view value)
{
    key.append(std::to_string(value.size()));
    key.push_back(':');
    key.append(value.data(), value.size());
}

std::string BuildIdempotencyKey(std::string_view caller_scope, std::string_view service,
                                std::string_view method, std::string_view key)
{
    std::string result;
    result.reserve(caller_scope.size() + service.size() + method.size() + key.size() + 32);
    AppendKeyPart(result, caller_scope);
    AppendKeyPart(result, service);
    AppendKeyPart(result, method);
    AppendKeyPart(result, key);
    return result;
}

bool SetNonBlocking(int fd)
{
    const int flags = fcntl(fd, F_GETFL, 0);
    return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

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

runtime::WaitResult RpcContext::AwaitReadable(int fd, std::uint64_t timeout_ms) const
{
    runtime::Coroutine *runtime = nullptr;
    {
        std::lock_guard<std::mutex> lock(binding_mutex_);
        runtime = runtime_;
    }
    if (runtime == nullptr || !runtime->InOwnerThread())
    {
        throw std::logic_error("RpcContext has no I/O coroutine runtime");
    }
    if (IsCancelled())
    {
        return runtime::WaitResult::CANCELLED;
    }
    const runtime::WaitResult result = runtime->AwaitReadable(fd, EffectiveTimeout(timeout_ms));
    if (result == runtime::WaitResult::TIMEOUT && DeadlineExceeded(runtime::TimerWheel::NowMs()))
    {
        cancelled_.store(true, std::memory_order_release);
    }
    return result;
}

runtime::WaitResult RpcContext::AwaitWritable(int fd, std::uint64_t timeout_ms) const
{
    runtime::Coroutine *runtime = nullptr;
    {
        std::lock_guard<std::mutex> lock(binding_mutex_);
        runtime = runtime_;
    }
    if (runtime == nullptr || !runtime->InOwnerThread())
    {
        throw std::logic_error("RpcContext has no I/O coroutine runtime");
    }
    if (IsCancelled())
    {
        return runtime::WaitResult::CANCELLED;
    }
    const runtime::WaitResult result = runtime->AwaitWritable(fd, EffectiveTimeout(timeout_ms));
    if (result == runtime::WaitResult::TIMEOUT && DeadlineExceeded(runtime::TimerWheel::NowMs()))
    {
        cancelled_.store(true, std::memory_order_release);
    }
    return result;
}

void RpcContext::YieldFor(std::uint64_t milliseconds) const
{
    runtime::Coroutine *runtime = nullptr;
    {
        std::lock_guard<std::mutex> lock(binding_mutex_);
        runtime = runtime_;
    }
    if (runtime == nullptr || !runtime->InOwnerThread())
    {
        throw std::logic_error("RpcContext has no I/O coroutine runtime");
    }
    if (IsCancelled())
    {
        return;
    }
    runtime->YieldFor(EffectiveTimeout(milliseconds));
    if (DeadlineExceeded(runtime::TimerWheel::NowMs()))
    {
        cancelled_.store(true, std::memory_order_release);
    }
}

std::uint64_t RpcContext::EffectiveTimeout(std::uint64_t timeout_ms) const noexcept
{
    if (deadline_ms_ == runtime::kNoTimeout)
    {
        return timeout_ms;
    }
    const std::uint64_t now = runtime::TimerWheel::NowMs();
    const std::uint64_t remaining = deadline_ms_ > now ? deadline_ms_ - now : 0;
    return timeout_ms == runtime::kNoTimeout ? remaining : std::min(timeout_ms, remaining);
}

void RpcContext::Bind(runtime::Coroutine &runtime, runtime::Coroutine::RoutineHandle routine)
{
    std::lock_guard<std::mutex> lock(binding_mutex_);
    runtime_ = &runtime;
    routine_ = std::move(routine);
}

void RpcContext::Unbind()
{
    std::lock_guard<std::mutex> lock(binding_mutex_);
    routine_.reset();
    runtime_ = nullptr;
}

void RpcContext::Cancel()
{
    cancelled_.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(binding_mutex_);
    if (runtime_ == nullptr || !routine_)
    {
        return;
    }
    runtime::Coroutine *runtime = runtime_;
    runtime::Coroutine::RoutineHandle routine = routine_;
    // Cancel() may run on a foreign thread, so resume work through the owner runtime.
    runtime->Submit([runtime, routine = std::move(routine)] {
        switch (routine->state())
        {
        case runtime::CoroutineState::WAIT_IO:
            runtime->CancelWait(routine);
            break;
        case runtime::CoroutineState::WAIT_TIMER:
        case runtime::CoroutineState::WAIT_EVENT:
            runtime->Resume(routine);
            break;
        default:
            break;
        }
    });
}

class RpcServer::BoundedExecutor {
public:
    BoundedExecutor(std::size_t thread_count, std::size_t max_queue) : max_queue_(max_queue)
    {
        if (thread_count == 0 || max_queue == 0)
        {
            throw std::invalid_argument("worker thread and queue limits must be non-zero");
        }
        workers_.reserve(thread_count);
        for (std::size_t index = 0; index < thread_count; ++index)
        {
            workers_.emplace_back([this] { Run(); });
        }
    }

    ~BoundedExecutor()
    {
        Stop();
    }

    bool TrySubmit(std::function<void()> task)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_ || tasks_.size() >= max_queue_)
        {
            return false;
        }
        tasks_.push_back(std::move(task));
        available_.notify_one();
        return true;
    }

    void Stop()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_)
            {
                return;
            }
            stopping_ = true;
            tasks_.clear();
        }
        available_.notify_all();
        for (std::thread &worker : workers_)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }
    }

private:
    void Run()
    {
        for (;;)
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                available_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
                if (tasks_.empty())
                {
                    return;
                }
                task = std::move(tasks_.front());
                tasks_.pop_front();
            }
            task();
        }
    }

    std::size_t max_queue_;
    std::mutex mutex_;
    std::condition_variable available_;
    std::deque<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
    bool stopping_ = false;
};

class RpcServer::IoCoroutineExecutor {
public:
    using Task = std::function<void(runtime::Coroutine &)>;

    IoCoroutineExecutor(std::size_t thread_count, std::size_t max_pending)
    {
        if (thread_count == 0 || max_pending == 0)
        {
            throw std::invalid_argument("I/O worker and queue limits must be non-zero");
        }
        workers_.reserve(thread_count);
        for (std::size_t index = 0; index < thread_count; ++index)
        {
            workers_.push_back(std::make_unique<Worker>(max_pending));
        }
    }

    ~IoCoroutineExecutor()
    {
        RequestStop();
        Join();
    }

    bool TrySubmit(Task task)
    {
        if (!task || stopping_.load(std::memory_order_acquire))
        {
            return false;
        }
        const std::size_t start = next_worker_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
        for (std::size_t count = 0; count < workers_.size(); ++count)
        {
            Worker &worker = *workers_[(start + count) % workers_.size()];
            if (worker.TrySubmit(task))
            {
                return true;
            }
        }
        return false;
    }

    void RequestStop(std::function<void()> on_stopped = {})
    {
        bool expected = false;
        if (!stopping_.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        {
            return;
        }
        struct ShutdownState {
            explicit ShutdownState(std::size_t count, std::function<void()> callback)
                : remaining(count), on_stopped(std::move(callback)) {}
            std::atomic<std::size_t> remaining;
            std::function<void()> on_stopped;
        };
        auto state = std::make_shared<ShutdownState>(workers_.size(), std::move(on_stopped));
        for (auto &worker : workers_)
        {
            worker->RequestStop([state] {
                if (state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1 && state->on_stopped)
                {
                    state->on_stopped();
                }
            });
        }
    }

    void Join()
    {
        for (auto &worker : workers_)
        {
            worker->Join();
        }
    }

private:
    class Worker {
    public:
        explicit Worker(std::size_t max_pending) : max_pending_(max_pending), thread_([this] { Run(); })
        {
            std::unique_lock<std::mutex> lock(mutex_);
            ready_.wait(lock, [this] { return runtime_ != nullptr || failed_; });
            if (failed_)
            {
                lock.unlock();
                thread_.join();
                throw std::runtime_error("failed to start I/O coroutine worker");
            }
        }

        bool TrySubmit(const Task &task)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!task || stopping_ || runtime_ == nullptr || pending_ >= max_pending_)
            {
                return false;
            }
            ++pending_;
            try
            {
                // Submit the work to the worker runtime. Submit creates the routine.
                if (!runtime_->Submit([this, task] {
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        if (stopping_)
                        {
                            --pending_;
                            return;
                        }
                    }
                    // Execute the task in the worker coroutine.
                    try
                    {
                        task(*runtime_);
                    }
                    catch (...)
                    {
                    }
                    std::lock_guard<std::mutex> lock(mutex_);
                    --pending_;
                }))
                {
                    --pending_;
                    return false;
                }
            }
            catch (...)
            {
                --pending_;
                return false;
            }
            return true;
        }

        void RequestStop(std::function<void()> on_stopped)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_)
            {
                return;
            }
            stopping_ = true;
            on_stopped_ = std::move(on_stopped);
            if (runtime_ != nullptr)
            {
            // DrainInbox applies the stop request on the owner thread.
                runtime_->Submit([this] { stop_requested_.store(true, std::memory_order_release); });
            }
        }

        void Join()
        {
            if (thread_.joinable())
            {
                thread_.join();
            }
        }

    private:
        void Run() noexcept
        {
            try
            {
                runtime::Coroutine runtime;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    runtime_ = &runtime;
                }
                ready_.notify_all();
                runtime.RunUntil(stop_requested_);
                runtime.Join();
                std::function<void()> callback;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    runtime_ = nullptr;
                    callback = std::move(on_stopped_);
                }
                if (callback)
                {
                    callback();
                }
            }
            catch (...)
            {
                std::lock_guard<std::mutex> lock(mutex_);
                failed_ = true;
                runtime_ = nullptr;
                ready_.notify_all();
            }
        }

        std::size_t max_pending_;
        std::mutex mutex_;
        std::condition_variable ready_;
        runtime::Coroutine *runtime_ = nullptr;
        std::size_t pending_ = 0;
        bool stopping_ = false;
        bool failed_ = false;
        std::function<void()> on_stopped_;
        std::atomic_bool stop_requested_{false};
        std::thread thread_;
    };

    std::vector<std::unique_ptr<Worker>> workers_;
    std::atomic<std::size_t> next_worker_{0};
    std::atomic<bool> stopping_{false};
};

class RpcServer::NetworkShard {
public:
    NetworkShard(RpcServer &server, std::size_t index, runtime::Coroutine *external_runtime)
        : server_(server), index_(index), runtime_(external_runtime)
    {
        if (runtime_ != nullptr)
        {
            socket_ = std::make_unique<runtime::AsyncSocket>(*runtime_);
            initialized_ = true;
        }
    }

    ~NetworkShard()
    {
        RequestStop();
        Join();
    }

    bool Start()
    {
        if (initialized_)
        {
            return true;
        }
        thread_ = std::thread([this] { Run(); });
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [this] { return initialized_ || failed_; });
        if (!failed_)
        {
            return true;
        }
        lock.unlock();
        Join();
        return false;
    }

    bool Post(std::function<void()> task)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!task || runtime_ == nullptr || stop_requested_.load(std::memory_order_acquire))
        {
            return false;
        }
        try
        {
            // Submit the accepted connection to the shard owner thread.
            return runtime_->Submit(std::move(task));
        }
        catch (...)
        {
            return false;
        }
    }

    void Dispatch(std::function<void()> task)
    {
        if (InOwnerThread())
        {
            task();
        }
        else if (!Post(std::move(task)))
        {
            throw std::runtime_error("network shard is unavailable");
        }
    }

    bool AttachAccepted(int fd);
    bool StartConnections();
    void BeginDrainConnections();
    void CloseAllConnections();
    void OnConnectionClosed(int fd);

    void RequestBeginDrain()
    {
        if (InOwnerThread())
        {
            BeginDrainConnections();
            return;
        }
        (void)Post([this] { BeginDrainConnections(); });
    }

    void RequestStop()
    {
        if (external())
        {
            if (InOwnerThread())
            {
                CloseAllConnections();
            }
            return;
        }
        (void)Post([this] {
            CloseAllConnections();
            stop_requested_.store(true, std::memory_order_release);
        });
    }

    void Join()
    {
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

    runtime::Coroutine &runtime() const { return *runtime_; }
    runtime::AsyncSocket &socket() const { return *socket_; }
    bool InOwnerThread() const noexcept
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return runtime_ != nullptr && runtime_->InOwnerThread();
    }
    bool external() const noexcept { return index_ == 0; }
    std::size_t connection_count() const noexcept { return connection_count_.load(std::memory_order_relaxed); }

private:
    friend class RpcServer;

    void Run() noexcept
    {
        try
        {
            runtime::Coroutine::Options runtime_options;
            runtime_options.shared_stack_count = 4;
            runtime::Coroutine runtime(runtime_options);
            auto socket = std::make_unique<runtime::AsyncSocket>(runtime);
            {
                std::lock_guard<std::mutex> lock(mutex_);
                runtime_ = &runtime;
                socket_ = std::move(socket);
                initialized_ = true;
            }
            ready_.notify_all();
            runtime.RunUntil(stop_requested_);
            runtime.Join();
            {
                std::lock_guard<std::mutex> lock(mutex_);
                socket_.reset();
                runtime_ = nullptr;
            }
        }
        catch (...)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            failed_ = true;
            runtime_ = nullptr;
            ready_.notify_all();
        }
    }

    RpcServer &server_;
    std::size_t index_;
    mutable std::mutex mutex_;
    std::condition_variable ready_;
    runtime::Coroutine *runtime_ = nullptr;
    std::unique_ptr<runtime::AsyncSocket> socket_;
    std::unordered_map<int, std::shared_ptr<ServerConnection>> connections_;
    std::atomic<std::size_t> connection_count_{0};
    std::atomic_bool stop_requested_{false};
    bool initialized_ = false;
    bool failed_ = false;
    std::thread thread_;
};

struct RpcServer::RequestState {
    std::weak_ptr<ServerConnection> connection;
    NetworkShard *owner = nullptr;
    std::uint64_t request_id = 0;
    std::uint64_t deadline_ms = runtime::kNoTimeout;
    std::string idempotency_key;
    std::uint64_t idempotency_fingerprint = 0;
    std::shared_ptr<RpcContext> context;
    bool completed = false;

};

struct RpcServer::ServerConnection : public std::enable_shared_from_this<ServerConnection> {
    ServerConnection(std::shared_ptr<RpcServer> server, NetworkShard &shard, int fd)
        : server_(std::move(server)), shard_(shard), fd_(fd), caller_scope_(PeerScope(fd))
    {
        requests_.reserve(std::min<std::size_t>(server_->options_.max_inflight_per_connection, 256));
        encode_buffer_pool_.reserve(64);
        write_batch_.reserve(kWriteBatchBytes);
    }

    bool Start()
    {
        const std::shared_ptr<ServerConnection> self = shared_from_this();
        try
        {
            reader_ = shard_.runtime().CreateCo([self] { self->ReaderLoop(); });
            writer_ = shard_.runtime().CreateCo([self] { self->WriterLoop(); });
            shard_.runtime().Resume(reader_);
            shard_.runtime().Resume(writer_);
            return true;
        }
        catch (...)
        {
            Close();
            return false;
        }
    }

    bool AddRequest(const std::shared_ptr<RequestState> &state, std::size_t max_inflight)
    {
        if (closed_ || requests_.size() >= max_inflight || requests_.find(state->request_id) != requests_.end())
        {
            return false;
        }
        requests_.emplace(state->request_id, state);
        return true;
    }

    std::shared_ptr<RequestState> FindRequest(std::uint64_t request_id) const
    {
        const auto found = requests_.find(request_id);
        return found == requests_.end() ? nullptr : found->second;
    }

    bool QueueFrame(protocol::Frame frame)
    {
        if (closed_)
        {
            return false;
        }
        std::string bytes = AcquireEncodeBuffer();
        if (!protocol::FrameEncoder::Encode(frame, bytes))
        {
            ReleaseEncodeBuffer(std::move(bytes));
            Close();
            return false;
        }
        return QueueEncoded(std::move(bytes));
    }

    bool QueueFrame(protocol::FrameView frame)
    {
        if (closed_)
        {
            return false;
        }
        std::string bytes = AcquireEncodeBuffer();
        if (!protocol::FrameEncoder::Encode(frame, bytes))
        {
            ReleaseEncodeBuffer(std::move(bytes));
            Close();
            return false;
        }
        return QueueEncoded(std::move(bytes));
    }

private:
    bool QueueEncoded(std::string bytes)
    {
        if (bytes.size() > server_->options_.max_outbound_bytes_per_connection ||
            outbound_bytes_ > server_->options_.max_outbound_bytes_per_connection - bytes.size())
        {
            ReleaseEncodeBuffer(std::move(bytes));
            Close();
            return false;
        }
        outbound_bytes_ += bytes.size();
        outbound_.push_back(std::move(bytes));
        if (writer_ != nullptr && writer_->state() == runtime::CoroutineState::WAIT_EVENT)
        {
            shard_.runtime().Resume(writer_);
        }
        return true;
    }

public:

    void CompleteRequest(std::uint64_t request_id, protocol::Frame response)
    {
        requests_.erase(request_id);
        QueueFrame(std::move(response));
        MaybeCloseAfterDrain();
    }

    void BeginDrain()
    {
        draining_ = true;
        MaybeCloseAfterDrain();
    }

    bool draining() const noexcept { return draining_; }

    std::vector<std::shared_ptr<RequestState>> DetachRequests()
    {
        std::vector<std::shared_ptr<RequestState>> detached;
        detached.reserve(requests_.size());
        for (auto &item : requests_)
        {
            detached.push_back(std::move(item.second));
        }
        requests_.clear();
        return detached;
    }

    void Close()
    {
        if (closed_)
        {
            return;
        }
        closed_ = true;
        const int old_fd = fd_;
        fd_ = -1;
        outbound_.clear();
        outbound_bytes_ = 0;
        if (old_fd >= 0)
        {
            shard_.socket().Close(old_fd);
        }
        if (writer_ != nullptr && writer_->state() == runtime::CoroutineState::WAIT_EVENT)
        {
            shard_.runtime().Resume(writer_);
        }
        shard_.OnConnectionClosed(old_fd);
    }

    int fd() const noexcept { return fd_; }
    NetworkShard &owner() const noexcept { return shard_; }
    const std::string &caller_scope() const noexcept { return caller_scope_; }

private:
    void ReaderLoop()
    {
        while (!closed_)
        {
            constexpr std::size_t kReadSize = 16 * 1024;
            char *buffer = decoder_.PrepareWritable(kReadSize);
            if (buffer == nullptr)
            {
                Close();
                break;
            }
            const ssize_t received = shard_.socket().Read(fd_, buffer, kReadSize, runtime::kNoTimeout);
            if (received <= 0)
            {
                (void)decoder_.CommitWritable(0);
                Close();
                break;
            }
            if (decoder_.CommitWritable(static_cast<std::size_t>(received)) ==
                protocol::DecodeStatus::PROTOCOL_ERROR)
            {
                Close();
                break;
            }
            for (;;)
            {
                protocol::FrameView frame;
                const protocol::DecodeStatus status = decoder_.NextView(frame);
                if (status == protocol::DecodeStatus::NEED_MORE)
                {
                    break;
                }
                if (status != protocol::DecodeStatus::FRAME_READY)
                {
                    Close();
                    break;
                }
                server_->OnFrame(shared_from_this(), frame);
                if (closed_)
                {
                    break;
                }
            }
        }
        reader_.reset();
    }

    void WriterLoop()
    {
        while (!closed_)
        {
            if (outbound_.empty())
            {
                shard_.runtime().Yield();
                continue;
            }
            write_batch_.clear();
            while (!outbound_.empty() &&
                   (write_batch_.empty() || write_batch_.size() + outbound_.front().size() <= kWriteBatchBytes))
            {
                std::string bytes = std::move(outbound_.front());
                outbound_.pop_front();
                outbound_bytes_ -= bytes.size();
                write_batch_.append(bytes);
                ReleaseEncodeBuffer(std::move(bytes));
            }
            if (shard_.socket().WriteAll(fd_, write_batch_.data(), write_batch_.size(), runtime::kNoTimeout) !=
                static_cast<ssize_t>(write_batch_.size()))
            {
                Close();
                break;
            }
            MaybeCloseAfterDrain();
        }
        writer_.reset();
    }

    void MaybeCloseAfterDrain()
    {
        if (draining_ && requests_.empty() && outbound_.empty() && !closed_)
        {
            Close();
        }
    }

    std::string AcquireEncodeBuffer()
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

    void ReleaseEncodeBuffer(std::string buffer)
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

    std::shared_ptr<RpcServer> server_;
    NetworkShard &shard_;
    int fd_ = -1;
    std::string caller_scope_;
    protocol::FrameDecoder decoder_;
    runtime::Coroutine::RoutineHandle reader_;
    runtime::Coroutine::RoutineHandle writer_;
    std::unordered_map<std::uint64_t, std::shared_ptr<RequestState>> requests_;
    std::deque<std::string> outbound_;
    std::vector<std::string> encode_buffer_pool_;
    std::string write_batch_;
    std::size_t outbound_bytes_ = 0;
    bool draining_ = false;
    bool closed_ = false;
};

bool RpcServer::NetworkShard::AttachAccepted(int fd)
{
    return server_.AttachConnection(*this, fd);
}

bool RpcServer::NetworkShard::StartConnections()
{
    for (auto &entry : connections_)
    {
        if (!entry.second->Start())
        {
            return false;
        }
    }
    return true;
}

void RpcServer::NetworkShard::BeginDrainConnections()
{
    std::vector<std::shared_ptr<ServerConnection>> connections;
    connections.reserve(connections_.size());
    for (auto &entry : connections_)
    {
        connections.push_back(entry.second);
    }
    for (const auto &connection : connections)
    {
        connection->BeginDrain();
    }
}

void RpcServer::NetworkShard::CloseAllConnections()
{
    std::vector<std::shared_ptr<ServerConnection>> connections;
    connections.reserve(connections_.size());
    for (auto &entry : connections_)
    {
        connections.push_back(entry.second);
    }
    for (const auto &connection : connections)
    {
        connection->Close();
    }
}

void RpcServer::NetworkShard::OnConnectionClosed(int fd)
{
    const auto found = connections_.find(fd);
    if (found == connections_.end())
    {
        return;
    }
    const std::vector<std::shared_ptr<RequestState>> detached = found->second->DetachRequests();
    connections_.erase(found);
    connection_count_.fetch_sub(1, std::memory_order_relaxed);
    server_.active_connections_.fetch_sub(1, std::memory_order_relaxed);
    for (const auto &state : detached)
    {
        server_.Abandon(*state);
    }
    server_.MaybeFinishDrain();
}

std::shared_ptr<RpcServer> RpcServer::Create(runtime::Coroutine &runtime, RpcHandler handler, RpcServerOptions options)
{
    if (!runtime.InOwnerThread())
    {
        throw std::logic_error("RpcServer must be created on its Coroutine owner thread");
    }
    return std::shared_ptr<RpcServer>(new RpcServer(runtime, std::move(handler), options));
}

std::shared_ptr<RpcServer> RpcServer::Create(RpcServerOptions options)
{
    return std::shared_ptr<RpcServer>(new RpcServer(std::move(options)));
}

RpcServer::RpcServer(runtime::Coroutine &runtime, RpcHandler handler, RpcServerOptions options)
    : owned_runtime_(nullptr), runtime_(runtime), handler_(std::move(handler)), options_(std::move(options))
{
    Initialize();
}

RpcServer::RpcServer(RpcServerOptions options)
    : owned_runtime_(std::make_unique<runtime::Coroutine>(options.runtime_options)),
      runtime_(*owned_runtime_), options_(std::move(options)), dispatcher_(std::make_unique<ServiceDispatcher>())
{
    handler_ = [this](const ServerRequest &request) { return dispatcher_->Dispatch(request); };
    options_.execution_mode_selector = [this](const protocol::RequestMetadata &metadata) {
        return dispatcher_->ExecutionMode(metadata);
    };
    options_.execution_mode_view_selector = [this](const protocol::RequestMetadataView &metadata) {
        return dispatcher_->ExecutionMode(metadata);
    };
    options_.inline_view_handler = [this](const ServerRequestView &request) {
        return dispatcher_->DispatchInline(request);
    };
    Initialize();
}

void RpcServer::Initialize()
{
    if (!handler_ || options_.max_connections == 0 || options_.max_inflight_per_connection == 0 ||
        options_.max_inflight_requests == 0 || options_.max_outbound_bytes_per_connection == 0 ||
        options_.max_idempotency_entries == 0 || options_.network_threads == 0 ||
        options_.io_worker_threads == 0)
    {
        throw std::invalid_argument("invalid RPC server options");
    }
    if (options_.execution_mode_selector || options_.execution_mode == RpcExecutionMode::CPU_POOL)
    {
        executor_ = std::make_unique<BoundedExecutor>(options_.worker_threads, options_.max_worker_queue);
    }
    if (options_.execution_mode_selector || options_.execution_mode == RpcExecutionMode::IO_COROUTINE)
    {
        io_executor_ = std::make_unique<IoCoroutineExecutor>(options_.io_worker_threads, options_.max_worker_queue);
    }
    network_shards_.reserve(options_.network_threads);
    network_shards_.push_back(std::make_unique<NetworkShard>(*this, 0, &runtime_));
    for (std::size_t index = 1; index < options_.network_threads; ++index)
    {
        network_shards_.push_back(std::make_unique<NetworkShard>(*this, index, nullptr));
    }
}

RpcServer::~RpcServer()
{
    StopNow();
    if (io_executor_)
    {
        io_executor_->Join();
    }
    for (std::size_t index = 1; index < network_shards_.size(); ++index)
    {
        network_shards_[index]->RequestStop();
        network_shards_[index]->Join();
    }
}

bool RpcServer::AttachListenFd(int listen_fd)
{
    if (!runtime_.InOwnerThread() || listen_fd < 0 || listen_fd_ >= 0 || started_.load(std::memory_order_acquire) ||
        !SetNonBlocking(listen_fd))
    {
        return false;
    }
    listen_fd_ = listen_fd;
    return true;
}

bool RpcServer::Listen(const sockaddr *address, socklen_t address_length, int backlog)
{
    if (address == nullptr || address_length == 0 || backlog <= 0)
    {
        return false;
    }
    const int fd = socket(address->sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0)
    {
        return false;
    }
    int enabled = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
    if (bind(fd, address, address_length) != 0 || listen(fd, backlog) != 0 || !AttachListenFd(fd))
    {
        close(fd);
        return false;
    }
    return true;
}

bool RpcServer::AttachConnection(int fd)
{
    if (!runtime_.InOwnerThread())
    {
        return false;
    }
    return AttachConnection(*network_shards_.front(), fd);
}

bool RpcServer::Register(std::string service, std::string method, RpcHandler handler,
                         RpcExecutionMode mode, MethodCapability capability)
{
    if (!dispatcher_ || started_.load(std::memory_order_acquire) || stopped())
    {
        return false;
    }
    return dispatcher_->Register(std::move(service), std::move(method), std::move(handler), mode,
                                 std::move(capability));
}

bool RpcServer::RegisterInline(std::string service, std::string method, RpcHandler handler,
                               std::function<protocol::FrameView(const ServerRequestView &)> view_handler,
                               MethodCapability capability)
{
    if (!dispatcher_ || started_.load(std::memory_order_acquire) || stopped())
    {
        return false;
    }
    return dispatcher_->RegisterInline(std::move(service), std::move(method), std::move(handler),
                                       std::move(view_handler), std::move(capability));
}

bool RpcServer::AttachConnection(NetworkShard &shard, int fd)
{
    if (!shard.InOwnerThread() || fd < 0 || stopped() || !SetNonBlocking(fd) || !ConfigureTcpLatency(fd))
    {
        return false;
    }
    std::size_t active = active_connections_.load(std::memory_order_relaxed);
    do
    {
        if (active >= options_.max_connections)
        {
            return false;
        }
    } while (!active_connections_.compare_exchange_weak(active, active + 1, std::memory_order_acq_rel,
                                                         std::memory_order_relaxed));

    const auto connection = std::make_shared<ServerConnection>(shared_from_this(), shard, fd);
    const auto inserted = shard.connections_.emplace(fd, connection);
    if (!inserted.second)
    {
        active_connections_.fetch_sub(1, std::memory_order_relaxed);
        return false;
    }
    shard.connection_count_.fetch_add(1, std::memory_order_relaxed);
    accepted_connections_.fetch_add(1, std::memory_order_relaxed);
    if (started_.load(std::memory_order_acquire) && !connection->Start())
    {
        return false;
    }
    return true;
}

bool RpcServer::Start()
{
    if (!runtime_.InOwnerThread() || started_.load(std::memory_order_acquire) || stopped())
    {
        return false;
    }
    for (std::size_t index = 1; index < network_shards_.size(); ++index)
    {
        if (!network_shards_[index]->Start())
        {
            StopNow();
            return false;
        }
    }
    started_.store(true, std::memory_order_release);
    const std::shared_ptr<RpcServer> self = shared_from_this();
    worker_barrier_ = runtime_.CreateCo([self] {
        self->runtime_.Yield();
        self->worker_barrier_.reset();
    });
    runtime_.Resume(worker_barrier_);
    if (listen_fd_ >= 0)
    {
        acceptor_ = runtime_.CreateCo([self] { self->AcceptLoop(); });
        runtime_.Resume(acceptor_);
    }
    if (!network_shards_.front()->StartConnections())
    {
        StopNow();
        return false;
    }
    if (!StartRegistry())
    {
        StopNow();
        return false;
    }
    return true;
}

bool RpcServer::StartRegistry()
{
    if (!dispatcher_ || options_.zookeeper_hosts.empty())
    {
        return true;
    }
    auto services = dispatcher_->Capabilities();
    if (services.empty())
    {
        return false;
    }

    discovery::ZookeeperRegistryOptions registry_options;
    registry_options.hosts = options_.zookeeper_hosts;
    registry_options.service_root = options_.service_root;
    registry_options.host = options_.advertise_host;
    registry_options.port = options_.advertise_port;
    registry_options.session_timeout_ms = options_.zookeeper_session_timeout_ms;
    if (registry_options.port == 0 && listen_fd_ >= 0)
    {
        sockaddr_storage address{};
        socklen_t length = sizeof(address);
        if (getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&address), &length) == 0)
        {
            if (address.ss_family == AF_INET)
            {
                registry_options.port = ntohs(reinterpret_cast<const sockaddr_in *>(&address)->sin_port);
            }
            else if (address.ss_family == AF_INET6)
            {
                registry_options.port = ntohs(reinterpret_cast<const sockaddr_in6 *>(&address)->sin6_port);
            }
        }
    }
    if (registry_options.host.empty() || registry_options.port == 0)
    {
        return false;
    }
    registry_ = std::make_unique<discovery::ZookeeperServiceRegistry>(std::move(registry_options));
    if (!registry_->Start() || !registry_->Publish(std::move(services)))
    {
        registry_->Stop();
        return false;
    }
    return true;
}

void RpcServer::RunUntil(const std::atomic_bool &stop_requested)
{
    runtime_.RunUntil(stop_requested);
}

void RpcServer::Shutdown(std::uint64_t drain_timeout_ms)
{
    if (!runtime_.InOwnerThread())
    {
        return;
    }
    BeginDrain(drain_timeout_ms);
    runtime_.Join();
}

RpcServer::NetworkShard &RpcServer::SelectNetworkShard()
{
    const std::size_t start = next_network_shard_.fetch_add(1, std::memory_order_relaxed) % network_shards_.size();
    NetworkShard *selected = network_shards_[start].get();
    for (std::size_t count = 1; count < network_shards_.size(); ++count)
    {
        NetworkShard *candidate = network_shards_[(start + count) % network_shards_.size()].get();
        if (candidate->connection_count() < selected->connection_count())
        {
            selected = candidate;
        }
    }
    return *selected;
}

void RpcServer::AcceptLoop()
{
    NetworkShard &accept_shard = *network_shards_.front();
    while (accepting() && listen_fd_ >= 0)
    {
        const int client_fd = accept_shard.socket().Accept(listen_fd_, nullptr, nullptr, runtime::kNoTimeout);
        if (client_fd >= 0)
        {
            NetworkShard &target = SelectNetworkShard();
            if (target.InOwnerThread())
            {
                if (!target.AttachAccepted(client_fd))
                {
                    rejected_connections_.fetch_add(1, std::memory_order_relaxed);
                    close(client_fd);
                }
            }
            else
            {
                const std::shared_ptr<RpcServer> self = shared_from_this();
                NetworkShard *target_ptr = &target;
                if (!target.Post([self, target_ptr, client_fd] {
                        if (!target_ptr->AttachAccepted(client_fd))
                        {
                            self->rejected_connections_.fetch_add(1, std::memory_order_relaxed);
                            close(client_fd);
                        }
                    }))
                {
                    rejected_connections_.fetch_add(1, std::memory_order_relaxed);
                    close(client_fd);
                }
            }
            continue;
        }
        if (!accepting())
        {
            break;
        }
        if (errno != EINTR)
        {
            break;
        }
    }
    acceptor_.reset();
}

bool RpcServer::AcquireInflight()
{
    std::size_t current = inflight_count_.load(std::memory_order_relaxed);
    do
    {
        if (current >= options_.max_inflight_requests)
        {
            return false;
        }
    } while (!inflight_count_.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel,
                                                     std::memory_order_relaxed));
    return true;
}

void RpcServer::OnFrame(const std::shared_ptr<ServerConnection> &connection, protocol::FrameView frame_view)
{
    if (frame_view.header.message_type == protocol::MessageType::CANCEL)
    {
        Cancel(connection, frame_view.header.request_id);
        return;
    }
    if (frame_view.header.message_type != protocol::MessageType::REQUEST)
    {
        connection->Close();
        return;
    }
    requests_received_.fetch_add(1, std::memory_order_relaxed);
    if (!accepting())
    {
        requests_rejected_.fetch_add(1, std::memory_order_relaxed);
        connection->QueueFrame(MakeStatusResponse(frame_view.header.request_id, RpcStatus::RESOURCE_EXHAUSTED));
        return;
    }

    protocol::RequestMetadataView metadata_view;
    if (!protocol::DecodeRequestMetadataView(frame_view.metadata, metadata_view))
    {
        requests_rejected_.fetch_add(1, std::memory_order_relaxed);
        connection->QueueFrame(MakeStatusResponse(frame_view.header.request_id, RpcStatus::INVALID_ARGUMENT));
        return;
    }
    const std::uint64_t now = runtime::TimerWheel::NowMs();
    const std::uint64_t deadline = AddDeadline(now, metadata_view.timeout_ms);
    if (deadline != runtime::kNoTimeout && deadline <= now)
    {
        requests_timed_out_.fetch_add(1, std::memory_order_relaxed);
        connection->QueueFrame(MakeStatusResponse(frame_view.header.request_id, RpcStatus::DEADLINE_EXCEEDED));
        return;
    }

    std::optional<protocol::RequestMetadata> owned_metadata;
    RpcExecutionMode execution_mode = options_.execution_mode;
    if (options_.execution_mode_view_selector)
    {
        execution_mode = options_.execution_mode_view_selector(metadata_view);
    }
    else if (options_.execution_mode_selector)
    {
        owned_metadata.emplace();
        owned_metadata->service.assign(metadata_view.service.data(), metadata_view.service.size());
        owned_metadata->method.assign(metadata_view.method.data(), metadata_view.method.size());
        owned_metadata->idempotency_key.assign(metadata_view.idempotency_key.data(), metadata_view.idempotency_key.size());
        owned_metadata->application_metadata.assign(metadata_view.application_metadata.data(),
                                                    metadata_view.application_metadata.size());
        owned_metadata->timeout_ms = metadata_view.timeout_ms;
        execution_mode = options_.execution_mode_selector(*owned_metadata);
    }
    if (execution_mode == RpcExecutionMode::NETWORK_INLINE && metadata_view.idempotency_key.empty() &&
        options_.inline_view_handler)
    {
        if (!AcquireInflight())
        {
            requests_rejected_.fetch_add(1, std::memory_order_relaxed);
            connection->QueueFrame(MakeStatusResponse(frame_view.header.request_id, RpcStatus::RESOURCE_EXHAUSTED));
            return;
        }

        RpcContext context(deadline);
        ServerRequestView request;
        request.request_id = frame_view.header.request_id;
        request.deadline_ms = deadline;
        request.metadata = metadata_view;
        request.frame = frame_view;
        request.context = &context;

        protocol::FrameView response;
        try
        {
            response = options_.inline_view_handler(request);
            response.header.message_type = protocol::MessageType::RESPONSE;
            response.header.request_id = request.request_id;
        }
        catch (...)
        {
            response.header.flags = static_cast<std::uint32_t>(RpcStatus::INTERNAL);
            response.header.message_type = protocol::MessageType::RESPONSE;
            response.header.request_id = request.request_id;
            response.metadata = {};
            response.body = {};
        }
        if (deadline != runtime::kNoTimeout && runtime::TimerWheel::NowMs() >= deadline)
        {
            context.Cancel();
            requests_timed_out_.fetch_add(1, std::memory_order_relaxed);
            response.header.flags = static_cast<std::uint32_t>(RpcStatus::DEADLINE_EXCEEDED);
            response.metadata = {};
            response.body = {};
        }
        inflight_count_.fetch_sub(1, std::memory_order_relaxed);
        requests_completed_.fetch_add(1, std::memory_order_relaxed);
        connection->QueueFrame(response);
        MaybeFinishDrain();
        return;
    }

    if (!owned_metadata)
    {
        owned_metadata.emplace();
        owned_metadata->service.assign(metadata_view.service.data(), metadata_view.service.size());
        owned_metadata->method.assign(metadata_view.method.data(), metadata_view.method.size());
        owned_metadata->idempotency_key.assign(metadata_view.idempotency_key.data(), metadata_view.idempotency_key.size());
        owned_metadata->application_metadata.assign(metadata_view.application_metadata.data(),
                                                    metadata_view.application_metadata.size());
        owned_metadata->timeout_ms = metadata_view.timeout_ms;
    }
    protocol::RequestMetadata metadata = std::move(*owned_metadata);
    protocol::Frame frame;
    frame.header = frame_view.header;
    frame.metadata.assign(frame_view.metadata.data(), frame_view.metadata.size());
    frame.body.assign(frame_view.body.data(), frame_view.body.size());

    auto state = std::make_shared<RequestState>();
    state->connection = connection;
    state->owner = &connection->owner();
    state->request_id = frame.header.request_id;
    state->deadline_ms = deadline;
    state->context = std::make_shared<RpcContext>(deadline);
    state->idempotency_key = metadata.idempotency_key.empty()
                                 ? std::string()
                                 : BuildIdempotencyKey(connection->caller_scope(), metadata.service, metadata.method,
                                                       metadata.idempotency_key);
    state->idempotency_fingerprint = BodyFingerprint(frame.body);
    if (!AcquireInflight())
    {
        requests_rejected_.fetch_add(1, std::memory_order_relaxed);
        connection->QueueFrame(MakeStatusResponse(frame.header.request_id, RpcStatus::RESOURCE_EXHAUSTED));
        return;
    }
    if (!connection->AddRequest(state, options_.max_inflight_per_connection))
    {
        inflight_count_.fetch_sub(1, std::memory_order_relaxed);
        requests_rejected_.fetch_add(1, std::memory_order_relaxed);
        connection->QueueFrame(MakeStatusResponse(frame.header.request_id, RpcStatus::RESOURCE_EXHAUSTED));
        return;
    }
    if (!state->idempotency_key.empty())
    {
        protocol::Frame cached_response;
        bool use_cached_response = false;
        bool reject_cache_capacity = false;
        bool conflicting_request = false;
        {
            std::lock_guard<std::mutex> lock(idempotency_mutex_);
            const auto existing = idempotency_.find(state->idempotency_key);
            if (existing != idempotency_.end())
            {
                duplicate_requests_.fetch_add(1, std::memory_order_relaxed);
                if (existing->second.body_fingerprint != state->idempotency_fingerprint ||
                    existing->second.body != frame.body)
                {
                    conflicting_request = true;
                }
                else if (existing->second.pending)
                {
                    existing->second.waiters.push_back(state);
                    return;
                }
                else
                {
                    cached_response = existing->second.response;
                    use_cached_response = true;
                }
            }
            else
            {
                PruneIdempotencyCache();
                if (idempotency_.size() >= options_.max_idempotency_entries)
                {
                    reject_cache_capacity = true;
                }
                else
                {
                    IdempotencyRecord record;
                    record.body_fingerprint = state->idempotency_fingerprint;
                    record.body = frame.body;
                    record.waiters.push_back(state);
                    idempotency_.emplace(state->idempotency_key, std::move(record));
                }
            }
        }
        if (use_cached_response)
        {
            Finish(*state, std::move(cached_response));
            return;
        }
        if (conflicting_request)
        {
            Finish(*state, MakeStatusResponse(state->request_id, RpcStatus::ALREADY_EXISTS));
            return;
        }
        if (reject_cache_capacity)
        {
            requests_rejected_.fetch_add(1, std::memory_order_relaxed);
            Finish(*state, MakeStatusResponse(state->request_id, RpcStatus::RESOURCE_EXHAUSTED));
            return;
        }
    }

    ServerRequest request;
    request.request_id = state->request_id;
    request.deadline_ms = state->deadline_ms;
    request.metadata = std::move(metadata);
    request.frame = std::move(frame);
    request.context = state->context;
    const std::shared_ptr<RequestState> retained_state = state;
    if (execution_mode == RpcExecutionMode::NETWORK_INLINE)
    {
        protocol::Frame response;
        bool cache_response = true;
        if (request.context->IsCancelled())
        {
            response = MakeStatusResponse(request.request_id, RpcStatus::CANCELLED);
            cache_response = false;
        }
        else
        {
            try
            {
                response = handler_(request);
                response.header.message_type = protocol::MessageType::RESPONSE;
                response.header.request_id = request.request_id;
            }
            catch (...)
            {
                response = MakeStatusResponse(request.request_id, RpcStatus::INTERNAL);
                cache_response = false;
            }
        }
        if (state->idempotency_key.empty())
        {
            Finish(*state, std::move(response));
        }
        else
        {
            FinishIdempotent(state->idempotency_key, std::move(response), cache_response);
        }
        return;
    }
    if (execution_mode == RpcExecutionMode::IO_COROUTINE)
    {
        const std::shared_ptr<RpcServer> self = shared_from_this();
        try
        {
            if (!io_executor_->TrySubmit([self, retained_state, request = std::move(request)](runtime::Coroutine &worker) mutable {
                protocol::Frame response;
                bool cache_response = true;
                bool bound = false;
                if (request.context->IsCancelled())
                {
                    response = MakeStatusResponse(request.request_id, RpcStatus::CANCELLED);
                    cache_response = false;
                }
                else
                {
                    request.context->Bind(worker, worker.Current());
                    bound = true;
                    try
                    {
                        response = self->handler_(request);
                        response.header.message_type = protocol::MessageType::RESPONSE;
                        response.header.request_id = request.request_id;
                    }
                    catch (...)
                    {
                        response = MakeStatusResponse(request.request_id, RpcStatus::INTERNAL);
                        cache_response = false;
                    }
                }
                if (bound)
                {
                    request.context->Unbind();
                }
                NetworkShard *owner = retained_state->owner;
                if (owner != nullptr)
                {
                    (void)owner->Post([self, completion = Completion{retained_state, std::move(response), cache_response}]() mutable {
                        self->OnWorkerCompletion(std::move(completion));
                    });
                }
            }))
            {
                throw std::runtime_error("I/O coroutine worker is unavailable");
            }
        }
        catch (...)
        {
            requests_rejected_.fetch_add(1, std::memory_order_relaxed);
            protocol::Frame response = MakeStatusResponse(state->request_id, RpcStatus::RESOURCE_EXHAUSTED);
            if (state->idempotency_key.empty())
            {
                Finish(*state, std::move(response));
            }
            else
            {
                FinishIdempotent(state->idempotency_key, std::move(response), false);
            }
        }
        return;
    }

    if (!executor_->TrySubmit([this, retained_state, request = std::move(request)]() mutable {
            protocol::Frame response;
            bool cache_response = true;
            if (request.context->IsCancelled())
            {
                response = MakeStatusResponse(request.request_id, RpcStatus::CANCELLED);
                cache_response = false;
            }
            else
            {
                try
                {
                    response = handler_(request);
                    response.header.message_type = protocol::MessageType::RESPONSE;
                    response.header.request_id = request.request_id;
                }
                catch (...)
                {
                    response = MakeStatusResponse(request.request_id, RpcStatus::INTERNAL);
                    cache_response = false;
                }
            }
            const std::shared_ptr<RpcServer> self = shared_from_this();
            NetworkShard *owner = retained_state->owner;
            if (owner != nullptr)
            {
                (void)owner->Post([self, completion = Completion{retained_state, std::move(response), cache_response}]() mutable {
                    self->OnWorkerCompletion(std::move(completion));
                });
            }
        }))
    {
        requests_rejected_.fetch_add(1, std::memory_order_relaxed);
        protocol::Frame response = MakeStatusResponse(state->request_id, RpcStatus::RESOURCE_EXHAUSTED);
        if (state->idempotency_key.empty())
        {
            Finish(*state, std::move(response));
        }
        else
        {
            FinishIdempotent(state->idempotency_key, std::move(response), false);
        }
    }
}

void RpcServer::OnWorkerCompletion(Completion completion)
{
    if (!completion.state)
    {
        return;
    }
    if (completion.state->idempotency_key.empty())
    {
        if (completion.state->completed)
        {
            return;
        }
        Finish(*completion.state, std::move(completion.response));
    }
    else
    {
        FinishIdempotent(completion.state->idempotency_key, std::move(completion.response), completion.cache_response);
    }
}

void RpcServer::Finish(RequestState &state, protocol::Frame response)
{
    if (state.completed)
    {
        return;
    }
    state.completed = true;
    if (state.deadline_ms != runtime::kNoTimeout && runtime::TimerWheel::NowMs() >= state.deadline_ms)
    {
        state.context->Cancel();
        requests_timed_out_.fetch_add(1, std::memory_order_relaxed);
        response = MakeStatusResponse(state.request_id, RpcStatus::DEADLINE_EXCEEDED);
    }
    inflight_count_.fetch_sub(1, std::memory_order_relaxed);
    requests_completed_.fetch_add(1, std::memory_order_relaxed);
    response.header.message_type = protocol::MessageType::RESPONSE;
    response.header.request_id = state.request_id;
    if (const std::shared_ptr<ServerConnection> connection = state.connection.lock())
    {
        connection->CompleteRequest(state.request_id, std::move(response));
    }
    MaybeFinishDrain();
}

void RpcServer::FinishIdempotent(const std::string &key, protocol::Frame response, bool cache_response)
{
    std::vector<std::weak_ptr<RequestState>> waiters;
    {
        std::lock_guard<std::mutex> lock(idempotency_mutex_);
        const auto found = idempotency_.find(key);
        if (found == idempotency_.end())
        {
            return;
        }
        waiters = std::move(found->second.waiters);
        if (cache_response)
        {
            found->second.pending = false;
            found->second.response = response;
            completed_idempotency_keys_.push_back(key);
            PruneIdempotencyCache();
        }
        else
        {
            idempotency_.erase(found);
        }
    }
    for (std::weak_ptr<RequestState> &waiter : waiters)
    {
        if (const std::shared_ptr<RequestState> state = waiter.lock())
        {
            FinishOnOwner(state, response);
        }
    }
}

void RpcServer::FinishOnOwner(const std::shared_ptr<RequestState> &state, protocol::Frame response)
{
    if (!state || state->owner == nullptr)
    {
        return;
    }
    NetworkShard *owner = state->owner;
    const std::shared_ptr<RpcServer> self = shared_from_this();
    owner->Dispatch([self, state, response = std::move(response)]() mutable {
        self->Finish(*state, std::move(response));
    });
}

void RpcServer::Cancel(const std::shared_ptr<ServerConnection> &connection, std::uint64_t request_id)
{
    const std::shared_ptr<RequestState> state = connection->FindRequest(request_id);
    if (state == nullptr)
    {
        return;
    }
    cancellations_.fetch_add(1, std::memory_order_relaxed);
    state->context->Cancel();
    if (!state->idempotency_key.empty())
    {
        std::lock_guard<std::mutex> lock(idempotency_mutex_);
        const auto found = idempotency_.find(state->idempotency_key);
        if (found != idempotency_.end() && found->second.pending)
        {
            auto &waiters = found->second.waiters;
            waiters.erase(std::remove_if(waiters.begin(), waiters.end(), [&](const auto &waiter) {
                              const auto locked = waiter.lock();
                              return !locked || locked.get() == state.get();
                          }),
                          waiters.end());
            if (waiters.empty())
            {
                idempotency_.erase(found);
            }
        }
    }
    Finish(*state, MakeStatusResponse(request_id, RpcStatus::CANCELLED));
}

void RpcServer::Abandon(RequestState &state)
{
    if (state.completed)
    {
        return;
    }
    state.completed = true;
    state.context->Cancel();
    inflight_count_.fetch_sub(1, std::memory_order_relaxed);
    if (!state.idempotency_key.empty())
    {
        std::lock_guard<std::mutex> lock(idempotency_mutex_);
        const auto found = idempotency_.find(state.idempotency_key);
        if (found != idempotency_.end() && found->second.pending)
        {
            auto &waiters = found->second.waiters;
            waiters.erase(std::remove_if(waiters.begin(), waiters.end(), [&](const auto &waiter) {
                              const auto locked = waiter.lock();
                              return !locked || locked.get() == &state;
                          }),
                          waiters.end());
            if (waiters.empty())
            {
                idempotency_.erase(found);
            }
        }
    }
}

void RpcServer::BeginDrain(std::uint64_t timeout_ms)
{
    if (!runtime_.InOwnerThread() || stopped() || !accepting())
    {
        return;
    }
    accepting_.store(false, std::memory_order_release);
    if (listen_fd_ >= 0)
    {
        network_shards_.front()->socket().Close(listen_fd_);
        listen_fd_ = -1;
    }
    if (timeout_ms == 0)
    {
        FinishDrainTimeout();
        return;
    }
    const std::weak_ptr<RpcServer> weak = weak_from_this();
    drain_waiter_ = runtime_.CreateCo([weak, timeout_ms] {
        runtime::Coroutine *runtime = nullptr;
        if (const std::shared_ptr<RpcServer> server = weak.lock())
        {
            runtime = &server->runtime_;
        }
        if (runtime == nullptr)
        {
            return;
        }
        runtime->YieldFor(timeout_ms);
        const std::shared_ptr<RpcServer> server = weak.lock();
        if (!server)
        {
            return;
        }
        server->drain_waiter_.reset();
        if (!server->stopped())
        {
            server->FinishDrainTimeout();
        }
    });
    runtime_.Resume(drain_waiter_);

    for (auto &shard : network_shards_)
    {
        shard->RequestBeginDrain();
    }
    if (!stopped())
    {
        MaybeFinishDrain();
    }
}

void RpcServer::MaybeFinishDrain()
{
    if (accepting() || stopped() || inflight_count() != 0 ||
        active_connections_.load(std::memory_order_relaxed) != 0)
    {
        return;
    }
    if (runtime_.InOwnerThread())
    {
        StopNow();
        return;
    }
    const std::weak_ptr<RpcServer> weak = weak_from_this();
    // The caller is not the owner thread, so submit through the runtime.
    runtime_.Submit([weak] {
        if (const auto self = weak.lock())
        {
            self->MaybeFinishDrain();
        }
    });
}

void RpcServer::FinishDrainTimeout()
{
    StopNow();
}

void RpcServer::StopNow()
{
    if (!runtime_.InOwnerThread())
    {
        const std::weak_ptr<RpcServer> weak = weak_from_this();
        runtime_.Submit([weak] {
            if (const auto self = weak.lock())
            {
                self->StopNow();
            }
        });
        return;
    }
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
    {
        return;
    }
    if (registry_)
    {
        registry_->Unpublish();
        registry_->Stop();
    }
    accepting_.store(false, std::memory_order_release);
    if (drain_waiter_ && drain_waiter_->state() == runtime::CoroutineState::WAIT_TIMER)
    {
        runtime_.Resume(drain_waiter_);
    }
    if (listen_fd_ >= 0)
    {
        network_shards_.front()->socket().Close(listen_fd_);
        listen_fd_ = -1;
    }
    for (auto &shard : network_shards_)
    {
        if (shard->InOwnerThread())
        {
            shard->CloseAllConnections();
        }
        else
        {
            (void)shard->Post([shard = shard.get()] { shard->CloseAllConnections(); });
        }
    }
    if (executor_)
    {
        executor_->Stop();
    }
    if (io_executor_)
    {
        const std::weak_ptr<RpcServer> weak = weak_from_this();
        io_executor_->RequestStop([weak] {
            if (const std::shared_ptr<RpcServer> self = weak.lock())
            {
                // The callback runs on the worker runtime, not self->runtime_.
                self->runtime_.Submit([self] {
                    self->FinalizeWorkerShutdown();
                });
            }
        });
    }
    else
    {
        FinalizeWorkerShutdown();
    }
}

void RpcServer::FinalizeWorkerShutdown()
{
    if (workers_finalized_)
    {
        return;
    }
    workers_finalized_ = true;
    if (io_executor_)
    {
        io_executor_->Join();
    }
    for (std::size_t index = 1; index < network_shards_.size(); ++index)
    {
        network_shards_[index]->RequestStop();
    }
    for (std::size_t index = 1; index < network_shards_.size(); ++index)
    {
        network_shards_[index]->Join();
    }
    if (worker_barrier_ && worker_barrier_->state() == runtime::CoroutineState::WAIT_EVENT)
    {
        runtime_.Resume(worker_barrier_);
    }
}

RpcServerMetrics RpcServer::metrics() const noexcept
{
    RpcServerMetrics snapshot;
    snapshot.accepted_connections = accepted_connections_.load(std::memory_order_relaxed);
    snapshot.rejected_connections = rejected_connections_.load(std::memory_order_relaxed);
    snapshot.requests_received = requests_received_.load(std::memory_order_relaxed);
    snapshot.requests_rejected = requests_rejected_.load(std::memory_order_relaxed);
    snapshot.requests_completed = requests_completed_.load(std::memory_order_relaxed);
    snapshot.requests_timed_out = requests_timed_out_.load(std::memory_order_relaxed);
    snapshot.duplicate_requests = duplicate_requests_.load(std::memory_order_relaxed);
    snapshot.cancellations = cancellations_.load(std::memory_order_relaxed);
    return snapshot;
}

void RpcServer::PruneIdempotencyCache()
{
    while (idempotency_.size() >= options_.max_idempotency_entries && !completed_idempotency_keys_.empty())
    {
        const std::string key = std::move(completed_idempotency_keys_.front());
        completed_idempotency_keys_.pop_front();
        const auto found = idempotency_.find(key);
        if (found != idempotency_.end() && !found->second.pending)
        {
            idempotency_.erase(found);
        }
    }
}

} // namespace rpc::transport
