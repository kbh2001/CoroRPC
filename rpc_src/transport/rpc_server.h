#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <functional>
#include <memory>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <sys/socket.h>

#include "protocol/frame.h"
#include "protocol/rpc_metadata.h"
#include "runtime/async_socket.h"
#include "transport/service_endpoint.h"

namespace rpc::discovery { class ZookeeperServiceRegistry; }

namespace rpc::transport {

class ServiceDispatcher;

struct ServerRequest {
    std::uint64_t request_id = 0;
    std::uint64_t deadline_ms = runtime::kNoTimeout;
    protocol::RequestMetadata metadata;
    protocol::Frame frame;
    std::shared_ptr<class RpcContext> context;
};

struct ServerRequestView {
    std::uint64_t request_id = 0;
    std::uint64_t deadline_ms = runtime::kNoTimeout;
    protocol::RequestMetadataView metadata;
    protocol::FrameView frame;
    class RpcContext *context = nullptr;
};

enum class RpcExecutionMode : std::uint8_t { CPU_POOL, IO_COROUTINE, NETWORK_INLINE };
// Optional per-request routing. IO_COROUTINE handlers use coroutine-aware
// nonblocking I/O; CPU_POOL handlers run on the bounded thread pool;
// NETWORK_INLINE handlers execute on the connection's NetworkShard and must
// never block or use RpcContext coroutine waits.
using RpcExecutionModeSelector = std::function<RpcExecutionMode(const protocol::RequestMetadata &)>;
using RpcExecutionModeViewSelector = std::function<RpcExecutionMode(const protocol::RequestMetadataView &)>;

// Cancellation is cooperative. A handler polls the context between blocking
// units of work; the runtime never tries to terminate arbitrary C++ code.
class RpcContext {
public:
    explicit RpcContext(std::uint64_t deadline_ms) : deadline_ms_(deadline_ms) {}

    bool IsCancelled() const noexcept { return cancelled_.load(std::memory_order_acquire); }
    bool DeadlineExceeded(std::uint64_t now_ms) const noexcept
    {
        return deadline_ms_ != runtime::kNoTimeout && now_ms >= deadline_ms_;
    }
    std::uint64_t deadline_ms() const noexcept { return deadline_ms_; }

    runtime::WaitResult AwaitReadable(int fd, std::uint64_t timeout_ms = runtime::kNoTimeout) const;
    runtime::WaitResult AwaitWritable(int fd, std::uint64_t timeout_ms = runtime::kNoTimeout) const;
    void YieldFor(std::uint64_t milliseconds) const;

private:
    friend class RpcServer;
    std::uint64_t EffectiveTimeout(std::uint64_t timeout_ms) const noexcept;
    void Bind(runtime::Coroutine &runtime, runtime::Coroutine::RoutineHandle routine);
    void Unbind();
    void Cancel();

    std::uint64_t deadline_ms_;
    mutable std::atomic<bool> cancelled_{false};
    mutable std::mutex binding_mutex_;
    runtime::Coroutine *runtime_ = nullptr;
    runtime::Coroutine::RoutineHandle routine_;
};

struct RpcServerOptions {
    std::size_t max_connections = 1024;
    std::size_t max_inflight_per_connection = 256;
    std::size_t max_inflight_requests = 8192;
    std::size_t max_outbound_bytes_per_connection = 16 * 1024 * 1024;
    // Network shards own accepted connections and perform read/decode/write.
    // Shard 0 uses the caller-provided Coroutine in explicit mode, or the
    // server-owned Coroutine in managed mode; additional shards own a
    // dedicated thread and Coroutine runtime.
    std::size_t network_threads = 1;
    // Each I/O worker owns one Coroutine runtime and executes IO_COROUTINE
    // handlers. Network and business I/O remain isolated by Post().
    std::size_t io_worker_threads = 1;
    std::size_t worker_threads = 4;
    std::size_t max_worker_queue = 4096;
    std::size_t max_idempotency_entries = 8192;
    RpcExecutionMode execution_mode = RpcExecutionMode::CPU_POOL;
    RpcExecutionModeSelector execution_mode_selector;
    RpcExecutionModeViewSelector execution_mode_view_selector;
    // Runs only on the connection's network owner thread. Request views must
    // not be retained; returned views are encoded before the decoder advances.
    std::function<protocol::FrameView(const ServerRequestView &)> inline_view_handler;

    // Managed server configuration. An empty hosts value disables ZooKeeper
    // registration; the explicit registry class remains available for custom
    // lifecycle control.
    runtime::Coroutine::Options runtime_options;
    std::string zookeeper_hosts;
    std::string service_root = "/rpc";
    std::string advertise_host = "127.0.0.1";
    std::uint16_t advertise_port = 0;
    int zookeeper_session_timeout_ms = 10 * 1000;

};

struct RpcServerMetrics {
    std::uint64_t accepted_connections = 0;
    std::uint64_t rejected_connections = 0;
    std::uint64_t requests_received = 0;
    std::uint64_t requests_rejected = 0;
    std::uint64_t requests_completed = 0;
    std::uint64_t requests_timed_out = 0;
    std::uint64_t duplicate_requests = 0;
    std::uint64_t cancellations = 0;
};

// CPU_POOL handlers may block. IO_COROUTINE handlers run as one service
// coroutine per RPC on one of the dedicated I/O worker threads.
using RpcHandler = std::function<protocol::Frame(const ServerRequest &)>;

// A sharded server. Every connection is pinned to one network Coroutine owner;
// I/O handlers and CPU handlers execute on separate worker domains.
class RpcServer : public std::enable_shared_from_this<RpcServer> {
public:
    static std::shared_ptr<RpcServer> Create(runtime::Coroutine &runtime, RpcHandler handler,
                                             RpcServerOptions options = {});
    // Managed mode: owns the runtime, dispatcher and optional ZooKeeper
    // registry. Listen() is still explicit because it accepts a sockaddr.
    static std::shared_ptr<RpcServer> Create(RpcServerOptions options = {});
    ~RpcServer();

    RpcServer(const RpcServer &) = delete;
    RpcServer &operator=(const RpcServer &) = delete;

    bool AttachListenFd(int listen_fd);
    bool Listen(const sockaddr *address, socklen_t address_length, int backlog = 256);
    // Useful for tests and embedded use. Takes ownership of a connected fd.
    bool AttachConnection(int fd);
    // Managed mode only. Service registration must finish before Start().
    bool Register(std::string service, std::string method, RpcHandler handler,
                  RpcExecutionMode mode = RpcExecutionMode::CPU_POOL,
                  MethodCapability capability = {"v1", "raw", "bytes", "bytes"});
    bool RegisterInline(std::string service, std::string method, RpcHandler handler,
                        std::function<protocol::FrameView(const ServerRequestView &)> view_handler,
                        MethodCapability capability = {"v1", "raw", "bytes", "bytes"});
    bool Start();

    void RunUntil(const std::atomic_bool &stop_requested);
    void Shutdown(std::uint64_t drain_timeout_ms = 10 * 1000);

    // Stop accepting new connections/requests, finish accepted work until the
    // deadline, then fail any remaining calls and close all connections.
    void BeginDrain(std::uint64_t timeout_ms);
    void StopNow();

    bool accepting() const noexcept { return accepting_.load(std::memory_order_acquire); }
    bool stopped() const noexcept { return stopped_.load(std::memory_order_acquire); }
    std::size_t inflight_count() const noexcept { return inflight_count_.load(std::memory_order_relaxed); }
    RpcServerMetrics metrics() const noexcept;

private:
    struct ServerConnection;
    struct RequestState;
    class NetworkShard;
    class BoundedExecutor;
    class IoCoroutineExecutor;
    struct Completion {
        std::shared_ptr<RequestState> state;
        protocol::Frame response;
        bool cache_response = true;
    };
    struct IdempotencyRecord {
        bool pending = true;
        std::uint64_t body_fingerprint = 0;
        std::string body;
        protocol::Frame response;
        std::vector<std::weak_ptr<RequestState>> waiters;
    };

    RpcServer(runtime::Coroutine &runtime, RpcHandler handler, RpcServerOptions options);
    explicit RpcServer(RpcServerOptions options);

    void Initialize();
    bool StartRegistry();

    void AcceptLoop();
    bool AttachConnection(NetworkShard &shard, int fd);
    NetworkShard &SelectNetworkShard();
    void OnFrame(const std::shared_ptr<ServerConnection> &connection, protocol::FrameView frame);
    void OnWorkerCompletion(Completion completion);
    bool AcquireInflight();
    void Finish(RequestState &state, protocol::Frame response);
    void FinishIdempotent(const std::string &key, protocol::Frame response, bool cache_response);
    void FinishOnOwner(const std::shared_ptr<RequestState> &state, protocol::Frame response);
    void Abandon(RequestState &state);
    void Cancel(const std::shared_ptr<ServerConnection> &connection, std::uint64_t request_id);
    void MaybeFinishDrain();
    void FinishDrainTimeout();
    void FinalizeWorkerShutdown();
    void PruneIdempotencyCache();

    std::unique_ptr<runtime::Coroutine> owned_runtime_;
    runtime::Coroutine &runtime_;
    RpcHandler handler_;
    RpcServerOptions options_;
    std::unique_ptr<ServiceDispatcher> dispatcher_;
    std::unique_ptr<discovery::ZookeeperServiceRegistry> registry_;
    std::unique_ptr<BoundedExecutor> executor_;
    std::unique_ptr<IoCoroutineExecutor> io_executor_;
    std::vector<std::unique_ptr<NetworkShard>> network_shards_;
    int listen_fd_ = -1;
    runtime::Coroutine::RoutineHandle acceptor_;
    runtime::Coroutine::RoutineHandle worker_barrier_;
    runtime::Coroutine::RoutineHandle drain_waiter_;
    std::atomic<std::size_t> next_network_shard_{0};
    std::atomic<bool> started_{false};
    bool workers_finalized_ = false;
    std::atomic<bool> accepting_{true};
    std::atomic<bool> stopped_{false};
    std::atomic<std::size_t> inflight_count_{0};
    std::atomic<std::size_t> active_connections_{0};
    std::atomic<std::uint64_t> accepted_connections_{0};
    std::atomic<std::uint64_t> rejected_connections_{0};
    std::atomic<std::uint64_t> requests_received_{0};
    std::atomic<std::uint64_t> requests_rejected_{0};
    std::atomic<std::uint64_t> requests_completed_{0};
    std::atomic<std::uint64_t> requests_timed_out_{0};
    std::atomic<std::uint64_t> duplicate_requests_{0};
    std::atomic<std::uint64_t> cancellations_{0};
    std::mutex idempotency_mutex_;
    std::unordered_map<std::string, IdempotencyRecord> idempotency_;
    std::deque<std::string> completed_idempotency_keys_;
};

} // namespace rpc::transport
