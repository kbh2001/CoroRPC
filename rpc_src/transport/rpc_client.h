#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "transport/multiplexed_connection.h"
#include "transport/rpc_call_options.h"
#include "transport/service_endpoint.h"
#include "runtime/coro_result.h"
#include "task/task.h"
#include "memory/task_pool.h"

namespace rpc::discovery { class ZookeeperServiceDiscovery; }

namespace rpc::transport {

struct RpcClientOptions {
    std::string zookeeper_hosts = "127.0.0.1:2181";
    std::string service_root = "/rpc";
    int zookeeper_session_timeout_ms = 10 * 1000;
    std::uint64_t connect_timeout_ms = 1000;
    ConnectionLimits connection_limits{};
    CircuitBreakerOptions circuit_breaker;
    RetryOptions retry;

    // Number of dedicated I/O shards started by the client.
    std::size_t io_threads = 1;
    std::size_t max_calls_per_shard = 4096;
    std::size_t soft_capacity_percent = 80;
};

// Fan-in point for a coroutine that issued several calls. This is the transport
// specialization of the generic coroutine primitive; see runtime/coro_result.h
// for why it must be heap resident.
using CroResult = runtime::CoroResult<CallResult>;

class RpcClient : public std::enable_shared_from_this<RpcClient> {
public:
    // Dedicated mode. io_threads is the global shard limit, not the number of
    // threads created at startup.
    static std::shared_ptr<RpcClient> Create(RpcClientOptions options);

    // Dedicated mode with a caller-provided immutable discovery snapshot.
    // This is useful for static deployments and benchmarks that must exclude
    // registry timing from the transport measurement.
    static std::shared_ptr<RpcClient> CreateStatic(
        std::unordered_map<std::string, std::unordered_map<std::string, Endpoint>> services,
        RpcClientOptions options = {});
    static std::shared_ptr<RpcClient> CreateStatic(runtime::Coroutine &runtime,
                                                   std::unordered_map<std::string,
                                                       std::unordered_map<std::string, Endpoint>> services,
                                                   RpcClientOptions options = {});

    // Local mode. Routing, connection I/O, and the caller all remain on the
    // supplied runtime. The client must be stopped on that runtime's thread.
    static std::shared_ptr<RpcClient> Create(runtime::Coroutine &runtime, RpcClientOptions options = {});

    static RpcClient& GetInstance(RpcClientOptions options = {});

    ~RpcClient();

    void Stop();

    // Task submission used by CoScope.
    runtime::Task<128> *AllocateTask();
    void SubmitTask(runtime::Task<128> *task);

    runtime::Coroutine* GetCoroutine();

    // Select the connection used by a task running on a client shard.
    static MultiplexedConnection* GetConnectionForCall(std::string_view service,
                                                        std::string_view method,
                                                        std::string_view request_schema = {},
                                                        CallStatus *failure = nullptr);

private:
    struct ClientShard;
    struct Breaker;

    // Choose the less loaded of two shards. Returns nullptr while stopping.
    ClientShard *PickShard();

    // Shared by user threads and client shards.
    memory::TaskPool<128> task_pool_;

    RpcClient(runtime::Coroutine &runtime, RpcClientOptions options);
    explicit RpcClient(RpcClientOptions options);

    bool StartOwned();
    bool StartDiscovery();
    bool ValidateOptions() const noexcept;
    ClientShard *StartShard();

    void RetireAddresses(const std::vector<std::string> &address_keys);

    void PublishService(std::string service, std::unordered_map<std::string, Endpoint> endpoints);
    std::uint64_t Allow(Breaker &breaker, std::uint64_t now_ms) const noexcept;
    void Record(Breaker &breaker, std::uint64_t permit, const CallResult &result,
                std::uint64_t now_ms) noexcept;
    void Open(Breaker &breaker, std::uint64_t expected_control, std::uint64_t now_ms) noexcept;

    runtime::Coroutine *runtime_ = nullptr;
    RpcClientOptions options_;

    mutable std::mutex lifecycle_mutex_;
    std::atomic<bool> stopping_{false};
    std::atomic<std::size_t> next_shard_{0};
    std::vector<std::unique_ptr<ClientShard>> shards_;
    std::shared_ptr<discovery::ZookeeperServiceDiscovery> discovery_;
    bool dedicated_ = true;

    std::shared_ptr<std::unordered_map<std::string,
        std::shared_ptr<const std::unordered_map<std::string, Endpoint>>>> services_;
    std::shared_ptr<const std::unordered_map<std::string, std::shared_ptr<Breaker>>> breakers_;
    std::atomic<std::uint64_t> services_generation_{1};

};

} // namespace rpc::transport
