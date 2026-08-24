#include "transport/rpc_client.h"

#include "discovery/zookeeper_service_discovery.h"
#include "protocol/rpc_metadata.h"
#include "task/task_complete.h"

#include <arpa/inet.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>

namespace rpc::transport {

thread_local void* tls_current_shard = nullptr;

namespace {

constexpr std::uint64_t kDeniedPermit = std::numeric_limits<std::uint64_t>::max();
constexpr std::uint64_t kStateMask = 0x3ULL;
constexpr std::uint64_t kClosed = 0;
constexpr std::uint64_t kOpen = 1;
constexpr std::uint64_t kHalfOpen = 2;
constexpr std::uint64_t kOpening = 3;
constexpr unsigned kInflightShift = 2;
constexpr unsigned kSuccessShift = 18;
constexpr unsigned kGenerationShift = 34;
constexpr std::uint64_t kCounterMask = 0xffffULL;
constexpr std::uint64_t kGenerationMask = (1ULL << 30) - 1;

std::uint64_t State(std::uint64_t control) noexcept
{
    return control & kStateMask;
}

std::uint64_t Generation(std::uint64_t control) noexcept
{
    return (control >> kGenerationShift) & kGenerationMask;
}

std::uint64_t Inflight(std::uint64_t control) noexcept
{
    return (control >> kInflightShift) & kCounterMask;
}

std::uint64_t Successes(std::uint64_t control) noexcept
{
    return (control >> kSuccessShift) & kCounterMask;
}

std::uint64_t Control(std::uint64_t state, std::uint64_t generation,
                      std::uint64_t inflight = 0, std::uint64_t successes = 0) noexcept
{
    return state | ((inflight & kCounterMask) << kInflightShift) |
           ((successes & kCounterMask) << kSuccessShift) |
           ((generation & kGenerationMask) << kGenerationShift);
}

std::uint64_t NextGeneration(std::uint64_t control) noexcept
{
    return (Generation(control) + 1) & kGenerationMask;
}

std::string BreakerKey(const std::string &service, const std::string &address)
{
    return service + '\n' + address;
}

bool ValidStaticServices(
    const std::unordered_map<std::string, std::unordered_map<std::string, Endpoint>> &services)
{
    if (services.empty())
    {
        return false;
    }
    for (const auto &service : services)
    {
        if (service.first.empty() || service.first.find('/') != std::string::npos || service.second.empty())
        {
            return false;
        }
        for (const auto &endpoint : service.second)
        {
            if (!endpoint.second.valid() || endpoint.first != endpoint.second.address_key())
            {
                return false;
            }
            for (const auto &method : endpoint.second.methods)
            {
                if (method.first.empty() || !method.second.valid())
                {
                    return false;
                }
            }
        }
    }
    return true;
}

std::size_t NextRandom() noexcept
{
    thread_local std::size_t state = std::hash<std::thread::id>{}(std::this_thread::get_id());
    state = state * 1103515245 + 12345;
    return state;
}

} // namespace

struct alignas(64) RpcClient::Breaker {
    std::atomic<std::uint64_t> control{Control(kClosed, 0)};
    std::atomic<std::uint64_t> window{0};
    std::atomic<std::uint64_t> reopen_at_ms{0};
    std::atomic<std::uint64_t> overload_until_ms{0};
    std::atomic<std::size_t> consecutive_opens{0};
};

struct RpcClient::ClientShard {
    struct CachedRoute {
        std::uint64_t generation = 0;
        std::string service;
        std::string method;
        std::string request_schema;
        std::string endpoint_key;
        sockaddr_storage address{};
        socklen_t address_length = 0;
        std::shared_ptr<Breaker> breaker;
        MultiplexedConnection *connection = nullptr;
    };

    explicit ClientShard(RpcClient& client, const RpcClientOptions &client_options)
        : client_(client), options(client_options), hard_capacity(client_options.max_calls_per_shard),
          soft_capacity(std::max<std::size_t>(1, (hard_capacity * client_options.soft_capacity_percent) / 100))
    {
    }

    bool Start()
    {
        thread = std::thread([this] { Run(); });
        std::unique_lock<std::mutex> lock(mutex);
        ready.wait(lock, [this] { return initialized; });
        return ok;
    }

    bool Post(runtime::Coroutine::Routine task)
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!accepting.load(std::memory_order_acquire) || runtime == nullptr)
        {
            return false;
        }
        try
        {
            runtime->Submit(std::move(task));
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    bool TryReserve(bool soft) noexcept
    {
        if (!accepting.load(std::memory_order_acquire))
        {
            return false;
        }
        const std::size_t limit = soft ? soft_capacity : hard_capacity;
        std::size_t current = reserved_calls.load(std::memory_order_relaxed);
        while (current < limit)
        {
            if (reserved_calls.compare_exchange_weak(current, current + 1,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_relaxed))
            {
                if (accepting.load(std::memory_order_acquire))
                {
                    return true;
                }
                reserved_calls.fetch_sub(1, std::memory_order_release);
                return false;
            }
        }
        return false;
    }

    void Release() noexcept
    {
        std::size_t prev = reserved_calls.fetch_sub(1, std::memory_order_acq_rel);
        if (prev == 0)
        {
            std::terminate();
        }
    }

    // Called on the shard owner thread.
    MultiplexedConnection* GetOrCreateConnection(std::string_view address_key,
                                                   const sockaddr* addr,
                                                   socklen_t addr_len)
    {
        auto it = std::find_if(connections.begin(), connections.end(),
                               [address_key](const auto &entry) { return entry.first == address_key; });
        if (it != connections.end())
        {
            if (it->second->state() == ConnectionState::READY)
            {
                return it->second.get();
            }
            connections.erase(it);
        }

        auto conn = MultiplexedConnection::Create(*runtime);
        if (!conn->Connect(addr, addr_len, 5000))
        {
            return nullptr;
        }
        MultiplexedConnection* ptr = conn.get();
        connections.emplace(std::string(address_key), std::move(conn));
        return ptr;
    }

    void Run()
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            try
            {
                runtime::Coroutine::Options coro_opts;
                coro_opts.stack_size = 256 * 1024;
                // Cover reader, writer and the expected per-shard share of
                // synchronous callers without copying one shared stack on
                // every context switch.
                coro_opts.shared_stack_count = 16;
                coro_opts.shared_stack_size = 256 * 1024;

                coro_opts.pool_max_size = options.max_calls_per_shard;
                coro_opts.pool_prewarm_count = options.max_calls_per_shard / 2;

                runtime = std::make_unique<runtime::Coroutine>(coro_opts);
                ok = runtime->InOwnerThread();
            }
            catch (...)
            {
                ok = false;
            }
            initialized = true;
        }
        ready.notify_one();
        if (!ok)
        {
            return;
        }
        tls_current_shard = this;
        runtime->RunUntil(stopping);
        for (auto &entry : connections)
        {
            entry.second->Close();
        }
        connections.clear();
        runtime->Join();
        tls_current_shard = nullptr;
    }

    void Stop()
    {
        accepting.store(false, std::memory_order_release);
        stopping.store(true, std::memory_order_release);
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (runtime != nullptr)
            {
                runtime->Wake();
            }
            else
            {
                std::cout << "[ClientShard::Stop] runtime is nullptr!" << std::endl;
            }
        }
        std::cout << "[ClientShard::Stop] Waiting for thread to join..." << std::endl;
        if (thread.joinable())
        {
            thread.join();
        }
        std::cout << "[ClientShard::Stop] Thread joined successfully" << std::endl;
    }

    RpcClient& client_;
    const RpcClientOptions &options;
    const std::size_t hard_capacity;
    const std::size_t soft_capacity;

    std::thread thread;
    std::mutex mutex;
    std::condition_variable ready;
    bool initialized = false;
    bool ok = false;
    std::unique_ptr<runtime::Coroutine> runtime;
    std::atomic<bool> accepting{true};
    std::atomic<bool> stopping{false};
    std::atomic<std::size_t> reserved_calls{0};

    // Each shard owns its connections.
    std::unordered_map<std::string, std::shared_ptr<MultiplexedConnection>> connections;
    CachedRoute cached_route;
};

RpcClient::RpcClient(RpcClientOptions options)
    : options_(std::move(options)), dedicated_(true)
{
    services_ = std::make_shared<std::unordered_map<std::string,
        std::shared_ptr<const std::unordered_map<std::string, Endpoint>>>>();
    breakers_ = std::make_shared<std::unordered_map<std::string, std::shared_ptr<Breaker>>>();
}

RpcClient::RpcClient(runtime::Coroutine &runtime, RpcClientOptions options)
    : runtime_(&runtime), options_(std::move(options)), dedicated_(false)
{
    services_ = std::make_shared<std::unordered_map<std::string,
        std::shared_ptr<const std::unordered_map<std::string, Endpoint>>>>();
    breakers_ = std::make_shared<std::unordered_map<std::string, std::shared_ptr<Breaker>>>();
}

std::shared_ptr<RpcClient> RpcClient::Create(RpcClientOptions options)
{
    auto client = std::shared_ptr<RpcClient>(new RpcClient(std::move(options)));
    if (!client->ValidateOptions() || !client->StartOwned() || !client->StartDiscovery())
    {
        client->Stop();
        return nullptr;
    }
    return client;
}

std::shared_ptr<RpcClient> RpcClient::CreateStatic(
    std::unordered_map<std::string, std::unordered_map<std::string, Endpoint>> services,
    RpcClientOptions options)
{
    if (!ValidStaticServices(services))
    {
        return nullptr;
    }
    auto client = std::shared_ptr<RpcClient>(new RpcClient(std::move(options)));
    if (!client->ValidateOptions() || !client->StartOwned())
    {
        client->Stop();
        return nullptr;
    }
    for (auto &service : services)
    {
        client->PublishService(std::move(service.first), std::move(service.second));
    }
    return client;
}

std::shared_ptr<RpcClient> RpcClient::CreateStatic(runtime::Coroutine &runtime,
    std::unordered_map<std::string, std::unordered_map<std::string, Endpoint>> services,
    RpcClientOptions options)
{
    if (!ValidStaticServices(services))
    {
        return nullptr;
    }
    auto client = std::shared_ptr<RpcClient>(new RpcClient(runtime, std::move(options)));
    if (!client->ValidateOptions())
    {
        return nullptr;
    }
    for (auto &service : services)
    {
        client->PublishService(std::move(service.first), std::move(service.second));
    }
    return client;
}

std::shared_ptr<RpcClient> RpcClient::Create(runtime::Coroutine &runtime, RpcClientOptions options)
{
    auto client = std::shared_ptr<RpcClient>(new RpcClient(runtime, std::move(options)));
    if (!client->ValidateOptions() || !client->StartDiscovery())
    {
        return nullptr;
    }
    return client;
}

RpcClient::~RpcClient()
{
    Stop();
}

bool RpcClient::ValidateOptions() const noexcept
{
    const auto &retry = options_.retry;
    return options_.io_threads > 0 && options_.max_calls_per_shard > 0 &&
           options_.soft_capacity_percent <= 100 && options_.connect_timeout_ms > 0 &&
           retry.max_attempts != 0 && retry.retry_budget_capacity != 0 && retry.base_backoff_ms != 0 &&
           retry.max_backoff_ms >= retry.base_backoff_ms &&
           options_.circuit_breaker.window_size > 0 &&
           options_.circuit_breaker.minimum_requests <= options_.circuit_breaker.window_size &&
           options_.circuit_breaker.failure_threshold_percent <= 100 &&
           options_.circuit_breaker.open_interval_ms > 0 &&
           options_.circuit_breaker.max_open_interval_ms >= options_.circuit_breaker.open_interval_ms &&
           options_.circuit_breaker.half_open_max_probes > 0;
}

bool RpcClient::StartOwned()
{
    if (!dedicated_)
    {
        return true;
    }

    // Start the configured number of shards.
    for (std::size_t i = 0; i < options_.io_threads; ++i)
    {
        ClientShard *shard = StartShard();
        if (shard == nullptr)
        {
            return false;
        }
    }
    return true;
}

bool RpcClient::StartDiscovery()
{
    if (options_.zookeeper_hosts.empty())
    {
        return true;
    }
    try
    {
        const auto weak = std::weak_ptr<RpcClient>(shared_from_this());
        discovery::ZookeeperDiscoveryOptions disco_opts;
        disco_opts.hosts = options_.zookeeper_hosts;
        disco_opts.service_root = options_.service_root;
        disco_opts.session_timeout_ms = options_.zookeeper_session_timeout_ms;

        discovery_ = discovery::ZookeeperServiceDiscovery::Create(
            [weak](std::string service, std::unordered_map<std::string, Endpoint> endpoints) {
                if (const auto self = weak.lock())
                {
                    self->PublishService(std::move(service), std::move(endpoints));
                }
            },
            disco_opts);
        if (discovery_)
        {
            bool started = discovery_->Start();
            return started;
        }
        return discovery_ != nullptr;
    }
    catch (const std::exception& e)
    {
        return false;
    }
    catch (...)
    {
        return false;
    }
}

RpcClient::ClientShard *RpcClient::StartShard()
{
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (stopping_.load(std::memory_order_acquire) || shards_.size() >= options_.io_threads)
    {
        return nullptr;
    }
    auto shard = std::make_unique<ClientShard>(*this, options_);
    if (!shard->Start())
    {
        return nullptr;
    }
    ClientShard *ptr = shard.get();
    shards_.push_back(std::move(shard));
    return ptr;
}

runtime::Coroutine* RpcClient::GetCoroutine()
{
    if (runtime_ != nullptr)
    {
        return runtime_;
    }
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (!shards_.empty() && shards_[0]->runtime != nullptr)
    {
        return shards_[0]->runtime.get();
    }
    return nullptr;
}

void RpcClient::Stop()
{
    if (stopping_.exchange(true, std::memory_order_acq_rel))
    {
        return;
    }

    if (discovery_)
    {
        discovery_->Stop();
        discovery_.reset();
    }

    // shards_ is immutable after startup so PickShard() can stay lock-free.
    // stopping_ closes the submit side before the stable shard objects stop.
    for (auto &shard : shards_)
    {
        shard->Stop();
    }
}

runtime::Task<128> *RpcClient::AllocateTask()
{
    return task_pool_.Allocate();
}

void RpcClient::SubmitTask(runtime::Task<128> *task)
{
    ClientShard *shard = PickShard();
    if (shard == nullptr)
    {
        task->exception = std::make_exception_ptr(
            std::runtime_error("RpcClient is stopping; task rejected"));
        runtime::CompleteTask(task, task_pool_);
        return;
    }

    if (!shard->TryReserve(false))
    {
        task->exception = std::make_exception_ptr(
            std::runtime_error("RpcClient shard overloaded; task rejected"));
        runtime::CompleteTask(task, task_pool_);
        return;
    }

    // Release the shard slot after the task completes.
    bool posted = shard->Post([task, shard, pool = &task_pool_] {
        try
        {
            task->Run();
        }
        catch (...)
        {
            if (!task->exception)
            {
                task->exception = std::current_exception();
            }
        }
        runtime::CompleteTask(task, *pool);
        shard->Release();
    });

    if (!posted)
    {
        shard->Release();
        task->exception = std::make_exception_ptr(
            std::runtime_error("RpcClient: failed to post task"));
        runtime::CompleteTask(task, task_pool_);
    }
}

RpcClient::ClientShard *RpcClient::PickShard()
{
    if (stopping_.load(std::memory_order_acquire) || shards_.empty())
    {
        return nullptr;
    }

    const std::size_t count = shards_.size();
    const std::size_t i = next_shard_.fetch_add(1, std::memory_order_relaxed) % count;
    if (count == 1)
    {
        return shards_[i].get();
    }

    const std::size_t j = (i + 1 + (NextRandom() % (count - 1))) % count;

    ClientShard *shard_i = shards_[i].get();
    ClientShard *shard_j = shards_[j].get();

    return shard_i->reserved_calls.load(std::memory_order_relaxed) <=
                   shard_j->reserved_calls.load(std::memory_order_relaxed)
               ? shard_i
               : shard_j;
}

void RpcClient::PublishService(std::string service, std::unordered_map<std::string, Endpoint> endpoints)
{
    if (stopping_.load(std::memory_order_acquire) || service.empty() || service.find('/') != std::string::npos)
    {
        return;
    }
    for (const auto &endpoint : endpoints)
    {
        if (!endpoint.second.valid() || endpoint.first != endpoint.second.address_key())
        {
            return;
        }
        for (const auto &method : endpoint.second.methods)
        {
            if (method.first.empty() || !method.second.valid())
            {
                return;
            }
        }
    }

    const auto old_breakers = std::atomic_load_explicit(&breakers_, std::memory_order_acquire);
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Breaker>>> breakers;
    for (const auto &endpoint : endpoints)
    {
        const std::string breaker_key = BreakerKey(service, endpoint.first);
        if (old_breakers->find(breaker_key) == old_breakers->end())
        {
            if (!breakers)
            {
                breakers = std::make_shared<std::unordered_map<std::string,
                    std::shared_ptr<Breaker>>>(*old_breakers);
            }
            breakers->emplace(breaker_key, std::make_shared<Breaker>());
        }
    }
    if (breakers)
    {
        std::atomic_store_explicit(
            &breakers_,
            std::shared_ptr<const std::unordered_map<std::string, std::shared_ptr<Breaker>>>(std::move(breakers)),
            std::memory_order_release);
    }

    std::vector<std::string> retired_addresses;
    auto next_endpoints = std::make_shared<const std::unordered_map<std::string, Endpoint>>(std::move(endpoints));
    auto services = std::atomic_load_explicit(&services_, std::memory_order_acquire);
    const auto service_entry = services->find(service);
    std::shared_ptr<const std::unordered_map<std::string, Endpoint>> previous;
    if (service_entry != services->end())
    {
        previous = std::atomic_load_explicit(&service_entry->second, std::memory_order_acquire);
        std::atomic_store_explicit(&service_entry->second, next_endpoints, std::memory_order_release);
    }
    else
    {
        auto next_services = std::make_shared<std::unordered_map<std::string,
            std::shared_ptr<const std::unordered_map<std::string, Endpoint>>>>(*services);
        next_services->emplace(std::move(service), next_endpoints);
        std::atomic_store_explicit(&services_, std::move(next_services), std::memory_order_release);
    }
    services_generation_.fetch_add(1, std::memory_order_release);

    if (previous)
    {
        for (const auto &endpoint : *previous)
        {
            if (next_endpoints->find(endpoint.first) == next_endpoints->end())
            {
                retired_addresses.push_back(endpoint.first);
            }
        }
    }

    const auto current_services = std::atomic_load_explicit(&services_, std::memory_order_acquire);
    std::vector<std::string> globally_retired;
    for (const std::string &address_key : retired_addresses)
    {
        bool active = false;
        for (const auto &current_service : *current_services)
        {
            const auto current_endpoints =
                std::atomic_load_explicit(&current_service.second, std::memory_order_acquire);
            if (current_endpoints->find(address_key) != current_endpoints->end())
            {
                active = true;
                break;
            }
        }
        if (!active)
        {
            globally_retired.push_back(address_key);
        }
    }
    RetireAddresses(globally_retired);
}

void RpcClient::RetireAddresses(const std::vector<std::string> &address_keys)
{
    if (address_keys.empty())
    {
        return;
    }

    // Address retirement is supported only in dedicated mode.
    if (!dedicated_)
    {
        return;
    }

    std::vector<ClientShard *> shards;
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        shards.reserve(shards_.size());
        for (const auto &shard : shards_)
        {
            shards.push_back(shard.get());
        }
    }
    for (ClientShard *shard : shards)
    {
        (void)shard->Post([shard, address_keys] {
            for (const std::string &address_key : address_keys)
            {
                const auto found = shard->connections.find(address_key);
                if (found != shard->connections.end())
                {
                    found->second->BeginDrain();
                    shard->connections.erase(found);
                }
            }
        });
    }
}

std::uint64_t RpcClient::Allow(Breaker &breaker, std::uint64_t now_ms) const noexcept
{
    if (now_ms < breaker.overload_until_ms.load(std::memory_order_relaxed))
    {
        return kDeniedPermit;
    }
    std::uint64_t control = breaker.control.load(std::memory_order_acquire);
    for (;;)
    {
        if (State(control) == kClosed)
        {
            return control;
        }
        if (State(control) == kOpen)
        {
            if (now_ms < breaker.reopen_at_ms.load(std::memory_order_relaxed))
            {
                return kDeniedPermit;
            }
            const std::uint64_t half = Control(kHalfOpen, NextGeneration(control), 1, 0);
            if (breaker.control.compare_exchange_weak(control, half,
                                                      std::memory_order_acq_rel,
                                                      std::memory_order_acquire))
            {
                return half;
            }
            continue;
        }
        if (State(control) == kOpening)
        {
            control = breaker.control.load(std::memory_order_acquire);
            continue;
        }
        if (Inflight(control) >= options_.circuit_breaker.half_open_max_probes)
        {
            return kDeniedPermit;
        }
        const std::uint64_t next = Control(kHalfOpen, Generation(control),
                                           Inflight(control) + 1, Successes(control));
        if (breaker.control.compare_exchange_weak(control, next,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire))
        {
            return next;
        }
    }
}

void RpcClient::Record(Breaker &breaker, std::uint64_t permit, const CallResult &result,
                       std::uint64_t now_ms) noexcept
{
    const bool local_failure = result.status == CallStatus::ENCODE_ERROR ||
                               result.status == CallStatus::PROTOCOL_ERROR;
    const bool overload = result.status == CallStatus::LOCAL_OVERLOADED ||
                         result.rpc_status == RpcStatus::RESOURCE_EXHAUSTED;
    const bool transport_failure = result.status == CallStatus::NOT_CONNECTED ||
                                  result.status == CallStatus::CONNECTION_CLOSED ||
                                  result.status == CallStatus::TIMEOUT ||
                                  result.rpc_status == RpcStatus::UNAVAILABLE ||
                                  result.rpc_status == RpcStatus::DEADLINE_EXCEEDED;

    if (overload)
    {
        const std::uint64_t base_backoff_ms = options_.retry.base_backoff_ms;
        breaker.overload_until_ms.store(now_ms + base_backoff_ms, std::memory_order_relaxed);
    }

    if (State(permit) == kHalfOpen)
    {
        if (transport_failure)
        {
            Open(breaker, permit, now_ms);
            return;
        }

        std::uint64_t control = breaker.control.load(std::memory_order_acquire);
        while (State(control) == kHalfOpen && Generation(control) == Generation(permit))
        {
            const std::uint64_t inflight = Inflight(control);
            const std::uint64_t successes = Successes(control) +
                                           (result.status == CallStatus::OK && result.rpc_status == RpcStatus::OK ? 1 : 0);
            const std::uint64_t next =
                (inflight == 1 && successes >= options_.circuit_breaker.half_open_max_probes)
                ? Control(kClosed, NextGeneration(control))
                : Control(kHalfOpen, Generation(control), inflight == 0 ? 0 : inflight - 1, successes);
            if (breaker.control.compare_exchange_weak(control, next,
                                                      std::memory_order_acq_rel,
                                                      std::memory_order_acquire))
            {
                if (State(next) == kClosed)
                {
                    breaker.window.store(0, std::memory_order_relaxed);
                    breaker.consecutive_opens.store(0, std::memory_order_relaxed);
                }
                return;
            }
        }
        return;
    }

    if (local_failure || overload || State(permit) != kClosed)
    {
        return;
    }

    std::uint64_t control = breaker.control.load(std::memory_order_acquire);
    if (State(control) != kClosed || Generation(control) != Generation(permit))
    {
        return;
    }

    for (;;)
    {
        std::uint64_t window = breaker.window.load(std::memory_order_relaxed);
        std::uint32_t count = static_cast<std::uint32_t>(window & 0xFFFFFFFFULL);
        std::uint32_t failures = static_cast<std::uint32_t>(window >> 32);
        if (count >= options_.circuit_breaker.window_size)
        {
            count = 0;
            failures = 0;
        }
        ++count;
        if (transport_failure)
        {
            ++failures;
        }
        const std::uint64_t next = (static_cast<std::uint64_t>(failures) << 32) | count;
        if (breaker.window.compare_exchange_weak(window, next,
                                                 std::memory_order_relaxed,
                                                 std::memory_order_relaxed))
        {
            if (count >= options_.circuit_breaker.minimum_requests &&
                failures * 100 >= count * options_.circuit_breaker.failure_threshold_percent)
            {
                Open(breaker, control, now_ms);
            }
            return;
        }
    }
}

void RpcClient::Open(Breaker &breaker, std::uint64_t expected_control, std::uint64_t now_ms) noexcept
{
    std::uint64_t control = expected_control;
    while (State(control) == State(expected_control) &&
           Generation(control) == Generation(expected_control))
    {
        const std::uint64_t generation = NextGeneration(control);
        const std::uint64_t opening = Control(kOpening, generation);
        if (breaker.control.compare_exchange_weak(control, opening,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire))
        {
            const std::size_t opens = breaker.consecutive_opens.fetch_add(1, std::memory_order_relaxed);
            const std::size_t shift = std::min<std::size_t>(opens, 16);
            const std::uint64_t interval = std::min(options_.circuit_breaker.open_interval_ms << shift,
                                                   options_.circuit_breaker.max_open_interval_ms);
            breaker.reopen_at_ms.store(now_ms + interval, std::memory_order_relaxed);
            breaker.window.store(0, std::memory_order_relaxed);
            const std::uint64_t open_control = Control(kOpen, generation);
            breaker.control.store(open_control, std::memory_order_release);
            return;
        }
    }
}

RpcClient& RpcClient::GetInstance(RpcClientOptions options)
{
    static auto instance = Create(std::move(options));
    if (!instance) {
        throw std::runtime_error("Failed to create RpcClient instance");
    }
    return *instance;
}

MultiplexedConnection* RpcClient::GetConnectionForCall(std::string_view service,
                                                        std::string_view method,
                                                        std::string_view request_schema,
                                                        CallStatus *failure)
{
    if (failure != nullptr)
    {
        *failure = CallStatus::SERVICE_NOT_FOUND;
    }
    extern thread_local void* tls_current_shard;
    if (tls_current_shard == nullptr)
    {
        if (failure != nullptr)
        {
            *failure = CallStatus::NOT_CONNECTED;
        }
        std::cout << "[GetConnectionForCall] ERROR: tls_current_shard is nullptr\n";
        return nullptr;
    }
    auto* shard = static_cast<ClientShard*>(tls_current_shard);
    RpcClient& client = shard->client_;

    const std::uint64_t generation = client.services_generation_.load(std::memory_order_acquire);
    auto &cached = shard->cached_route;
    if (cached.generation == generation && cached.service == service && cached.method == method &&
        cached.request_schema == request_schema)
    {
        const std::uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        if (!cached.breaker || client.Allow(*cached.breaker, now_ms) != kDeniedPermit)
        {
            if (cached.connection != nullptr && cached.connection->state() == ConnectionState::READY)
            {
                if (failure != nullptr)
                {
                    *failure = CallStatus::OK;
                }
                return cached.connection;
            }
            cached.connection = shard->GetOrCreateConnection(
                cached.endpoint_key, reinterpret_cast<const sockaddr *>(&cached.address), cached.address_length);
            if (cached.connection != nullptr && failure != nullptr)
            {
                *failure = CallStatus::OK;
            }
            return cached.connection;
        }
        cached.generation = 0;
    }

    auto services_snapshot = std::atomic_load_explicit(&client.services_, std::memory_order_acquire);
    if (!services_snapshot)
    {
        std::cout << "[GetConnectionForCall] ERROR: services_snapshot is nullptr\n";
        return nullptr;
    }

    auto service_it = std::find_if(services_snapshot->begin(), services_snapshot->end(),
                                   [service](const auto &entry) { return entry.first == service; });
    if (service_it == services_snapshot->end() || !service_it->second)
    {
        if (client.discovery_)
        {
            client.discovery_->WatchService(std::string(service));
        }
        return nullptr;
    }

    auto endpoints_map = service_it->second;
    if (!endpoints_map || endpoints_map->empty())
    {
        return nullptr;
    }

    const std::uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    auto breakers_snapshot = std::atomic_load_explicit(&client.breakers_, std::memory_order_acquire);

    bool method_found = false;
    bool schema_matched = request_schema.empty();
    const std::string method_key(method);
    for (const auto& [endpoint_key, endpoint] : *endpoints_map)
    {
        const auto method_it = endpoint.methods.find(method_key);
        if (method_it == endpoint.methods.end())
        {
            continue;
        }
        method_found = true;
        if (!request_schema.empty() && method_it->second.request_schema != request_schema)
        {
            continue;
        }
        schema_matched = true;
        thread_local std::string breaker_key;
        breaker_key.clear();
        breaker_key.reserve(service.size() + 1 + endpoint_key.size());
        breaker_key.append(service.data(), service.size());
        breaker_key.push_back('\n');
        breaker_key.append(endpoint_key);
        auto breaker_it = breakers_snapshot->find(breaker_key);

        if (breaker_it != breakers_snapshot->end())
        {
            std::uint64_t permit = client.Allow(*breaker_it->second, now_ms);
            if (permit == kDeniedPermit)
            {
                continue;
            }
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(endpoint.port);

        if (inet_pton(AF_INET, endpoint.host.c_str(), &addr.sin_addr) != 1)
        {
            continue;
        }

        auto* conn = shard->GetOrCreateConnection(endpoint_key,
                                                   reinterpret_cast<const sockaddr*>(&addr),
                                                   sizeof(addr));

        if (conn)
        {
            cached.generation = generation;
            cached.service.assign(service.data(), service.size());
            cached.method.assign(method.data(), method.size());
            cached.request_schema.assign(request_schema.data(), request_schema.size());
            cached.endpoint_key = endpoint_key;
            cached.address = {};
            std::memcpy(&cached.address, &addr, sizeof(addr));
            cached.address_length = sizeof(addr);
            cached.breaker = breaker_it == breakers_snapshot->end() ? nullptr : breaker_it->second;
            cached.connection = conn;
            if (failure != nullptr)
            {
                *failure = CallStatus::OK;
            }
            return conn;
        }
    }

    if (failure != nullptr && method_found && !schema_matched)
    {
        *failure = CallStatus::INVALID_REQUEST;
    }
    // No endpoint is currently available.
    return nullptr;
}


} // namespace rpc::transport
