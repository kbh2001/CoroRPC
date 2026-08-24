#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "transport/service_endpoint.h"

struct _zhandle;
using zhandle_t = struct _zhandle;
struct String_vector;
struct Stat;

namespace rpc::discovery {

struct ZookeeperDiscoveryOptions {
    std::string hosts = "127.0.0.1:2181";
    std::string service_root = "/rpc";
    int session_timeout_ms = 10 * 1000;
};

class ZookeeperServiceDiscovery : public std::enable_shared_from_this<ZookeeperServiceDiscovery> {
public:
    static std::shared_ptr<ZookeeperServiceDiscovery> Create(
        std::function<void(std::string, std::unordered_map<std::string, transport::Endpoint>)> sink,
        ZookeeperDiscoveryOptions options);

    ZookeeperServiceDiscovery(const ZookeeperServiceDiscovery &) = delete;
    ZookeeperServiceDiscovery &operator=(const ZookeeperServiceDiscovery &) = delete;
    ~ZookeeperServiceDiscovery();

    bool Start();
    bool WatchService(const std::string &service);
    void Stop();
    bool started() const noexcept { return started_.load(std::memory_order_acquire); }

private:
    ZookeeperServiceDiscovery(
        std::function<void(std::string, std::unordered_map<std::string, transport::Endpoint>)> sink,
        ZookeeperDiscoveryOptions options);

    static void Watcher(zhandle_t *handle, int type, int state, const char *path, void *context);
    static void ExistsCompletion(int result, const ::Stat *stat, const void *context);
    static void ChildrenCompletion(int result, const ::String_vector *children, const void *context);
    static void EndpointCompletion(int result, const char *value, int value_length, const ::Stat *stat,
                                   const void *context);
    static void RootChildrenCompletion(int result, const ::String_vector *children, const void *context);
    void RefreshService(const std::string &service);
    void ReconnectExpired(zhandle_t *expired_handle);
    void FinishRefresh(const std::string &service, std::uint64_t generation);
    void PublishService(std::string service, std::unordered_map<std::string, transport::Endpoint> endpoints,
                        std::uint64_t generation);
    void DiscoverAllServices();
    static bool ParseEndpoint(const std::string &address_key, const char *value, int value_length,
                              transport::Endpoint &endpoint);
    std::string ServiceFromPath(const char *path) const;

    std::function<void(std::string, std::unordered_map<std::string, transport::Endpoint>)> sink_;
    ZookeeperDiscoveryOptions options_;
    std::mutex mutex_;
    zhandle_t *handle_ = nullptr;
    std::unordered_set<std::string> watched_services_;
    std::unordered_set<std::string> refresh_pending_;
    std::unordered_set<std::string> refresh_again_;
    std::atomic<bool> started_{false};
    std::atomic<std::uint64_t> session_generation_{0};
};

} // namespace rpc::discovery
