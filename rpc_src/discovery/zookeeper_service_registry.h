#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "transport/service_endpoint.h"

struct _zhandle;
using zhandle_t = struct _zhandle;
struct Stat;

namespace rpc::discovery {

struct ZookeeperRegistryOptions {
    std::string hosts = "127.0.0.1:2181";
    std::string service_root = "/rpc";
    std::string host;
    std::uint16_t port = 0;
    int session_timeout_ms = 10 * 1000;
};

class ZookeeperServiceRegistry {
public:
    explicit ZookeeperServiceRegistry(ZookeeperRegistryOptions options);
    ~ZookeeperServiceRegistry();

    ZookeeperServiceRegistry(const ZookeeperServiceRegistry &) = delete;
    ZookeeperServiceRegistry &operator=(const ZookeeperServiceRegistry &) = delete;

    bool Start();
    bool Publish(std::unordered_map<std::string,
                 std::unordered_map<std::string, transport::MethodCapability>> services);
    void Unpublish();
    void Stop();
    bool published() const noexcept { return published_.load(std::memory_order_acquire); }

private:
    static void Watcher(zhandle_t *handle, int type, int state, const char *path, void *context);
    static void RootCompletion(int result, const char *value, const void *context);
    static void ServiceCompletion(int result, const char *value, const void *context);
    static void ProvidersCompletion(int result, const char *value, const void *context);
    static void EndpointCompletion(int result, const char *value, const void *context);
    static void ExistingEndpointCompletion(int result, const ::Stat *stat, const void *context);
    static void SetCompletion(int result, const ::Stat *stat, const void *context);
    void ReconnectExpired(zhandle_t *expired_handle);
    void BeginPublish();
    void PublishCurrentService(std::uint64_t generation);
    void PublishProviders(std::uint64_t generation);
    void PublishEndpoint(std::uint64_t generation);
    void FinishCurrentService(std::uint64_t generation, bool success);
    void FinishPublish(std::uint64_t generation);
    std::string Encode(const std::string &service) const;
    std::string AddressKey() const;

    ZookeeperRegistryOptions options_;
    mutable std::mutex mutex_;
    zhandle_t *handle_ = nullptr;
    std::unordered_map<std::string,
        std::unordered_map<std::string, transport::MethodCapability>> services_;
    std::vector<std::string> publishing_services_;
    std::unordered_set<std::string> published_paths_;
    std::size_t publishing_index_ = 0;
    bool publishing_ = false;
    bool publish_again_ = false;
    bool publish_failed_ = false;
    std::atomic<bool> started_{false};
    std::atomic<bool> published_{false};
    std::atomic<std::uint64_t> session_generation_{0};
};

} // namespace rpc::discovery
