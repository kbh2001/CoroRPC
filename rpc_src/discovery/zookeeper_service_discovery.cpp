#include "discovery/zookeeper_service_discovery.h"

#include <iostream>

extern "C" {
#include "zookeeper.h"
}

#include <charconv>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "third_part/nlohmann/json.hpp"

namespace rpc::discovery {
namespace {

struct DiscoveryRefresh {
    std::weak_ptr<ZookeeperServiceDiscovery> discovery;
    std::string service;
    std::uint64_t session_generation = 0;
    std::size_t remaining = 0;
    bool failed = false;
    std::unordered_map<std::string, transport::Endpoint> endpoints;
};

bool ValidPathSegment(const std::string &value)
{
    return !value.empty() && value != "." && value != ".." && value.find('/') == std::string::npos;
}

} // namespace

std::shared_ptr<ZookeeperServiceDiscovery> ZookeeperServiceDiscovery::Create(
    std::function<void(std::string, std::unordered_map<std::string, transport::Endpoint>)> sink,
    ZookeeperDiscoveryOptions options)
{
    if (!sink || options.hosts.empty() || options.service_root.empty() || options.service_root.front() != '/' ||
        options.session_timeout_ms <= 0)
    {
        throw std::invalid_argument("invalid ZooKeeper discovery options");
    }
    return std::shared_ptr<ZookeeperServiceDiscovery>(
        new ZookeeperServiceDiscovery(std::move(sink), std::move(options)));
}

ZookeeperServiceDiscovery::ZookeeperServiceDiscovery(
    std::function<void(std::string, std::unordered_map<std::string, transport::Endpoint>)> sink,
    ZookeeperDiscoveryOptions options)
    : sink_(std::move(sink)), options_(std::move(options))
{
    while (options_.service_root.size() > 1 && options_.service_root.back() == '/')
    {
        options_.service_root.pop_back();
    }
}

ZookeeperServiceDiscovery::~ZookeeperServiceDiscovery()
{
    Stop();
}

bool ZookeeperServiceDiscovery::Start()
{
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
    {
        return false;
    }
    zhandle_t *handle = zookeeper_init(options_.hosts.c_str(), &ZookeeperServiceDiscovery::Watcher,
                                       options_.session_timeout_ms, nullptr, this, 0);
    if (handle == nullptr)
    {
        started_.store(false, std::memory_order_release);
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    handle_ = handle;
    session_generation_.store(1, std::memory_order_release);
    return true;
}

void ZookeeperServiceDiscovery::DiscoverAllServices()
{
    zhandle_t *handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_.load(std::memory_order_acquire) || handle_ == nullptr)
        {
            return;
        }
        handle = handle_;
    }

    auto *context = new std::weak_ptr<ZookeeperServiceDiscovery>(weak_from_this());
    const int result = zoo_awget_children(handle, options_.service_root.c_str(),
                                          &ZookeeperServiceDiscovery::Watcher, this,
                                          &ZookeeperServiceDiscovery::RootChildrenCompletion, context);
    if (result != ZOK)
    {
        delete context;
    }
}

bool ZookeeperServiceDiscovery::WatchService(const std::string &service)
{
    if (!ValidPathSegment(service) || !started_.load(std::memory_order_acquire))
    {
        return false;
    }
    bool inserted = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_.load(std::memory_order_acquire))
        {
            return false;
        }
        inserted = watched_services_.insert(service).second;
    }
    if (inserted)
    {
        RefreshService(service);
    }
    return true;
}

void ZookeeperServiceDiscovery::Stop()
{
    if (!started_.exchange(false, std::memory_order_acq_rel))
    {
        return;
    }
    zhandle_t *handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        handle = handle_;
        handle_ = nullptr;
        session_generation_.fetch_add(1, std::memory_order_acq_rel);
        refresh_pending_.clear();
        refresh_again_.clear();
    }
    if (handle != nullptr)
    {
        zookeeper_close(handle);
    }
}

void ZookeeperServiceDiscovery::Watcher(zhandle_t *handle, int type, int state, const char *path, void *context)
{
    auto *discovery = static_cast<ZookeeperServiceDiscovery *>(context);
    if (discovery == nullptr || !discovery->started_.load(std::memory_order_acquire))
    {
        return;
    }
    if (type == ZOO_SESSION_EVENT && state == ZOO_EXPIRED_SESSION_STATE)
    {
        discovery->ReconnectExpired(handle);
        return;
    }

    std::vector<std::string> services;
    {
        std::lock_guard<std::mutex> lock(discovery->mutex_);
        if (discovery->handle_ != handle)
        {
            return;
        }
        if (type == ZOO_SESSION_EVENT && state == ZOO_CONNECTED_STATE)
        {
            services.assign(discovery->watched_services_.begin(), discovery->watched_services_.end());
        }
    }
    if (!services.empty())
    {
        // Rediscover services and refresh existing watches after reconnecting.
        discovery->DiscoverAllServices();
        for (const std::string &service : services)
        {
            discovery->RefreshService(service);
        }
        return;
    }
    else if (type == ZOO_SESSION_EVENT && state == ZOO_CONNECTED_STATE)
    {
        // Start discovery after the initial connection.
        discovery->DiscoverAllServices();
        return;
    }

    // Handle changes to the /rpc root children.
    if (path != nullptr && std::string(path) == discovery->options_.service_root &&
        type == ZOO_CHILD_EVENT)
    {
        discovery->DiscoverAllServices();
        return;
    }

    if (type == ZOO_CHILD_EVENT || type == ZOO_CHANGED_EVENT || type == ZOO_CREATED_EVENT ||
        type == ZOO_DELETED_EVENT)
    {
        const std::string service = discovery->ServiceFromPath(path);
        if (!service.empty())
        {
            discovery->RefreshService(service);
        }
    }
}

void ZookeeperServiceDiscovery::ReconnectExpired(zhandle_t *expired_handle)
{
    std::uint64_t generation = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_.load(std::memory_order_acquire) || handle_ != expired_handle)
        {
            return;
        }
        handle_ = nullptr;
        generation = session_generation_.fetch_add(1, std::memory_order_acq_rel) + 1;
        refresh_pending_.clear();
        refresh_again_.clear();
    }
    zookeeper_close(expired_handle);
    if (!started_.load(std::memory_order_acquire))
    {
        return;
    }
    zhandle_t *replacement = zookeeper_init(options_.hosts.c_str(), &ZookeeperServiceDiscovery::Watcher,
                                             options_.session_timeout_ms, nullptr, this, 0);
    if (replacement == nullptr)
    {
        return;
    }

    std::vector<std::string> services;
    bool retained = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (started_.load(std::memory_order_acquire) && handle_ == nullptr &&
            session_generation_.load(std::memory_order_acquire) == generation)
        {
            handle_ = replacement;
            retained = true;
            services.assign(watched_services_.begin(), watched_services_.end());
        }
    }
    if (!retained)
    {
        zookeeper_close(replacement);
        return;
    }
    // Rediscover all services after reconnecting.
    DiscoverAllServices();
    for (const std::string &service : services)
    {
        RefreshService(service);
    }
}

void ZookeeperServiceDiscovery::RootChildrenCompletion(int result, const ::String_vector *children, const void *context)
{
    std::unique_ptr<std::weak_ptr<ZookeeperServiceDiscovery>> holder(
        const_cast<std::weak_ptr<ZookeeperServiceDiscovery> *>(
            static_cast<const std::weak_ptr<ZookeeperServiceDiscovery> *>(context)));

    const auto discovery = holder->lock();
    if (!discovery || !discovery->started_.load(std::memory_order_acquire))
    {
        return;
    }

    if (result != ZOK || children == nullptr)
    {
        return;
    }

    // Watch every discovered service.
    for (int i = 0; i < children->count; ++i)
    {
        if (children->data[i] != nullptr)
        {
            const std::string service_name = children->data[i];
            if (!service_name.empty() && service_name != "." && service_name != ".." &&
                service_name.find('/') == std::string::npos)
            {
                discovery->WatchService(service_name);
            }
        }
    }
}

void ZookeeperServiceDiscovery::RefreshService(const std::string &service)
{
    zhandle_t *handle = nullptr;
    std::uint64_t generation = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_.load(std::memory_order_acquire) || watched_services_.find(service) == watched_services_.end())
        {
            return;
        }
        if (!refresh_pending_.insert(service).second)
        {
            refresh_again_.insert(service);
            return;
        }
        handle = handle_;
        generation = session_generation_.load(std::memory_order_acquire);
    }
    if (handle == nullptr)
    {
        FinishRefresh(service, generation);
        return;
    }

    auto refresh = std::make_shared<DiscoveryRefresh>();
    refresh->discovery = weak_from_this();
    refresh->service = service;
    refresh->session_generation = generation;
    auto *context = new std::shared_ptr<DiscoveryRefresh>(refresh);
    const std::string path = options_.service_root + '/' + service + "/providers";
    const int result = zoo_awexists(handle, path.c_str(), &ZookeeperServiceDiscovery::Watcher, this,
                                    &ZookeeperServiceDiscovery::ExistsCompletion, context);
    if (result != ZOK)
    {
        delete context;
        FinishRefresh(service, generation);
    }
}

void ZookeeperServiceDiscovery::ExistsCompletion(int result, const ::Stat *, const void *context)
{
    std::unique_ptr<std::shared_ptr<DiscoveryRefresh>> holder(
        const_cast<std::shared_ptr<DiscoveryRefresh> *>(
            static_cast<const std::shared_ptr<DiscoveryRefresh> *>(context)));
    const std::shared_ptr<DiscoveryRefresh> refresh = *holder;
    const auto discovery = refresh->discovery.lock();
    if (!discovery || refresh->session_generation !=
                          discovery->session_generation_.load(std::memory_order_acquire))
    {
        return;
    }
    if (result == ZNONODE)
    {
        discovery->PublishService(refresh->service, {}, refresh->session_generation);
        discovery->FinishRefresh(refresh->service, refresh->session_generation);
        return;
    }
    if (result != ZOK)
    {
        discovery->FinishRefresh(refresh->service, refresh->session_generation);
        return;
    }


    zhandle_t *handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(discovery->mutex_);
        if (refresh->session_generation == discovery->session_generation_.load(std::memory_order_acquire))
        {
            handle = discovery->handle_;
        }
    }
    if (handle == nullptr)
    {
        discovery->FinishRefresh(refresh->service, refresh->session_generation);
        return;
    }
    auto *next_context = new std::shared_ptr<DiscoveryRefresh>(refresh);
    const std::string path = discovery->options_.service_root + '/' + refresh->service + "/providers";
    const int read_result = zoo_awget_children(handle, path.c_str(), &ZookeeperServiceDiscovery::Watcher, discovery.get(),
                                               &ZookeeperServiceDiscovery::ChildrenCompletion, next_context);
    if (read_result != ZOK)
    {
        delete next_context;
        discovery->FinishRefresh(refresh->service, refresh->session_generation);
    }
}

void ZookeeperServiceDiscovery::ChildrenCompletion(int result, const ::String_vector *children, const void *context)
{
    std::unique_ptr<std::shared_ptr<DiscoveryRefresh>> holder(
        const_cast<std::shared_ptr<DiscoveryRefresh> *>(
            static_cast<const std::shared_ptr<DiscoveryRefresh> *>(context)));
    const std::shared_ptr<DiscoveryRefresh> refresh = *holder;
    const auto discovery = refresh->discovery.lock();
    if (!discovery || refresh->session_generation !=
                          discovery->session_generation_.load(std::memory_order_acquire))
    {
        return;
    }
    if (result == ZNONODE)
    {
        discovery->PublishService(refresh->service, {}, refresh->session_generation);
        discovery->FinishRefresh(refresh->service, refresh->session_generation);
        return;
    }
    if (result != ZOK || children == nullptr)
    {
        discovery->FinishRefresh(refresh->service, refresh->session_generation);
        return;
    }
    if (children->count == 0)
    {
        discovery->PublishService(refresh->service, {}, refresh->session_generation);
        discovery->FinishRefresh(refresh->service, refresh->session_generation);
        return;
    }

    zhandle_t *handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(discovery->mutex_);
        handle = discovery->handle_;
    }
    refresh->remaining = static_cast<std::size_t>(children->count);
    refresh->endpoints.reserve(refresh->remaining);
    for (int index = 0; index < children->count; ++index)
    {
        const std::string address_key = children->data[index] == nullptr ? std::string() : children->data[index];
        if (handle == nullptr || address_key.empty())
        {
            refresh->failed = true;
            if (--refresh->remaining == 0)
            {
                std::lock_guard<std::mutex> lock(discovery->mutex_);
                discovery->refresh_again_.insert(refresh->service);
            }
            continue;
        }
        auto *endpoint_context = new std::pair<std::shared_ptr<DiscoveryRefresh>, std::string>(refresh, address_key);
        const std::string path = discovery->options_.service_root + '/' + refresh->service + "/providers/" + address_key;
        const int read_result = zoo_awget(handle, path.c_str(), &ZookeeperServiceDiscovery::Watcher, discovery.get(),
                                          &ZookeeperServiceDiscovery::EndpointCompletion, endpoint_context);
        if (read_result != ZOK)
        {
            delete endpoint_context;
            refresh->failed = true;
            if (--refresh->remaining == 0)
            {
                std::lock_guard<std::mutex> lock(discovery->mutex_);
                discovery->refresh_again_.insert(refresh->service);
            }
        }
    }
    if (refresh->remaining == 0)
    {
        discovery->FinishRefresh(refresh->service, refresh->session_generation);
    }
}

void ZookeeperServiceDiscovery::EndpointCompletion(int result, const char *value, int value_length, const ::Stat *,
                                                    const void *context)
{
    std::unique_ptr<std::pair<std::shared_ptr<DiscoveryRefresh>, std::string>> read(
        const_cast<std::pair<std::shared_ptr<DiscoveryRefresh>, std::string> *>(
            static_cast<const std::pair<std::shared_ptr<DiscoveryRefresh>, std::string> *>(context)));
    const std::shared_ptr<DiscoveryRefresh> refresh = read->first;
    const auto discovery = refresh->discovery.lock();
    if (!discovery || refresh->session_generation !=
                          discovery->session_generation_.load(std::memory_order_acquire))
    {
        return;
    }
    transport::Endpoint endpoint;
    if (result == ZOK && ParseEndpoint(read->second, value, value_length, endpoint))
    {
        refresh->endpoints.emplace(read->second, std::move(endpoint));
    }
    else
    {
        refresh->failed = true;
    }

    if (--refresh->remaining != 0)
    {
        return;
    }
    if (refresh->failed)
    {
        std::lock_guard<std::mutex> lock(discovery->mutex_);
        discovery->refresh_again_.insert(refresh->service);
    }
    else
    {
        discovery->PublishService(refresh->service, std::move(refresh->endpoints), refresh->session_generation);
    }
    discovery->FinishRefresh(refresh->service, refresh->session_generation);
}

void ZookeeperServiceDiscovery::PublishService(
    std::string service, std::unordered_map<std::string, transport::Endpoint> endpoints,
    std::uint64_t generation)
{
    if (started_.load(std::memory_order_acquire) &&
        generation == session_generation_.load(std::memory_order_acquire))
    {
        sink_(std::move(service), std::move(endpoints));
    }
}

void ZookeeperServiceDiscovery::FinishRefresh(const std::string &service, std::uint64_t generation)
{
    bool again = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (generation != session_generation_.load(std::memory_order_acquire))
        {
            return;
        }
        refresh_pending_.erase(service);
        again = refresh_again_.erase(service) != 0 && started_.load(std::memory_order_acquire);
    }
    if (again)
    {
        RefreshService(service);
    }
}

bool ZookeeperServiceDiscovery::ParseEndpoint(const std::string &address_key, const char *value, int value_length,
                                               transport::Endpoint &endpoint)
{
    std::string host;
    const char *port_first = nullptr;
    if (!address_key.empty() && address_key.front() == '[')
    {
        const std::size_t closing = address_key.find("]:");
        if (closing == std::string::npos || closing == 1 || closing + 2 == address_key.size())
        {
            return false;
        }
        host = address_key.substr(1, closing - 1);
        port_first = address_key.data() + closing + 2;
    }
    else
    {
        const std::size_t separator = address_key.rfind(':');
        if (separator == std::string::npos || separator == 0 || separator + 1 == address_key.size() ||
            address_key.find(':') != separator)
        {
            return false;
        }
        host = address_key.substr(0, separator);
        port_first = address_key.data() + separator + 1;
    }
    unsigned int port = 0;
    const char *last = address_key.data() + address_key.size();
    const auto parsed = std::from_chars(port_first, last, port);
    if (parsed.ec != std::errc{} || parsed.ptr != last || port == 0 || port > 65535 || value == nullptr ||
        value_length <= 0)
    {
        return false;
    }

    try
    {
        const nlohmann::json document = nlohmann::json::parse(value, value + value_length);
        if (!document.contains("methods") || !document["methods"].is_object())
        {
            return false;
        }
        endpoint.host = std::move(host);
        endpoint.port = static_cast<std::uint16_t>(port);
        for (const auto &item : document["methods"].items())
        {
            if (!ValidPathSegment(item.key()) || !item.value().is_object())
            {
                return false;
            }
            transport::MethodCapability capability;
            capability.service_version = item.value().value("version", std::string());
            capability.codec = item.value().value("codec", std::string());
            capability.request_schema = item.value().value("request_schema", std::string());
            capability.response_schema = item.value().value("response_schema", std::string());
            if (!capability.valid())
            {
                return false;
            }
            endpoint.methods.emplace(item.key(), std::move(capability));
        }
        return endpoint.valid() && endpoint.address_key() == address_key;
    }
    catch (...)
    {
        return false;
    }
}

std::string ZookeeperServiceDiscovery::ServiceFromPath(const char *path) const
{
    if (path == nullptr)
    {
        return {};
    }
    const std::string value(path);
    const std::string prefix = options_.service_root + '/';
    if (value.compare(0, prefix.size(), prefix) != 0)
    {
        return {};
    }
    const std::size_t providers = value.find("/providers", prefix.size());
    if (providers == std::string::npos ||
        (providers + 10 != value.size() && value[providers + 10] != '/'))
    {
        return {};
    }
    const std::string service = value.substr(prefix.size(), providers - prefix.size());
    return ValidPathSegment(service) ? service : std::string();
}

} // namespace rpc::discovery
