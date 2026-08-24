#include "discovery/zookeeper_service_registry.h"

extern "C" {
#include "zookeeper.h"
}

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <utility>

#include "third_part/nlohmann/json.hpp"

namespace rpc::discovery {
namespace {

bool ValidPathSegment(const std::string &value)
{
    return !value.empty() && value != "." && value != ".." && value.find('/') == std::string::npos;
}

} // namespace

ZookeeperServiceRegistry::ZookeeperServiceRegistry(ZookeeperRegistryOptions options) : options_(std::move(options))
{
    while (options_.service_root.size() > 1 && options_.service_root.back() == '/')
    {
        options_.service_root.pop_back();
    }
    if (options_.hosts.empty() || options_.service_root.size() <= 1 || options_.service_root.front() != '/' ||
        options_.service_root.find('/', 1) != std::string::npos || options_.host.empty() || options_.port == 0 ||
        options_.session_timeout_ms <= 0)
    {
        throw std::invalid_argument("invalid ZooKeeper registry options");
    }
}

ZookeeperServiceRegistry::~ZookeeperServiceRegistry()
{
    Stop();
}

bool ZookeeperServiceRegistry::Start()
{
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
    {
        return false;
    }
    zhandle_t *handle = zookeeper_init(options_.hosts.c_str(), &ZookeeperServiceRegistry::Watcher,
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

bool ZookeeperServiceRegistry::Publish(
    std::unordered_map<std::string,
        std::unordered_map<std::string, transport::MethodCapability>> services)
{
    if (services.empty())
    {
        return false;
    }
    for (const auto &service : services)
    {
        if (!ValidPathSegment(service.first) || service.second.empty())
        {
            return false;
        }
        for (const auto &method : service.second)
        {
            if (!ValidPathSegment(method.first) || !method.second.valid())
            {
                return false;
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_.load(std::memory_order_acquire))
        {
            return false;
        }
        if (publishing_)
        {
            session_generation_.fetch_add(1, std::memory_order_acq_rel);
            publishing_ = false;
            publishing_services_.clear();
            publishing_index_ = 0;
            publish_failed_ = false;
        }
        services_ = std::move(services);
        publish_again_ = false;
        published_.store(false, std::memory_order_release);
    }
    BeginPublish();
    return true;
}

void ZookeeperServiceRegistry::Unpublish()
{
    zhandle_t *handle = nullptr;
    std::vector<std::string> paths;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        services_.clear();
        paths.assign(published_paths_.begin(), published_paths_.end());
        published_paths_.clear();
        publishing_services_.clear();
        publishing_ = false;
        publish_again_ = false;
        publish_failed_ = false;
        session_generation_.fetch_add(1, std::memory_order_acq_rel);
        handle = handle_;
        published_.store(false, std::memory_order_release);
    }
    if (handle != nullptr)
    {
        for (const std::string &path : paths)
        {
            zoo_delete(handle, path.c_str(), -1);
        }
    }
}

void ZookeeperServiceRegistry::Stop()
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
        publishing_ = false;
        publish_again_ = false;
        published_paths_.clear();
        session_generation_.fetch_add(1, std::memory_order_acq_rel);
        published_.store(false, std::memory_order_release);
    }
    if (handle != nullptr)
    {
        zookeeper_close(handle);
    }
}

void ZookeeperServiceRegistry::Watcher(zhandle_t *handle, int type, int state, const char *, void *context)
{
    auto *registry = static_cast<ZookeeperServiceRegistry *>(context);
    if (registry == nullptr || !registry->started_.load(std::memory_order_acquire) || type != ZOO_SESSION_EVENT)
    {
        return;
    }
    if (state == ZOO_EXPIRED_SESSION_STATE)
    {
        registry->ReconnectExpired(handle);
        return;
    }
    if (state != ZOO_CONNECTED_STATE)
    {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(registry->mutex_);
        if (registry->handle_ != handle)
        {
            return;
        }
    }
    registry->BeginPublish();
}

void ZookeeperServiceRegistry::ReconnectExpired(zhandle_t *expired_handle)
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
        publishing_ = false;
        publish_again_ = false;
        published_paths_.clear();
        published_.store(false, std::memory_order_release);
    }
    zookeeper_close(expired_handle);
    if (!started_.load(std::memory_order_acquire))
    {
        return;
    }
    zhandle_t *replacement = zookeeper_init(options_.hosts.c_str(), &ZookeeperServiceRegistry::Watcher,
                                             options_.session_timeout_ms, nullptr, this, 0);
    if (replacement == nullptr)
    {
        return;
    }
    bool retained = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (started_.load(std::memory_order_acquire) && handle_ == nullptr &&
            session_generation_.load(std::memory_order_acquire) == generation)
        {
            handle_ = replacement;
            retained = true;
        }
    }
    if (!retained)
    {
        zookeeper_close(replacement);
    }
}

void ZookeeperServiceRegistry::BeginPublish()
{
    zhandle_t *handle = nullptr;
    std::uint64_t generation = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_.load(std::memory_order_acquire) || handle_ == nullptr || services_.empty() ||
            zoo_state(handle_) != ZOO_CONNECTED_STATE)
        {
            return;
        }
        if (publishing_)
        {
            publish_again_ = true;
            return;
        }
        publishing_ = true;
        publish_again_ = false;
        publish_failed_ = false;
        publishing_index_ = 0;
        publishing_services_.clear();
        publishing_services_.reserve(services_.size());
        for (const auto &service : services_)
        {
            publishing_services_.push_back(service.first);
        }
        std::sort(publishing_services_.begin(), publishing_services_.end());
        handle = handle_;
        generation = session_generation_.load(std::memory_order_acquire);
    }
    auto *operation = new std::pair<ZookeeperServiceRegistry *, std::uint64_t>(this, generation);
    const int result = zoo_acreate(handle, options_.service_root.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE, 0,
                                   &ZookeeperServiceRegistry::RootCompletion, operation);
    if (result != ZOK)
    {
        delete operation;
        FinishPublish(generation);
    }
}

void ZookeeperServiceRegistry::RootCompletion(int result, const char *, const void *context)
{
    std::unique_ptr<std::pair<ZookeeperServiceRegistry *, std::uint64_t>> operation(
        const_cast<std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(
            static_cast<const std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(context)));
    if (result != ZOK && result != ZNODEEXISTS)
    {
        operation->first->FinishPublish(operation->second);
        return;
    }
    operation->first->PublishCurrentService(operation->second);
}

void ZookeeperServiceRegistry::PublishCurrentService(std::uint64_t generation)
{
    zhandle_t *handle = nullptr;
    std::string path;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!publishing_ || generation != session_generation_.load(std::memory_order_acquire) ||
            publishing_index_ >= publishing_services_.size())
        {
            return;
        }
        handle = handle_;
        path = options_.service_root + '/' + publishing_services_[publishing_index_];
    }
    auto *operation = new std::pair<ZookeeperServiceRegistry *, std::uint64_t>(this, generation);
    const int result = handle == nullptr ? ZINVALIDSTATE
                                         : zoo_acreate(handle, path.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE, 0,
                                                       &ZookeeperServiceRegistry::ServiceCompletion, operation);
    if (result != ZOK)
    {
        delete operation;
        FinishCurrentService(generation, false);
    }
}

void ZookeeperServiceRegistry::ServiceCompletion(int result, const char *, const void *context)
{
    std::unique_ptr<std::pair<ZookeeperServiceRegistry *, std::uint64_t>> operation(
        const_cast<std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(
            static_cast<const std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(context)));
    if (result != ZOK && result != ZNODEEXISTS)
    {
        operation->first->FinishCurrentService(operation->second, false);
        return;
    }
    operation->first->PublishProviders(operation->second);
}

void ZookeeperServiceRegistry::PublishProviders(std::uint64_t generation)
{
    zhandle_t *handle = nullptr;
    std::string path;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!publishing_ || generation != session_generation_.load(std::memory_order_acquire) ||
            publishing_index_ >= publishing_services_.size())
        {
            return;
        }
        handle = handle_;
        path = options_.service_root + '/' + publishing_services_[publishing_index_] + "/providers";
    }
    auto *operation = new std::pair<ZookeeperServiceRegistry *, std::uint64_t>(this, generation);
    const int result = handle == nullptr ? ZINVALIDSTATE
                                         : zoo_acreate(handle, path.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE, 0,
                                                       &ZookeeperServiceRegistry::ProvidersCompletion, operation);
    if (result != ZOK)
    {
        delete operation;
        FinishCurrentService(generation, false);
    }
}

void ZookeeperServiceRegistry::ProvidersCompletion(int result, const char *, const void *context)
{
    std::unique_ptr<std::pair<ZookeeperServiceRegistry *, std::uint64_t>> operation(
        const_cast<std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(
            static_cast<const std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(context)));
    if (result != ZOK && result != ZNODEEXISTS)
    {
        operation->first->FinishCurrentService(operation->second, false);
        return;
    }
    operation->first->PublishEndpoint(operation->second);
}

void ZookeeperServiceRegistry::PublishEndpoint(std::uint64_t generation)
{
    zhandle_t *handle = nullptr;
    std::string path;
    std::string data;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!publishing_ || generation != session_generation_.load(std::memory_order_acquire) ||
            publishing_index_ >= publishing_services_.size())
        {
            return;
        }
        handle = handle_;
        const std::string &service = publishing_services_[publishing_index_];
        path = options_.service_root + '/' + service + "/providers/" + AddressKey();
        data = Encode(service);
    }
    auto *operation = new std::pair<ZookeeperServiceRegistry *, std::uint64_t>(this, generation);
    const int result = handle == nullptr
                           ? ZINVALIDSTATE
                           : zoo_acreate(handle, path.c_str(), data.data(), static_cast<int>(data.size()),
                                         &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL,
                                         &ZookeeperServiceRegistry::EndpointCompletion, operation);
    if (result != ZOK)
    {
        delete operation;
        FinishCurrentService(generation, false);
    }
}

void ZookeeperServiceRegistry::EndpointCompletion(int result, const char *value, const void *context)
{
    std::unique_ptr<std::pair<ZookeeperServiceRegistry *, std::uint64_t>> operation(
        const_cast<std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(
            static_cast<const std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(context)));
    ZookeeperServiceRegistry *registry = operation->first;
    const std::uint64_t generation = operation->second;
    if (result == ZOK)
    {
        if (value != nullptr)
        {
            std::lock_guard<std::mutex> lock(registry->mutex_);
            if (generation == registry->session_generation_.load(std::memory_order_acquire))
            {
                registry->published_paths_.insert(value);
            }
        }
        registry->FinishCurrentService(generation, true);
        return;
    }
    if (result != ZNODEEXISTS)
    {
        registry->FinishCurrentService(generation, false);
        return;
    }

    zhandle_t *handle = nullptr;
    std::string path;
    {
        std::lock_guard<std::mutex> lock(registry->mutex_);
        if (!registry->publishing_ || generation != registry->session_generation_.load(std::memory_order_acquire) ||
            registry->publishing_index_ >= registry->publishing_services_.size())
        {
            return;
        }
        handle = registry->handle_;
        path = registry->options_.service_root + '/' +
               registry->publishing_services_[registry->publishing_index_] + "/providers/" +
               registry->AddressKey();
    }
    auto *next = new std::pair<ZookeeperServiceRegistry *, std::uint64_t>(registry, generation);
    const int exists_result = handle == nullptr
                                  ? ZINVALIDSTATE
                                  : zoo_aexists(handle, path.c_str(), 0,
                                                &ZookeeperServiceRegistry::ExistingEndpointCompletion, next);
    if (exists_result != ZOK)
    {
        delete next;
        registry->FinishCurrentService(generation, false);
    }
}

void ZookeeperServiceRegistry::ExistingEndpointCompletion(int result, const ::Stat *stat, const void *context)
{
    std::unique_ptr<std::pair<ZookeeperServiceRegistry *, std::uint64_t>> operation(
        const_cast<std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(
            static_cast<const std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(context)));
    ZookeeperServiceRegistry *registry = operation->first;
    const std::uint64_t generation = operation->second;
    zhandle_t *handle = nullptr;
    std::string path;
    std::string data;
    {
        std::lock_guard<std::mutex> lock(registry->mutex_);
        if (result != ZOK || stat == nullptr || !registry->publishing_ ||
            generation != registry->session_generation_.load(std::memory_order_acquire) ||
            registry->publishing_index_ >= registry->publishing_services_.size())
        {
            handle = nullptr;
        }
        else
        {
            handle = registry->handle_;
            const clientid_t *client = handle == nullptr ? nullptr : zoo_client_id(handle);
            if (client == nullptr || stat->ephemeralOwner == 0 || stat->ephemeralOwner != client->client_id)
            {
                handle = nullptr;
            }
            else
            {
                const std::string &service = registry->publishing_services_[registry->publishing_index_];
                path = registry->options_.service_root + '/' + service + "/providers/" + registry->AddressKey();
                data = registry->Encode(service);
            }
        }
    }
    if (handle == nullptr)
    {
        registry->FinishCurrentService(generation, false);
        return;
    }
    auto *next = new std::pair<ZookeeperServiceRegistry *, std::uint64_t>(registry, generation);
    const int set_result = zoo_aset(handle, path.c_str(), data.data(), static_cast<int>(data.size()), -1,
                                    &ZookeeperServiceRegistry::SetCompletion, next);
    if (set_result != ZOK)
    {
        delete next;
        registry->FinishCurrentService(generation, false);
    }
}

void ZookeeperServiceRegistry::SetCompletion(int result, const ::Stat *, const void *context)
{
    std::unique_ptr<std::pair<ZookeeperServiceRegistry *, std::uint64_t>> operation(
        const_cast<std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(
            static_cast<const std::pair<ZookeeperServiceRegistry *, std::uint64_t> *>(context)));
    ZookeeperServiceRegistry *registry = operation->first;
    const std::uint64_t generation = operation->second;
    if (result == ZOK)
    {
        std::lock_guard<std::mutex> lock(registry->mutex_);
        if (generation == registry->session_generation_.load(std::memory_order_acquire) &&
            registry->publishing_index_ < registry->publishing_services_.size())
        {
            registry->published_paths_.insert(
                registry->options_.service_root + '/' + registry->publishing_services_[registry->publishing_index_] +
                "/providers/" + registry->AddressKey());
        }
    }
    registry->FinishCurrentService(generation, result == ZOK);
}

void ZookeeperServiceRegistry::FinishCurrentService(std::uint64_t generation, bool success)
{
    bool more = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!publishing_ || generation != session_generation_.load(std::memory_order_acquire))
        {
            return;
        }
        publish_failed_ = publish_failed_ || !success;
        ++publishing_index_;
        more = publishing_index_ < publishing_services_.size();
    }
    if (more)
    {
        PublishCurrentService(generation);
    }
    else
    {
        FinishPublish(generation);
    }
}

void ZookeeperServiceRegistry::FinishPublish(std::uint64_t generation)
{
    bool again = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!publishing_ || generation != session_generation_.load(std::memory_order_acquire))
        {
            return;
        }
        const bool success = !publish_failed_ && publishing_index_ == publishing_services_.size() &&
                             !publishing_services_.empty();
        published_.store(success, std::memory_order_release);
        publishing_ = false;
        publishing_services_.clear();
        publishing_index_ = 0;
        publish_failed_ = false;
        again = publish_again_;
        publish_again_ = false;
    }
    if (again)
    {
        BeginPublish();
    }
}

std::string ZookeeperServiceRegistry::Encode(const std::string &service) const
{
    nlohmann::json document;
    document["methods"] = nlohmann::json::object();
    const auto found = services_.find(service);
    if (found == services_.end())
    {
        return document.dump();
    }
    for (const auto &method : found->second)
    {
        document["methods"][method.first] = {{"version", method.second.service_version},
                                               {"codec", method.second.codec},
                                               {"request_schema", method.second.request_schema},
                                               {"response_schema", method.second.response_schema}};
    }
    return document.dump();
}

std::string ZookeeperServiceRegistry::AddressKey() const
{
    if (options_.host.find(':') != std::string::npos &&
        (options_.host.empty() || options_.host.front() != '['))
    {
        return '[' + options_.host + "]:" + std::to_string(options_.port);
    }
    return options_.host + ':' + std::to_string(options_.port);
}

} // namespace rpc::discovery
