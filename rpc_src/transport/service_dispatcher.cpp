#include "transport/service_dispatcher.h"

#include "transport/rpc_status.h"

#include <utility>

namespace rpc::transport {

ServiceDispatcher::ServiceDispatcher() : methods_(std::make_shared<const MethodTable>())
{
}

bool ServiceDispatcher::Register(std::string service, std::string method, RpcHandler handler,
                                 RpcExecutionMode mode, MethodCapability capability)
{
    if (service.empty() || method.empty() || !handler || !capability.valid())
    {
        return false;
    }
    std::lock_guard<std::mutex> lock(update_mutex_);
    const auto current = std::atomic_load_explicit(&methods_, std::memory_order_acquire);
    auto next = std::make_shared<MethodTable>(*current);
    std::string key = Key(service, method);
    Method entry{std::move(handler), {}, mode, std::move(capability), service, method};
    const bool inserted = next->emplace(std::move(key), std::move(entry)).second;
    if (inserted)
    {
        std::atomic_store_explicit(&methods_, std::shared_ptr<const MethodTable>(std::move(next)),
                                   std::memory_order_release);
        generation_.fetch_add(1, std::memory_order_release);
    }
    return inserted;
}

bool ServiceDispatcher::RegisterInline(std::string service, std::string method, RpcHandler handler,
                                       RpcViewHandler view_handler, MethodCapability capability)
{
    if (service.empty() || method.empty() || !handler || !view_handler || !capability.valid())
    {
        return false;
    }
    std::lock_guard<std::mutex> lock(update_mutex_);
    const auto current = std::atomic_load_explicit(&methods_, std::memory_order_acquire);
    auto next = std::make_shared<MethodTable>(*current);
    std::string key = Key(service, method);
    Method entry{std::move(handler), std::move(view_handler),
                 RpcExecutionMode::NETWORK_INLINE, std::move(capability), service, method};
    const bool inserted = next->emplace(std::move(key), std::move(entry)).second;
    if (inserted)
    {
        std::atomic_store_explicit(&methods_, std::shared_ptr<const MethodTable>(std::move(next)),
                                   std::memory_order_release);
        generation_.fetch_add(1, std::memory_order_release);
    }
    return inserted;
}

bool ServiceDispatcher::Unregister(const std::string &service, const std::string &method)
{
    std::lock_guard<std::mutex> lock(update_mutex_);
    const auto current = std::atomic_load_explicit(&methods_, std::memory_order_acquire);
    if (current->find(Key(service, method)) == current->end())
    {
        return false;
    }
    auto next = std::make_shared<MethodTable>(*current);
    next->erase(Key(service, method));
    std::atomic_store_explicit(&methods_, std::shared_ptr<const MethodTable>(std::move(next)),
                               std::memory_order_release);
    generation_.fetch_add(1, std::memory_order_release);
    return true;
}

bool ServiceDispatcher::Contains(const std::string &service, const std::string &method) const
{
    const auto snapshot = std::atomic_load_explicit(&methods_, std::memory_order_acquire);
    return snapshot->find(Key(service, method)) != snapshot->end();
}

RpcExecutionMode ServiceDispatcher::ExecutionMode(const protocol::RequestMetadata &metadata) const
{
    const Method *method = Lookup(metadata.service, metadata.method);
    return method == nullptr ? RpcExecutionMode::CPU_POOL : method->mode;
}

RpcExecutionMode ServiceDispatcher::ExecutionMode(const protocol::RequestMetadataView &metadata) const
{
    const Method *method = Lookup(metadata.service, metadata.method);
    return method == nullptr ? RpcExecutionMode::CPU_POOL : method->mode;
}

protocol::Frame ServiceDispatcher::Dispatch(const ServerRequest &request) const
{
    const Method *method = Lookup(request.metadata.service, request.metadata.method);
    if (method == nullptr)
    {
        protocol::Frame response;
        SetResponseStatus(response, RpcStatus::UNIMPLEMENTED);
        return response;
    }
    return method->handler(request);
}

protocol::FrameView ServiceDispatcher::DispatchInline(const ServerRequestView &request) const
{
    const Method *method = Lookup(request.metadata.service, request.metadata.method);
    if (method == nullptr || !method->view_handler)
    {
        protocol::FrameView response;
        response.header.flags = static_cast<std::uint32_t>(RpcStatus::UNIMPLEMENTED);
        return response;
    }
    return method->view_handler(request);
}

ServiceDispatcher::CapabilityMap ServiceDispatcher::Capabilities() const
{
    CapabilityMap result;
    const auto snapshot = std::atomic_load_explicit(&methods_, std::memory_order_acquire);
    for (const auto &entry : *snapshot)
    {
        result[entry.second.service][entry.second.method] = entry.second.capability;
    }
    return result;
}

const ServiceDispatcher::Method *ServiceDispatcher::Lookup(std::string_view service, std::string_view method) const
{
    struct Cache {
        const ServiceDispatcher *owner = nullptr;
        std::uint64_t generation = 0;
        std::string service;
        std::string method_name;
        std::shared_ptr<const MethodTable> snapshot;
        const Method *method = nullptr;
    };
    thread_local Cache cache;

    const std::uint64_t generation = generation_.load(std::memory_order_acquire);
    if (cache.owner == this && cache.generation == generation &&
        cache.service == service && cache.method_name == method)
    {
        return cache.method;
    }

    auto snapshot = std::atomic_load_explicit(&methods_, std::memory_order_acquire);
    const auto found = snapshot->find(Key(std::string(service), std::string(method)));
    cache.owner = this;
    cache.generation = generation;
    cache.service.assign(service.data(), service.size());
    cache.method_name.assign(method.data(), method.size());
    cache.snapshot = std::move(snapshot);
    cache.method = found == cache.snapshot->end() ? nullptr : &found->second;
    return cache.method;
}

std::string ServiceDispatcher::Key(const std::string &service, const std::string &method)
{
    return service + '\n' + method;
}

} // namespace rpc::transport
