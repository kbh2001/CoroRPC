#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "transport/rpc_server.h"
#include "transport/service_endpoint.h"

namespace rpc::transport {

// Copy-on-write method table. Dispatch is lock-free; registration publishes a
// new immutable snapshot and may safely happen after the server starts.
class ServiceDispatcher {
public:
    using RpcViewHandler = std::function<protocol::FrameView(const ServerRequestView &)>;
    using CapabilityMap = std::unordered_map<std::string,
        std::unordered_map<std::string, MethodCapability>>;
    ServiceDispatcher();

    bool Register(std::string service, std::string method, RpcHandler handler,
                  RpcExecutionMode mode = RpcExecutionMode::CPU_POOL,
                  MethodCapability capability = {"v1", "raw", "bytes", "bytes"});
    bool RegisterInline(std::string service, std::string method, RpcHandler handler,
                        RpcViewHandler view_handler,
                        MethodCapability capability = {"v1", "raw", "bytes", "bytes"});
    bool Unregister(const std::string &service, const std::string &method);
    bool Contains(const std::string &service, const std::string &method) const;
    RpcExecutionMode ExecutionMode(const protocol::RequestMetadata &metadata) const;
    RpcExecutionMode ExecutionMode(const protocol::RequestMetadataView &metadata) const;
    protocol::Frame Dispatch(const ServerRequest &request) const;
    protocol::FrameView DispatchInline(const ServerRequestView &request) const;
    CapabilityMap Capabilities() const;

private:
    struct Method {
        RpcHandler handler;
        RpcViewHandler view_handler;
        RpcExecutionMode mode = RpcExecutionMode::CPU_POOL;
        MethodCapability capability;
        std::string service;
        std::string method;
    };
    using MethodTable = std::unordered_map<std::string, Method>;

    static std::string Key(const std::string &service, const std::string &method);
    const Method *Lookup(std::string_view service, std::string_view method) const;
    mutable std::mutex update_mutex_;
    std::shared_ptr<const MethodTable> methods_;
    std::atomic<std::uint64_t> generation_{1};
};

} // namespace rpc::transport
