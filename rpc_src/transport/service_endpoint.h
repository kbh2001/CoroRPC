#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

namespace rpc::transport {

struct MethodCapability {
    std::string service_version;
    std::string codec;
    std::string request_schema;
    std::string response_schema;

    bool valid() const noexcept { return !service_version.empty() && !codec.empty(); }
};

// One ZooKeeper leaf (/rpc/<service>/providers/<ip:port>). The service and
// method names are represented by the parent/key, so they are not duplicated.
struct Endpoint {
    std::string host;
    std::uint16_t port = 0;
    std::unordered_map<std::string, MethodCapability> methods;

    bool valid() const noexcept { return !host.empty() && port != 0 && !methods.empty(); }
    std::string address_key() const
    {
        if (host.find(':') != std::string::npos && (host.empty() || host.front() != '['))
        {
            return std::string("[") + host + "]:" + std::to_string(port);
        }
        return host + ':' + std::to_string(port);
    }

    bool Supports(const std::string &method) const noexcept
    {
        return methods.find(method) != methods.end();
    }
};

} // namespace rpc::transport
