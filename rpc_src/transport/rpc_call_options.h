#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace rpc::transport {

struct CircuitBreakerOptions {
    std::size_t window_size = 20;
    std::size_t minimum_requests = 10;
    std::size_t failure_threshold_percent = 50;
    std::uint64_t open_interval_ms = 1000;
    std::uint64_t max_open_interval_ms = 30000;
    std::size_t half_open_max_probes = 1;
};

struct RetryOptions {
    std::size_t max_attempts = 3;
    std::size_t retry_budget_capacity = 100;
    std::uint64_t base_backoff_ms = 5;
    std::uint64_t max_backoff_ms = 250;
};

struct RpcCallOptions {
    std::string service;
    std::string method;
    // Optional caller-side contract advertised through service discovery, for
    // example "int.char.string". An empty value preserves the raw-body API.
    std::string request_schema;
    std::string idempotency_key;
    std::string application_metadata;
    std::uint64_t timeout_ms = 1000;
};

} // namespace rpc::transport
