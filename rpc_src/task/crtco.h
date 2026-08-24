#pragma once

#include "transport/rpc_client.h"
#include "transport/multiplexed_connection.h"
#include "protocol/frame.h"
#include "protocol/rpc_metadata.h"
#include "runtime/coroutine.h"
#include "task/co_scope.h"

#include <string>
#include <string_view>
#include <iostream>

namespace rpc::transport {
namespace detail {

struct TaskSubmitter {
    rpc::runtime::CoScope* scope = nullptr;

    TaskSubmitter() = default;
    explicit TaskSubmitter(rpc::runtime::CoScope* sc) : scope(sc) {}

    template <class F>
    void operator+(F&& f) {
        if (scope) {
            scope->Dispatch(std::forward<F>(f));
        } else {
            auto& client = RpcClient::GetInstance();
            auto* task = client.AllocateTask();
            if (task) {
                task->SetLambda(std::forward<F>(f));
                client.SubmitTask(task);
            }
        }
    }
};

} // namespace detail

// Static RPC helpers used inside submitted tasks.
class RpcStub {
public:
    // Call from a client shard coroutine.
    static CallResult Call(std::string_view service,
                           std::string_view method,
                           std::string_view body,
                           std::uint64_t timeout_ms = 5000) {
        CallStatus failure = CallStatus::NOT_CONNECTED;
        auto *conn = RpcClient::GetConnectionForCall(service, method, {}, &failure);
        if (conn == nullptr) {
            return {failure, RpcStatus::OK, 0, {}};
        }
        protocol::RequestMetadataView metadata;
        metadata.service = service;
        metadata.method = method;
        metadata.timeout_ms = timeout_ms;
        return conn->Call(metadata, body, timeout_ms);
    }

    // request_schema is a caller-side input contract, such as
    // "int.char.string". It is checked against service discovery before the
    // request is sent; the response representation remains the caller's choice.
    static CallResult Call(std::string_view service,
                           std::string_view method,
                           std::string_view request_schema,
                           std::string_view body,
                           std::uint64_t timeout_ms = 5000) {
        CallStatus failure = CallStatus::NOT_CONNECTED;
        auto *conn = RpcClient::GetConnectionForCall(service, method, request_schema, &failure);
        if (conn == nullptr) {
            return {failure, RpcStatus::OK, 0, {}};
        }
        protocol::RequestMetadataView metadata;
        metadata.service = service;
        metadata.method = method;
        metadata.timeout_ms = timeout_ms;
        return conn->Call(metadata, body, timeout_ms);
    }

    static CallResult Call(const RpcCallOptions &options, std::string_view body) {
        CallStatus failure = CallStatus::NOT_CONNECTED;
        auto* conn = RpcClient::GetConnectionForCall(options.service, options.method,
                                                     options.request_schema, &failure);
        if (!conn) {
            return {failure, RpcStatus::OK, 0, {}};
        }

        protocol::RequestMetadataView metadata;
        metadata.service = options.service;
        metadata.method = options.method;
        metadata.idempotency_key = options.idempotency_key;
        metadata.application_metadata = options.application_metadata;
        metadata.timeout_ms = options.timeout_ms;

        return conn->Call(metadata, body, options.timeout_ms);
    }

    // Spawn a child coroutine on the current runtime.
    template <class F>
    static void Go(F&& f) {
        auto* runtime = runtime::Coroutine::CurrentRuntime();
        if (runtime) {
            runtime->Go(std::forward<F>(f));
        }
    }
};

} // namespace rpc::transport

// Submit a task to a client shard.
#define crtco(...) ::rpc::transport::detail::TaskSubmitter{__VA_ARGS__} + [&]() mutable
