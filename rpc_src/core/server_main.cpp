#include "transport/rpc_server.h"
#include "transport/rpc_status.h"

#include <arpa/inet.h>
#include <signal.h>

#include <atomic>
#include <fstream>
#include <iostream>

#include "third_part/nlohmann/json.hpp"

namespace {

std::atomic<bool> g_shutdown{false};

void SignalHandler(int)
{
    g_shutdown.store(true, std::memory_order_relaxed);
}

} // namespace

int main(int argc, char **argv)
{
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);
    try
    {
        const std::string config_path = argc > 1 ? argv[1] : "config/rpc_server.json";
        std::ifstream config_file(config_path);
        if (!config_file)
        {
            std::cerr << "cannot open server configuration: " << config_path << '\n';
            return 1;
        }
        const nlohmann::json config = nlohmann::json::parse(config_file);
        const std::string listen_ip = config.value("listen_ip", "0.0.0.0");
        const int listen_port = config.value("listen_port", 8989);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(static_cast<std::uint16_t>(listen_port));
        if (listen_port <= 0 || listen_port > 65535 || inet_pton(AF_INET, listen_ip.c_str(), &address.sin_addr) != 1)
        {
            std::cerr << "invalid listen address\n";
            return 1;
        }

        rpc::transport::RpcServerOptions options;
        options.runtime_options.shared_stack_count = 8;
        options.runtime_options.pool_prewarm_count = config.value("pool_prewarm_count", 1000U);
        options.runtime_options.pool_max_size = config.value("pool_max_size", 10000U);
        options.max_connections = config.value("max_connections", 1024U);
        options.max_inflight_per_connection = config.value("max_inflight_per_connection", 256U);
        options.max_inflight_requests = config.value("max_inflight_requests", 8192U);
        options.network_threads = config.value("network_threads", 2U);
        options.io_worker_threads = config.value("io_worker_threads", 2U);
        options.worker_threads = config.value("worker_threads", 4U);
        options.max_worker_queue = config.value("max_worker_queue", 4096U);
        options.zookeeper_hosts = config.value("zookeeper_hosts", std::string("127.0.0.1:2181"));
        options.service_root = config.value("service_root", std::string("/rpc"));
        options.advertise_host = config.value("advertise_ip", std::string("127.0.0.1"));
        options.advertise_port = static_cast<std::uint16_t>(listen_port);
        options.zookeeper_session_timeout_ms = config.value("zookeeper_session_timeout_ms", 10000);
        auto server = rpc::transport::RpcServer::Create(options);
        if (!server->RegisterInline("EchoService", "Echo", [](const rpc::transport::ServerRequest &request) {
                rpc::protocol::Frame response;
                if (request.context->IsCancelled())
                {
                    rpc::transport::SetResponseStatus(response, rpc::transport::RpcStatus::CANCELLED);
                    return response;
                }
                response.body = request.frame.body;
                return response;
            }, [](const rpc::transport::ServerRequestView &request) {
                rpc::protocol::FrameView response;
                if (request.context->IsCancelled())
                {
                    response.header.flags = static_cast<std::uint32_t>(rpc::transport::RpcStatus::CANCELLED);
                    return response;
                }
                response.body = request.frame.body;
                return response;
            }))
        {
            return 1;
        }
        if (!server->Listen(reinterpret_cast<const sockaddr *>(&address), sizeof(address), static_cast<int>(options.max_connections)) ||
            !server->Start())
        {
            std::cerr << "failed to start RPC server or register it in ZooKeeper\n";
            return 1;
        }
        std::cout << "RPC server listening on " << listen_ip << ':' << listen_port << '\n';
        server->RunUntil(g_shutdown);
        server->Shutdown(config.value("drain_timeout_ms", 10000U));
    }
    catch (const std::exception &error)
    {
        std::cerr << "RPC server failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
