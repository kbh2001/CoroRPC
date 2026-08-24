#include "task/co_scope.h"
#include "task/crtco.h"
#include "transport/rpc_client.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main(int argc, char **argv)
{
    rpc::transport::RpcClientOptions client_options;
    if (argc > 1)
    {
        client_options.zookeeper_hosts = argv[1];
    }
    if (argc > 2)
    {
        client_options.service_root = argv[2];
    }
    const std::size_t call_count = argc > 3 ? std::stoull(argv[3]) : 32;
    const std::size_t io_threads = argc > 4 ? std::stoull(argv[4]) : 1;
    client_options.io_threads = io_threads;

    const auto client = rpc::transport::RpcClient::Create(std::move(client_options));
    if (!client)
    {
        std::cerr << "RPC client initialization failed\n";
        return 1;
    }

    // Discovery callbacks are asynchronous. Give the client a short window to
    // receive the initial service snapshot before submitting the demo calls.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::atomic<std::size_t> successful{0};
    std::atomic<std::size_t> failed{0};
    rpc::runtime::CoScope scope(*client);
    for (std::size_t submitted = 0; submitted < call_count; ++submitted)
    {
        crtco(&scope) {
            const auto result = rpc::transport::RpcStub::Call(
                "EchoService", "Echo", "client_main", 1000);
            if (result.status == rpc::transport::CallStatus::OK &&
                result.rpc_status == rpc::transport::RpcStatus::OK)
            {
                successful.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                failed.fetch_add(1, std::memory_order_relaxed);
            }
        };
    }

    scope.Join();
    std::cout << "successful=" << successful.load(std::memory_order_relaxed)
              << " failed=" << failed.load(std::memory_order_relaxed) << '\n';
    client->Stop();
    return failed.load(std::memory_order_relaxed) == 0 ? 0 : 1;
}
