#include "task/crtco.h"
#include "task/co_scope.h"
#include "transport/rpc_client.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

using namespace rpc::transport;
using namespace rpc::runtime;

int main()
{
    RpcClientOptions opts;
    opts.io_threads = 8;
    opts.max_calls_per_shard = 4096;

    auto &client = RpcClient::GetInstance(opts);

    std::this_thread::sleep_for(std::chrono::seconds(3));

    const int num_threads = 50;
    const int duration_seconds = 6;
    const int warmup_seconds = 3;
    const int max_concurrent_requests = 20000; // In-flight request limit.

    std::atomic<int> in_flight{0};
    std::atomic<uint64_t> total_requests{0};
    std::atomic<uint64_t> total_successful{0};
    std::atomic<uint64_t> total_failed{0};
    std::atomic<uint64_t> total_rejected{0};
    std::atomic<bool> warmup_done{false};
    std::atomic<bool> should_stop{false};

    std::cout << "Starting async benchmark with flow control..." << std::endl;
    std::cout << "Worker threads: " << num_threads << std::endl;
    std::cout << "Duration: " << duration_seconds << "s (+" << warmup_seconds << "s warmup)" << std::endl;
    std::cout << "Max concurrent: " << max_concurrent_requests << std::endl;
    std::cout << "Mode: Pure async with flow control" << std::endl;

    auto warmup_thread = std::thread([&]()
                                     {
        std::this_thread::sleep_for(std::chrono::seconds(warmup_seconds));
        total_requests.store(0, std::memory_order_release);
        total_successful.store(0, std::memory_order_release);
        total_failed.store(0, std::memory_order_release);
        total_rejected.store(0, std::memory_order_release);
        warmup_done.store(true, std::memory_order_release);
        std::cout << "[" << warmup_seconds << "s] Warmup complete, stats reset" << std::endl; });

    auto timer_thread = std::thread([&]()
                                    {
        std::this_thread::sleep_for(std::chrono::seconds(warmup_seconds + duration_seconds));
        should_stop.store(true, std::memory_order_release);
        std::cout << "[" << (warmup_seconds + duration_seconds) << "s] Time's up, stopping..." << std::endl; });

    auto monitor_thread = std::thread([&]()
                                      {
        while (!should_stop.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            if (warmup_done.load(std::memory_order_acquire)) {
                std::cout << "[Monitor] In-flight: " << in_flight.load()
                          << ", Requests: " << total_requests.load()
                          << ", Success: " << total_successful.load()
                          << ", Failed: " << total_failed.load()
                          << ", Rejected: " << total_rejected.load() << std::endl;
            }
        } });

    std::vector<std::thread> workers;
    for (int i = 0; i < num_threads; ++i)
    {
        workers.emplace_back([&, thread_id = i]()
                             {
            while (!should_stop.load(std::memory_order_acquire)) {
                int current = in_flight.load(std::memory_order_acquire);
                if (current >= max_concurrent_requests) {
                    total_rejected.fetch_add(1, std::memory_order_relaxed);
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                    continue;
                }

                in_flight.fetch_add(1, std::memory_order_release);

                crtco() {
                    total_requests.fetch_add(1, std::memory_order_relaxed);

                    auto result = RpcStub::Call("EchoService", "Echo", "benchmark_async", 5000);

                    in_flight.fetch_sub(1, std::memory_order_release);

                    if (result.status == CallStatus::OK) {
                        total_successful.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        total_failed.fetch_add(1, std::memory_order_relaxed);
                    }
                };
            } });
    }

    warmup_thread.join();
    timer_thread.join();
    monitor_thread.join();
    for (auto &t : workers)
    {
        t.join();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    uint64_t requests = total_requests.load();
    uint64_t successful = total_successful.load();
    uint64_t failed = total_failed.load();
    uint64_t rejected = total_rejected.load();
    double qps = static_cast<double>(requests) / duration_seconds;

    std::cout << "\n=== Async Benchmark Results ===" << std::endl;
    std::cout << "Duration:       " << duration_seconds << "s" << std::endl;
    std::cout << "Total Requests: " << requests << std::endl;
    std::cout << "Successful:     " << successful << " ("
              << (requests > 0 ? (successful * 100.0 / requests) : 0) << "%)" << std::endl;
    std::cout << "Failed:         " << failed << std::endl;
    std::cout << "Rejected:       " << rejected << " (flow control)" << std::endl;
    std::cout << "QPS:            " << qps << " req/s" << std::endl;

    return 0;
}
