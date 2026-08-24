#include "task/crtco.h"
#include "task/co_scope.h"
#include "transport/rpc_client.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace rpc::transport;
using namespace rpc::runtime;
using namespace std::chrono_literals;

namespace {

std::uint64_t Percentile(const std::vector<std::uint64_t> &sorted_samples,
                         std::size_t percentile)
{
    if (sorted_samples.empty())
    {
        return 0;
    }
    const std::size_t rank = (percentile * sorted_samples.size() + 99) / 100;
    return sorted_samples[std::min(rank - 1, sorted_samples.size() - 1)];
}

} // namespace

int main(int argc, char **argv)
{
    const int duration_seconds = argc > 1 ? std::stoi(argv[1]) : 30;
    const int discovery_wait_seconds = argc > 2 ? std::stoi(argv[2]) : 5;
    const int client_io_threads = argc > 3 ? std::stoi(argv[3]) : 8;
    const bool measure_latency = argc > 4 && std::stoi(argv[4]) != 0;
    const int payload_bytes = argc > 5 ? std::stoi(argv[5]) : 4;
    if (duration_seconds <= 0 || discovery_wait_seconds < 0 || client_io_threads <= 0 || payload_bytes <= 0)
    {
        throw std::invalid_argument(
            "duration, client I/O threads, and payload bytes must be positive; discovery wait must be non-negative");
    }
    const std::string payload(static_cast<std::size_t>(payload_bytes), 'x');

    RpcClientOptions opts;
    opts.io_threads = static_cast<std::size_t>(client_io_threads);
    opts.max_calls_per_shard = 4096;

    auto &client = RpcClient::GetInstance(opts);

    std::cout << "Waiting for service to be ready...\n";
    std::this_thread::sleep_for(std::chrono::seconds(discovery_wait_seconds));

    std::vector<int> concurrency_levels = {50};

    for (int num_threads : concurrency_levels)
    {
        std::cout << "\n=== Test: " << num_threads << " threads, " << duration_seconds
                  << " seconds, " << client_io_threads << " client shards, " << payload_bytes
                  << "B payload ===\n";

        std::atomic<int> completed{0};
        std::atomic<int> failed{0};
        std::atomic<bool> stop{false};
        std::vector<std::vector<std::uint64_t>> thread_latencies(num_threads);
        if (measure_latency)
        {
            for (auto &samples : thread_latencies)
            {
                samples.reserve(static_cast<std::size_t>(duration_seconds) * 4096);
            }
        }

        std::vector<std::thread> workers;
        for (int t = 0; t < num_threads; ++t)
        {
            workers.emplace_back([&, t]()
                                 {
                CoScope coscpe;
                while (!stop.load(std::memory_order_acquire)) {
                    const auto started_at = std::chrono::steady_clock::now();
                    bool call_succeeded = false;
                    crtco(&coscpe) {
                        auto r = RpcStub::Call("EchoService", "Echo", payload, 5000);
                        if (r.status == CallStatus::OK) {
                            call_succeeded = true;
                            completed.fetch_add(1, std::memory_order_release);
                        } else {
                            failed.fetch_add(1, std::memory_order_release);
                        }
                    };
                    coscpe.Join();
                    if (measure_latency && call_succeeded) {
                        const auto elapsed = std::chrono::steady_clock::now() - started_at;
                        thread_latencies[t].push_back(
                            static_cast<std::uint64_t>(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()));
                    }
                } });
        }

        // Monitor the requested measurement window.
        int last = 0;
        int max_qps = 0;
        for (int i = 0; i < duration_seconds; ++i)
        {
            std::this_thread::sleep_for(1s);
            int c = completed.load(std::memory_order_acquire);
            int f = failed.load(std::memory_order_acquire);
            int qps = c - last;
            if (qps > max_qps)
                max_qps = qps;
            last = c;

            if ((i + 1) % 5 == 0 || i < 3)
            {
                std::cout << "  [" << (i + 1) << "s] QPS: " << qps
                          << " | Total: " << c << " | Failed: " << f << "\n";
            }
        }

        stop.store(true, std::memory_order_release);
        for (auto &t : workers)
        {
            t.join();
        }

        int total = completed.load() + failed.load();
        std::cout << "  Results:\n"
                  << "    Total: " << total << "\n"
                  << "    Success: " << completed.load() << " ("
                  << (total > 0 ? 100.0 * completed.load() / total : 0) << "%)\n"
                  << "    Failed: " << failed.load() << " ("
                  << (total > 0 ? 100.0 * failed.load() / total : 0) << "%)\n"
                  << "    Average QPS: " << (completed.load() / duration_seconds) << "\n"
                  << "    Peak QPS: " << max_qps << "\n";

        if (measure_latency)
        {
            std::vector<std::uint64_t> samples;
            samples.reserve(static_cast<std::size_t>(completed.load()));
            for (const auto &thread_samples : thread_latencies)
            {
                samples.insert(samples.end(), thread_samples.begin(), thread_samples.end());
            }
            std::sort(samples.begin(), samples.end());
            const auto print_latency = [&](const char *label, std::uint64_t nanoseconds) {
                std::cout << "    " << label << ": " << std::fixed << std::setprecision(3)
                          << (static_cast<double>(nanoseconds) / 1'000'000.0) << " ms\n";
            };
            std::cout << "    Latency samples: " << samples.size() << "\n";
            print_latency("P50", Percentile(samples, 50));
            print_latency("P95", Percentile(samples, 95));
            print_latency("P99", Percentile(samples, 99));
            print_latency("Max", samples.empty() ? 0 : samples.back());
        }
    }

    return 0;
}
