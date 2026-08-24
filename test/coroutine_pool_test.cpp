#include "runtime/coroutine_scheduler.h"
#include <chrono>
#include <iostream>
#include <atomic>

using namespace rpc::runtime;

std::atomic<int> malloc_count{0};

int main() {
    const int iterations = 1000000;

    {
        std::cout << "=== Test 1: Without Pool (baseline) ===\n";
        CoroutineScheduler::Options opts;
        opts.pool_prewarm_count = 0;
        opts.pool_max_size = 0;

        CoroutineScheduler scheduler(opts);
        int counter = 0;

        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto routine = scheduler.Spawn([&counter] {
                counter++;
            });
            scheduler.RunReady(1);
        }
        auto end = std::chrono::steady_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        std::cout << "  Counter: " << counter << "\n";
        std::cout << "  Time: " << ms << " ms\n";
        std::cout << "  Throughput: " << (iterations * 1000.0 / ms) << " ops/s\n\n";
    }

    {
        std::cout << "=== Test 2: With Pool (no prewarm) ===\n";
        CoroutineScheduler::Options opts;
        opts.pool_max_size = 512;
        opts.pool_prewarm_count = 0;

        CoroutineScheduler scheduler(opts);
        int counter = 0;

        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto routine = scheduler.Spawn([&counter] {
                counter++;
            });
            scheduler.RunReady(1);
        }
        auto end = std::chrono::steady_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        std::cout << "  Counter: " << counter << "\n";
        std::cout << "  Time: " << ms << " ms\n";
        std::cout << "  Throughput: " << (iterations * 1000.0 / ms) << " ops/s\n\n";
    }

    {
        std::cout << "=== Test 3: With Pool (prewarm 1000) ===\n";
        CoroutineScheduler::Options opts;
        opts.pool_max_size = 1024;
        opts.pool_prewarm_count = 1000;

        CoroutineScheduler scheduler(opts);
        int counter = 0;

        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto routine = scheduler.Spawn([&counter] {
                counter++;
            });
            scheduler.RunReady(1);
        }
        auto end = std::chrono::steady_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        std::cout << "  Counter: " << counter << "\n";
        std::cout << "  Time: " << ms << " ms\n";
        std::cout << "  Throughput: " << (iterations * 1000.0 / ms) << " ops/s\n\n";
    }

    return 0;
}
