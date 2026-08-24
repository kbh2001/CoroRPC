#include <atomic>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "memory/task_pool.h"

int main(int argc, char **argv) {
    const std::size_t thread_count =
        argc > 1 ? static_cast<std::size_t>(std::stoull(argv[1])) : 50;
    const std::size_t iterations_per_thread =
        argc > 2 ? static_cast<std::size_t>(std::stoull(argv[2])) : 20'000;

    rpc::memory::TaskPool<128> pool;
    std::atomic<bool> start{false};
    std::atomic<bool> duplicate{false};
    std::atomic<std::size_t> executed{0};
    std::mutex live_mutex;
    std::unordered_set<void *> live;
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    for (std::size_t thread_index = 0; thread_index < thread_count; ++thread_index) {
        threads.emplace_back([&, thread_index] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (std::size_t iteration = 0; iteration < iterations_per_thread; ++iteration) {
                auto *task = pool.Allocate();
                {
                    std::lock_guard<std::mutex> lock(live_mutex);
                    if (!live.insert(task).second) {
                        duplicate.store(true, std::memory_order_relaxed);
                    }
                }

                task->SetLambda([&executed] {
                    executed.fetch_add(1, std::memory_order_relaxed);
                });
                task->Run();

                if (((iteration + thread_index) & 63U) == 0) {
                    std::this_thread::yield();
                }

                {
                    std::lock_guard<std::mutex> lock(live_mutex);
                    live.erase(task);
                }
                pool.Free(task);
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto &thread : threads) {
        thread.join();
    }

    const std::size_t expected = thread_count * iterations_per_thread;
    if (duplicate.load(std::memory_order_relaxed) || !live.empty() ||
        executed.load(std::memory_order_relaxed) != expected) {
        std::cerr << "hazard TaskPool stress failed: duplicate="
                  << duplicate.load(std::memory_order_relaxed)
                  << " live=" << live.size()
                  << " executed=" << executed.load(std::memory_order_relaxed)
                  << " expected=" << expected << '\n';
        return 1;
    }

    std::cout << "hazard TaskPool stress passed: " << expected << " operations\n";
    return 0;
}
