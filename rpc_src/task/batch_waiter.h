#pragma once

// Batch completion waiter for user-thread synchronization. Unlike the
// request-oriented Completion in condition_changed.cpp, this object has no
// claim/timeout protocol: CoScope signals it once when its batch reaches zero.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <immintrin.h>
#include <semaphore.h>
#include <thread>

namespace rpc::runtime {

enum class NotifyMode : std::uint8_t {
    ConditionVariable = 0,
    SpinSemaphore = 1,
};

enum class BatchWaitState : std::uint8_t {
    Idle = 0,
    Armed = 1,
    Signaled = 2,
};

inline NotifyMode ParseNotifyMode(const char *value) noexcept
{
    if (value != nullptr &&
        (value[0] == 's' || value[0] == 'S' || value[0] == '1')) {
        return NotifyMode::SpinSemaphore;
    }
    return NotifyMode::ConditionVariable;
}

namespace detail {

struct NotifyConfig {
    std::atomic<bool> ready{false};
    std::atomic<std::uint64_t> spin_ns{0};
    std::atomic<int> max_spinners{1};
    std::atomic<int> spinners{0};
};

inline NotifyConfig &notify_config()
{
    static NotifyConfig *config = new NotifyConfig;
    return *config;
}

inline std::uint64_t NowNs() noexcept
{
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::uint64_t>(ts.tv_sec) * 1000000000ull +
           static_cast<std::uint64_t>(ts.tv_nsec);
}

inline std::uint64_t MeasureHandoffNs(int rounds = 2000)
{
    sem_t ping{}, pong{};
    if (sem_init(&ping, 0, 0) != 0 || sem_init(&pong, 0, 0) != 0) {
        std::abort();
    }
    auto take = [](sem_t *semaphore) {
        while (sem_wait(semaphore) != 0 && errno == EINTR) {
        }
    };
    std::thread peer([&] {
        for (int i = 0; i < rounds; ++i) {
            take(&ping);
            sem_post(&pong);
        }
    });
    const std::uint64_t start = NowNs();
    for (int i = 0; i < rounds; ++i) {
        sem_post(&ping);
        take(&pong);
    }
    const std::uint64_t elapsed = NowNs() - start;
    peer.join();
    sem_destroy(&ping);
    sem_destroy(&pong);
    return elapsed / static_cast<std::uint64_t>(std::max(1, rounds * 2));
}

inline void EnsureNotifyConfig()
{
    NotifyConfig &config = notify_config();
    if (config.ready.load(std::memory_order_acquire)) {
        return;
    }
    static const bool once = [&config] {
        if (!config.ready.load(std::memory_order_acquire)) {
            const unsigned ncpu = std::max(1u, std::thread::hardware_concurrency());
            config.spin_ns.store(ncpu == 1 ? 0 : MeasureHandoffNs(), std::memory_order_relaxed);
            // Keep one waiter spinning so the I/O shard threads retain CPU time.
            config.max_spinners.store(1, std::memory_order_relaxed);
            config.ready.store(true, std::memory_order_release);
        }
        return true;
    }();
    (void)once;
}

} // namespace detail

inline void ConfigureNotifySpin(std::uint64_t spin_ns, int max_spinners)
{
    auto &config = detail::notify_config();
    config.spin_ns.store(spin_ns, std::memory_order_relaxed);
    config.max_spinners.store(std::max(1, max_spinners), std::memory_order_relaxed);
    config.ready.store(true, std::memory_order_release);
}

inline NotifyMode &NotifyModeStorage() noexcept
{
    static NotifyMode mode = ParseNotifyMode(std::getenv("CORORPC_NOTIFY_MODE"));
    return mode;
}

inline NotifyMode GetNotifyMode() noexcept
{
    return NotifyModeStorage();
}

inline void SetNotifyMode(NotifyMode mode) noexcept
{
    NotifyModeStorage() = mode;
}

class BatchWaiter {
public:
    BatchWaiter()
    {
        if (sem_init(&semaphore_, 0, 0) != 0) {
            std::abort();
        }
    }

    ~BatchWaiter()
    {
        sem_destroy(&semaphore_);
    }

    BatchWaiter(const BatchWaiter &) = delete;
    BatchWaiter &operator=(const BatchWaiter &) = delete;

    void Signal() noexcept
    {
        if (sem_post(&semaphore_) != 0) {
            std::abort();
        }
    }

    void Wait()
    {
        detail::EnsureNotifyConfig();
        auto &config = detail::notify_config();
        const std::uint64_t budget = config.spin_ns.load(std::memory_order_relaxed);
        if (budget != 0 && Spin(config, budget)) {
            return;
        }
        while (sem_wait(&semaphore_) != 0) {
            if (errno != EINTR) {
                std::abort();
            }
        }
    }

private:
    bool Spin(detail::NotifyConfig &config, std::uint64_t budget)
    {
        if (config.spinners.fetch_add(1, std::memory_order_relaxed) >=
            config.max_spinners.load(std::memory_order_relaxed)) {
            config.spinners.fetch_sub(1, std::memory_order_relaxed);
            return false;
        }
        bool received = false;
        const std::uint64_t start = detail::NowNs();
        while (!received && detail::NowNs() - start < budget) {
            for (int i = 0; i < 4 && !received; ++i) {
                if (sem_trywait(&semaphore_) == 0) {
                    received = true;
                } else if (errno != EAGAIN && errno != EINTR) {
                    std::abort();
                } else {
                    _mm_pause();
                }
            }
        }
        config.spinners.fetch_sub(1, std::memory_order_relaxed);
        return received;
    }

    sem_t semaphore_{};
};

} // namespace rpc::runtime
