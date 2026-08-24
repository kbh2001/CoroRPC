#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <unordered_map>
#include <thread>
#include <vector>

#include "runtime/coroutine_routine.h"
#include "runtime/sanitizer_config.h"

namespace rpc::runtime {

class CoroutinePool;

class CoroutineScheduler {
public:
    static constexpr std::size_t kDefaultStackSize = 128 * 1024;

    struct Options {
#if RPC_RUNTIME_HAS_ASAN
        std::size_t shared_stack_count = 0;
#else
        std::size_t shared_stack_count = 1;
#endif
        std::size_t shared_stack_size = kDefaultStackSize;
        std::size_t pool_prewarm_count = 0;
        std::size_t pool_max_size = 0;
        std::size_t pool_stack_size = kDefaultStackSize;
    };

    CoroutineScheduler();
    explicit CoroutineScheduler(Options options);
    ~CoroutineScheduler();

    CoroutineScheduler(const CoroutineScheduler &) = delete;
    CoroutineScheduler &operator=(const CoroutineScheduler &) = delete;

    std::shared_ptr<Coroutine_Routine> Create(Coroutine_Routine::Entry entry,
                                              std::size_t stack_size = kDefaultStackSize);
    // Bypass the pool when prewarming it.
    std::shared_ptr<Coroutine_Routine> CreateRaw(Coroutine_Routine::Entry entry,
                                                  std::size_t stack_size = kDefaultStackSize);
    // Create in READY state and enqueue. The routine first runs from the root
    // context in a later RunReady() round, not from the caller's stack.
    std::shared_ptr<Coroutine_Routine> Spawn(Coroutine_Routine::Entry entry,
                                             std::size_t stack_size = kDefaultStackSize);
    // Zero runs the ready-queue snapshot present at entry. Routines made
    // ready while this snapshot runs wait for the next scheduling round.
    std::size_t RunReady(std::size_t max_count = 0);
    void Resume(const std::shared_ptr<Coroutine_Routine> &routine);
    // Transport event owners use the raw handle stored in their wait record.
    // The scheduler resolves it through routines_ before switching context.
    void Resume(Coroutine_Routine *routine);
    bool MakeReady(Coroutine_Routine *routine);
    bool HasReady() const noexcept { return !ready_queue_.empty(); }

    static void SuspendCurrent(CoroutineState waiting_state);
    static CoroutineScheduler *Current() noexcept;
    static Coroutine_Routine *CurrentRoutine() noexcept;

private:
    friend class Coroutine;
    friend class IoManager;

    struct SharedStackRestore {
        void *begin = nullptr;
        const void *data = nullptr;
        std::size_t size = 0;
    };

    static void CoroutineTrampoline() noexcept;

    void CheckOwnerThread() const;
    void InitializeContext(Coroutine_Routine &routine);
    SharedStackRestore PrepareSharedStack(Coroutine_Routine &routine, Coroutine_Routine *current,
                                          const void *current_stack_pointer);
    void SaveSharedStack(Coroutine_Routine &routine, const void *stack_pointer = nullptr);
    void EnqueueReady(const std::shared_ptr<Coroutine_Routine> &routine);
    std::shared_ptr<Coroutine_Routine> FindRoutine(Coroutine_Routine *routine) const;
    void YieldCurrent(CoroutineState next_state);
    void FinishRoutine(const std::shared_ptr<Coroutine_Routine> &routine);

    std::thread::id owner_thread_;
    CoroutineContext root_context_;
#if RPC_RUNTIME_HAS_ASAN
    void *root_asan_fake_stack_ = nullptr;
    const void *root_asan_stack_bottom_ = nullptr;
    std::size_t root_asan_stack_size_ = 0;
#endif
    std::deque<std::shared_ptr<Coroutine_Routine>> ready_queue_;
    std::unordered_map<std::uint64_t, std::shared_ptr<Coroutine_Routine>> routines_;
    std::vector<Coroutine_Routine *> call_stack_{nullptr};
    std::vector<SharedStackSlot> shared_stacks_;
    std::uint64_t next_id_ = 1;
    std::size_t next_shared_stack_ = 0;
    Coroutine_Routine *current_ = nullptr;
    std::unique_ptr<CoroutinePool> pool_;  // Optional routine pool.
};

} // namespace rpc::runtime
