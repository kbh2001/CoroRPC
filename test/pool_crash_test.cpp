#include "runtime/coroutine_scheduler.h"

#include <cstdint>
#include <iostream>

namespace {

using rpc::runtime::CoroutineScheduler;
using rpc::runtime::CoroutineState;

bool TestFirstBatchAndReuse()
{
    CoroutineScheduler::Options options;
    options.shared_stack_count = 2;
    options.pool_max_size = 10;

    CoroutineScheduler scheduler(options);
    int completed = 0;
    for (int index = 0; index < 100; ++index)
    {
        scheduler.Spawn([&, index] {
            volatile int values[100]{};
            for (int item = 0; item < 100; ++item)
            {
                values[item] = index * 1000 + item;
            }
            for (int item = 0; item < 100; ++item)
            {
                if (values[item] != index * 1000 + item)
                {
                    return;
                }
            }
            ++completed;
        });

        if (index % 10 == 9)
        {
            scheduler.RunReady();
        }
    }
    return completed == 100 && !scheduler.HasReady();
}

bool TestRetainedHandleIsNotReused()
{
    CoroutineScheduler::Options options;
    options.pool_prewarm_count = 1;
    options.pool_max_size = 1;

    CoroutineScheduler scheduler(options);
    int completed = 0;

    auto retained = scheduler.Spawn([&] { ++completed; });
    scheduler.RunReady();
    const std::uint64_t retained_id = retained->id();

    auto second = scheduler.Spawn([&] { ++completed; });
    const bool distinct_while_retained = second->id() != retained_id;
    scheduler.RunReady();

    retained.reset();
    auto third = scheduler.Spawn([&] { ++completed; });
    const bool reused_after_release = third->id() == retained_id;
    scheduler.RunReady();

    return distinct_while_retained && reused_after_release && completed == 3 &&
           !second->has_exception() && !third->has_exception();
}

bool TestSharedStackSnapshotThenReuse()
{
    CoroutineScheduler::Options options;
    options.shared_stack_count = 1;
    options.shared_stack_size = 64 * 1024;
    options.pool_prewarm_count = 1;
    options.pool_max_size = 1;
    options.pool_stack_size = 64 * 1024;

    CoroutineScheduler scheduler(options);
    int completed = 0;

    auto first = scheduler.Spawn([&] {
        volatile std::uint64_t canary = 0x12345678U;
        CoroutineScheduler::SuspendCurrent(CoroutineState::WAIT_EVENT);
        if (canary == 0x12345678U)
        {
            ++completed;
        }
    }, 64 * 1024);
    const std::uint64_t pooled_id = first->id();
    scheduler.RunReady();
    if (first->state() != CoroutineState::WAIT_EVENT || !scheduler.MakeReady(first.get()))
    {
        return false;
    }
    scheduler.RunReady();
    if (first->has_exception())
    {
        return false;
    }
    first.reset();

    auto second = scheduler.Spawn([&] {
        volatile std::uint64_t canary = 0xabcdef01U;
        CoroutineScheduler::SuspendCurrent(CoroutineState::WAIT_EVENT);
        if (canary == 0xabcdef01U)
        {
            ++completed;
        }
    }, 64 * 1024);
    const bool reused = second->id() == pooled_id;
    scheduler.RunReady();
    if (second->state() != CoroutineState::WAIT_EVENT || !scheduler.MakeReady(second.get()))
    {
        return false;
    }
    scheduler.RunReady();

    return reused && completed == 2 && !second->has_exception();
}

} // namespace

int main()
{
    const bool first_batch = TestFirstBatchAndReuse();
    const bool retained_handle = TestRetainedHandleIsNotReused();
    const bool shared_stack = TestSharedStackSnapshotThenReuse();

    std::cout << "first batch and reuse: " << (first_batch ? "PASS" : "FAIL") << '\n';
    std::cout << "retained handle: " << (retained_handle ? "PASS" : "FAIL") << '\n';
    std::cout << "shared stack reuse: " << (shared_stack ? "PASS" : "FAIL") << '\n';
    return first_batch && retained_handle && shared_stack ? 0 : 1;
}
