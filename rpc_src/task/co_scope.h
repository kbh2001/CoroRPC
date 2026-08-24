#pragma once

#include "memory/task_pool.h"
#include "runtime/coroutine.h"
#include "task/task.h"

#include <exception>
#include <utility>

namespace rpc::transport {
class RpcClient;
}

namespace rpc::runtime {

// Scope for a batch of tasks submitted from a user thread.
//
// Join() blocks the user thread on a condition variable. Use CoroResult when
// waiting from inside a coroutine.
//
// Example:
//   CoScope scope(coroutine);
//   scope.Dispatch([&]{ /* runs on the coroutine thread */ });
//   scope.Join();
//
// With RpcClient:
//   auto& client = RpcClient::GetInstance();
//   CoScope scope(client);
//   crtco(&scope) { /* task */ };
//   scope.Join();
//
// With the global RpcClient singleton:
//   CoScope scope;
//   crtco(&scope) { /* task */ };
//   scope.Join();
//
// Reference-capturing tasks require Join() before captured stack variables go
// out of scope. The destructor joins automatically.
class CoScope {
public:
    CoScope();

    explicit CoScope(Coroutine &coroutine) : coroutine_(&coroutine) {}

    // Distribute tasks across the client's shards.
    explicit CoScope(transport::RpcClient &client) : client_(&client) {}

    ~CoScope() {
        if (!joined_) {
            Join();
        }
    }

    // Copying and moving are disabled because tasks reference gather_.
    CoScope(const CoScope &) = delete;
    CoScope(CoScope &&) = delete;
    CoScope &operator=(const CoScope &) = delete;
    CoScope &operator=(CoScope &&) = delete;

    // Submit a closure. Captures must fit the 128-byte Task storage.
    template <class F>
    void Dispatch(F &&f);

    // Block until all submitted tasks complete and rethrow the first exception.
    void Join();

private:
    Task<128> *AllocateTask();
    void SubmitTask(Task<128> *task);

    Coroutine *coroutine_ = nullptr;
    transport::RpcClient *client_ = nullptr;
    Gather gather_;
    bool joined_ = false;
};

template <class F>
void CoScope::Dispatch(F &&f) {
    if (joined_) {
        // A scope is reusable after the previous batch has completed. This is
        // the synchronous user-thread fast path: Dispatch one call, then Join.
        joined_ = false;
        gather_.first_exception = nullptr;
    }
    // Increment before submission so a fast task cannot decrement an empty count.
    gather_.remaining.fetch_add(1, std::memory_order_relaxed);

    Task<128> *task = AllocateTask();
    task->SetLambda(std::forward<F>(f));
    task->gather = &gather_;
    SubmitTask(task);
}

} // namespace rpc::runtime
