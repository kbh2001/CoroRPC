#include "task/co_scope.h"
#include "transport/rpc_client.h"
#include "task/task_complete.h"
#include <stdexcept>

namespace rpc::runtime {

CoScope::CoScope() : client_(&transport::RpcClient::GetInstance()) {}

Task<128> *CoScope::AllocateTask()
{
    return client_ != nullptr ? client_->AllocateTask()
                              : rpc::memory::GlobalTaskPool<128>().Allocate();
}

void CoScope::SubmitTask(Task<128> *task)
{
    if (client_ != nullptr)
    {
        client_->SubmitTask(task);
        return;
    }
    if (coroutine_ == nullptr || !coroutine_->Submit([task] {
            try
            {
                task->Run();
            }
            catch (...)
            {
                task->exception = std::current_exception();
            }
            rpc::runtime::CompleteTask(task, rpc::memory::GlobalTaskPool<128>());
        }))
    {
        task->exception = std::make_exception_ptr(std::runtime_error("CoScope: coroutine rejected task"));
        rpc::runtime::CompleteTask(task, rpc::memory::GlobalTaskPool<128>());
    }
}

void CoScope::Join() {
    if (joined_) {
        return;
    }
    joined_ = true;

    std::unique_lock<std::mutex> lock(gather_.mutex);
    gather_.cv.wait(lock, [this] {
        return gather_.remaining.load(std::memory_order_acquire) == 0;
    });

    if (gather_.first_exception) {
        std::exception_ptr exception = std::move(gather_.first_exception);
        std::rethrow_exception(exception);
    }
}

} // namespace rpc::runtime
