#pragma once

#include <cstddef>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "runtime/coroutine.h"

namespace rpc::runtime {

template <typename T>
class CoroResult : public std::enable_shared_from_this<CoroResult<T>> {
    struct PrivateTag {};

public:
    CoroResult(PrivateTag, std::size_t count);

    CoroResult(const CoroResult &) = delete;
    CoroResult &operator=(const CoroResult &) = delete;

    static std::shared_ptr<CoroResult> Create(std::size_t count)
    {
        return std::make_shared<CoroResult>(PrivateTag{}, count);
    }

    void Set(std::size_t index, T value);

    void SetException(std::size_t index, std::exception_ptr error);

    template <class F>
    void GoInto(Coroutine &runtime, std::size_t index, F &&fn);

    void Wait();

    bool Ready() const noexcept { return remaining_ == 0; }
    std::size_t remaining() const noexcept { return remaining_; }
    std::size_t size() const noexcept { return results_.size(); }

    const std::vector<std::optional<T>> &results() const noexcept { return results_; }

    T Take(std::size_t index);

private:
    void CheckSetPrecondition(std::size_t index) const;
    void FinishSlot();

    Coroutine *runtime_ = nullptr;
    Coroutine::RoutineHandle parent_;
    std::size_t remaining_;
    bool waiting_ = false;
    std::vector<std::optional<T>> results_;
    std::exception_ptr first_error_;
};

template <>
class CoroResult<void> : public std::enable_shared_from_this<CoroResult<void>> {
    struct PrivateTag {};

public:
    CoroResult(PrivateTag, std::size_t count);

    CoroResult(const CoroResult &) = delete;
    CoroResult &operator=(const CoroResult &) = delete;

    static std::shared_ptr<CoroResult> Create(std::size_t count)
    {
        return std::make_shared<CoroResult>(PrivateTag{}, count);
    }

    void Done();
    void SetException(std::exception_ptr error);

    template <class F>
    void GoInto(Coroutine &runtime, F &&fn);

    void Wait();

    bool Ready() const noexcept { return remaining_ == 0; }
    std::size_t remaining() const noexcept { return remaining_; }

private:
    void FinishSlot();

    Coroutine *runtime_ = nullptr;
    Coroutine::RoutineHandle parent_;
    std::size_t remaining_;
    bool waiting_ = false;
    std::exception_ptr first_error_;
};

template <typename T>
CoroResult<T>::CoroResult(PrivateTag, std::size_t count) : remaining_(count), results_(count)
{
    runtime_ = Coroutine::CurrentRuntime();
    if (runtime_ == nullptr || !runtime_->Current())
    {
        throw std::logic_error("CoroResult must be created inside a running coroutine");
    }
    parent_ = runtime_->Current();
}

template <typename T>
void CoroResult<T>::CheckSetPrecondition(std::size_t index) const
{
    if (Coroutine::CurrentRuntime() != runtime_)
    {
        throw std::logic_error("CoroResult::Set must run on the runtime that created it");
    }
    if (index >= results_.size())
    {
        throw std::out_of_range("CoroResult::Set index out of range");
    }
    if (remaining_ == 0)
    {
        throw std::logic_error("CoroResult::Set called after all slots were filled");
    }
}

template <typename T>
void CoroResult<T>::Set(std::size_t index, T value)
{
    CheckSetPrecondition(index);
    if (results_[index].has_value())
    {
        throw std::logic_error("CoroResult::Set called twice for one slot");
    }
    results_[index].emplace(std::move(value));
    FinishSlot();
}

template <typename T>
void CoroResult<T>::SetException(std::size_t index, std::exception_ptr error)
{
    CheckSetPrecondition(index);
    if (first_error_ == nullptr)
    {
        first_error_ = error;
    }
    FinishSlot();
}

template <typename T>
void CoroResult<T>::FinishSlot()
{
    --remaining_;
    if (remaining_ != 0 || !waiting_)
    {
        return;
    }

    auto self = this->shared_from_this();
    Coroutine::RoutineHandle parent = parent_;
    Coroutine *runtime = runtime_;
    runtime->Resume(std::move(parent));
}

template <typename T>
template <class F>
void CoroResult<T>::GoInto(Coroutine &runtime, std::size_t index, F &&fn)
{
    if (&runtime != runtime_)
    {
        throw std::logic_error("CoroResult::GoInto requires the runtime that created it");
    }
    auto self = this->shared_from_this();
    runtime.Go([self, index, fn = std::forward<F>(fn)]() mutable {
        try
        {
            self->Set(index, fn());
        }
        catch (...)
        {

            try
            {
                self->SetException(index, std::current_exception());
            }
            catch (...)
            {
            }
        }
    });
}

template <typename T>
void CoroResult<T>::Wait()
{
    if (Coroutine::CurrentRuntime() != runtime_ || runtime_->Current() != parent_)
    {
        throw std::logic_error("CoroResult::Wait must be called by the coroutine that created it");
    }
    while (remaining_ != 0)
    {
        waiting_ = true;
        runtime_->Yield();
    }
    waiting_ = false;
    if (first_error_ != nullptr)
    {
        std::rethrow_exception(first_error_);
    }
}

template <typename T>
T CoroResult<T>::Take(std::size_t index)
{
    if (index >= results_.size() || !results_[index].has_value())
    {
        throw std::logic_error("CoroResult::Take on an unfilled slot");
    }
    T value = std::move(*results_[index]);
    results_[index].reset();
    return value;
}

inline CoroResult<void>::CoroResult(PrivateTag, std::size_t count) : remaining_(count)
{
    runtime_ = Coroutine::CurrentRuntime();
    if (runtime_ == nullptr || !runtime_->Current())
    {
        throw std::logic_error("CoroResult must be created inside a running coroutine");
    }
    parent_ = runtime_->Current();
}

inline void CoroResult<void>::Done()
{
    if (Coroutine::CurrentRuntime() != runtime_)
    {
        throw std::logic_error("CoroResult::Done must run on the runtime that created it");
    }
    if (remaining_ == 0)
    {
        throw std::logic_error("CoroResult::Done called after all slots were filled");
    }
    FinishSlot();
}

inline void CoroResult<void>::SetException(std::exception_ptr error)
{
    if (Coroutine::CurrentRuntime() != runtime_)
    {
        throw std::logic_error("CoroResult::SetException must run on the runtime that created it");
    }
    if (remaining_ == 0)
    {
        throw std::logic_error("CoroResult::SetException called after all slots were filled");
    }
    if (first_error_ == nullptr)
    {
        first_error_ = error;
    }
    FinishSlot();
}

inline void CoroResult<void>::FinishSlot()
{
    --remaining_;
    if (remaining_ != 0 || !waiting_)
    {
        return;
    }
    auto self = this->shared_from_this();
    Coroutine::RoutineHandle parent = parent_;
    Coroutine *runtime = runtime_;
    runtime->Resume(std::move(parent));
}

template <class F>
void CoroResult<void>::GoInto(Coroutine &runtime, F &&fn)
{
    if (&runtime != runtime_)
    {
        throw std::logic_error("CoroResult::GoInto requires the runtime that created it");
    }
    auto self = this->shared_from_this();
    runtime.Go([self, fn = std::forward<F>(fn)]() mutable {
        try
        {
            fn();
            self->Done();
        }
        catch (...)
        {
            try
            {
                self->SetException(std::current_exception());
            }
            catch (...)
            {
            }
        }
    });
}

inline void CoroResult<void>::Wait()
{
    if (Coroutine::CurrentRuntime() != runtime_ || runtime_->Current() != parent_)
    {
        throw std::logic_error("CoroResult::Wait must be called by the coroutine that created it");
    }
    while (remaining_ != 0)
    {
        waiting_ = true;
        runtime_->Yield();
    }
    waiting_ = false;
    if (first_error_ != nullptr)
    {
        std::rethrow_exception(first_error_);
    }
}

} // namespace rpc::runtime
