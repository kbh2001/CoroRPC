#pragma once

#include <cstddef>

namespace rpc::runtime {

class CoroutineStack {
public:
    explicit CoroutineStack(std::size_t usable_size);
    ~CoroutineStack();

    CoroutineStack(const CoroutineStack &) = delete;
    CoroutineStack &operator=(const CoroutineStack &) = delete;
    CoroutineStack(CoroutineStack &&) = delete;
    CoroutineStack &operator=(CoroutineStack &&) = delete;

    void *stack_top() const noexcept;
    void *usable_bottom() const noexcept;
    std::size_t usable_size() const noexcept;

private:
    void *mapping_ = nullptr;
    std::size_t mapping_size_ = 0;
    std::size_t page_size_ = 0;
    std::size_t usable_size_ = 0;
};

} // namespace rpc::runtime
