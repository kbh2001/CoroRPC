#include "runtime/stack_allocator.h"

#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>
#include <system_error>

namespace rpc::runtime {

CoroutineStack::CoroutineStack(std::size_t usable_size)
{
    const long system_page_size = sysconf(_SC_PAGESIZE);
    if (system_page_size <= 0)
    {
        throw std::runtime_error("unable to determine system page size");
    }
    page_size_ = static_cast<std::size_t>(system_page_size);
    usable_size_ = ((usable_size + page_size_ - 1) / page_size_) * page_size_;
    if (usable_size_ == 0)
    {
        throw std::invalid_argument("coroutine stack size must be non-zero");
    }

    mapping_size_ = page_size_ + usable_size_;
    mapping_ = mmap(nullptr, mapping_size_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapping_ == MAP_FAILED)
    {
        mapping_ = nullptr;
        throw std::system_error(errno, std::generic_category(), "mmap coroutine stack");
    }
    if (mprotect(mapping_, page_size_, PROT_NONE) != 0)
    {
        const int error = errno;
        munmap(mapping_, mapping_size_);
        mapping_ = nullptr;
        throw std::system_error(error, std::generic_category(), "mprotect coroutine guard page");
    }
}

CoroutineStack::~CoroutineStack()
{
    if (mapping_ != nullptr)
    {
        munmap(mapping_, mapping_size_);
    }
}

void *CoroutineStack::stack_top() const noexcept
{
    return static_cast<char *>(mapping_) + mapping_size_;
}

void *CoroutineStack::usable_bottom() const noexcept
{
    return static_cast<char *>(mapping_) + page_size_;
}

std::size_t CoroutineStack::usable_size() const noexcept
{
    return usable_size_;
}

} // namespace rpc::runtime
