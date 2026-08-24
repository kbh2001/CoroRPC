#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace rpc::runtime {

// Linux x86-64 SysV context saved by coroutine_context_switch_x86_64.S.
struct CoroutineContext {
    std::uint64_t rbx = 0;
    std::uint64_t rbp = 0;
    std::uint64_t r12 = 0;
    std::uint64_t r13 = 0;
    std::uint64_t r14 = 0;
    std::uint64_t r15 = 0;
    std::uint64_t rsp = 0;
    std::uint64_t rip = 0;
};

static_assert(std::is_standard_layout<CoroutineContext>::value, "assembly requires a stable context layout");
static_assert(offsetof(CoroutineContext, rbx) == 0, "assembly offset mismatch: rbx");
static_assert(offsetof(CoroutineContext, rbp) == 8, "assembly offset mismatch: rbp");
static_assert(offsetof(CoroutineContext, r12) == 16, "assembly offset mismatch: r12");
static_assert(offsetof(CoroutineContext, r13) == 24, "assembly offset mismatch: r13");
static_assert(offsetof(CoroutineContext, r14) == 32, "assembly offset mismatch: r14");
static_assert(offsetof(CoroutineContext, r15) == 40, "assembly offset mismatch: r15");
static_assert(offsetof(CoroutineContext, rsp) == 48, "assembly offset mismatch: rsp");
static_assert(offsetof(CoroutineContext, rip) == 56, "assembly offset mismatch: rip");
static_assert(sizeof(CoroutineContext) == 64, "assembly offsets must match CoroutineContext");

extern "C" void coroutine_context_switch(CoroutineContext *from, CoroutineContext *to, void *restore_begin,
                                           const void *restore_data, std::size_t restore_size) noexcept;
extern "C" const void *coroutine_stack_pointer() noexcept;

} // namespace rpc::runtime
