#pragma once

// Keep the coroutine object layout and stack-switch instrumentation in sync
// for every translation unit that includes runtime headers.
#ifndef RPC_RUNTIME_HAS_ASAN
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define RPC_RUNTIME_HAS_ASAN 1
#endif
#endif

#if defined(__SANITIZE_ADDRESS__)
#define RPC_RUNTIME_HAS_ASAN 1
#endif

#ifndef RPC_RUNTIME_HAS_ASAN
#define RPC_RUNTIME_HAS_ASAN 0
#endif
#endif
