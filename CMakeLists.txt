cmake_minimum_required(VERSION 3.16)
project(rpc LANGUAGES CXX ASM)

include(CTest)
find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
option(RPC_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

set(RPC_SOURCES
    rpc_src/discovery/zookeeper_service_discovery.cpp
    rpc_src/discovery/zookeeper_service_registry.cpp
    rpc_src/runtime/async_socket.cpp
    rpc_src/runtime/coroutine_pool.cpp
    rpc_src/runtime/coroutine_routine.cpp
    rpc_src/runtime/coroutine_runtime.cpp
    rpc_src/runtime/coroutine_context_x86_64.S
    rpc_src/runtime/coroutine_scheduler.cpp
    rpc_src/runtime/io_manager.cpp
    rpc_src/runtime/stack_allocator.cpp
    rpc_src/runtime/timer_wheel.cpp
    rpc_src/task/co_scope.cpp
    rpc_src/protocol/frame.cpp
    rpc_src/protocol/rpc_metadata.cpp
    rpc_src/transport/multiplexed_connection.cpp
    rpc_src/transport/rpc_client.cpp
    rpc_src/transport/rpc_server.cpp
    rpc_src/transport/service_dispatcher.cpp
)

add_library(rpc_core STATIC ${RPC_SOURCES})
set(RPC_ZOOKEEPER_ROOT "" CACHE PATH "ZooKeeper C client installation prefix")
find_path(RPC_ZOOKEEPER_INCLUDE_DIR
    NAMES zookeeper.h
    HINTS ${RPC_ZOOKEEPER_ROOT}
    PATH_SUFFIXES include/zookeeper include
)
find_path(RPC_ZOOKEEPER_JUTE_INCLUDE_DIR
    NAMES zookeeper.jute.h
    HINTS ${RPC_ZOOKEEPER_ROOT}
    PATH_SUFFIXES include/zookeeper include generated
)
find_library(RPC_ZOOKEEPER_LIBRARY
    NAMES zookeeper_mt
    HINTS ${RPC_ZOOKEEPER_ROOT}
    PATH_SUFFIXES lib lib64
)

if(NOT RPC_ZOOKEEPER_INCLUDE_DIR OR NOT RPC_ZOOKEEPER_JUTE_INCLUDE_DIR OR
   NOT RPC_ZOOKEEPER_LIBRARY)
    message(FATAL_ERROR
        "ZooKeeper C client was not found. Install libzookeeper-mt-dev or build "
        "zookeeper-client-c separately, then set RPC_ZOOKEEPER_ROOT to its "
        "installation prefix.")
endif()

add_library(rpc_zookeeper_mt UNKNOWN IMPORTED GLOBAL)
set_target_properties(rpc_zookeeper_mt PROPERTIES
    IMPORTED_LOCATION ${RPC_ZOOKEEPER_LIBRARY}
    INTERFACE_INCLUDE_DIRECTORIES "${RPC_ZOOKEEPER_INCLUDE_DIR};${RPC_ZOOKEEPER_JUTE_INCLUDE_DIR}"
)
target_include_directories(rpc_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/rpc_src)
target_link_libraries(rpc_core PUBLIC Threads::Threads rpc_zookeeper_mt)
target_compile_definitions(rpc_core PUBLIC THREADED)
target_compile_options(rpc_core PRIVATE -Wall -Wextra -Wpedantic)

if(RPC_ENABLE_SANITIZERS)
    target_compile_options(rpc_core PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(rpc_core PUBLIC -fsanitize=address,undefined)
endif()

add_executable(rpc_server rpc_src/core/server_main.cpp)
target_link_libraries(rpc_server PRIVATE rpc_core)

add_executable(rpc_client rpc_src/core/client_main.cpp)
target_link_libraries(rpc_client PRIVATE rpc_core)

# Benchmarks
add_executable(brpc_style_test test/brpc_style_test.cpp)
target_link_libraries(brpc_style_test PRIVATE rpc_core)

add_executable(async_benchmark test/async_benchmark.cpp)
target_link_libraries(async_benchmark PRIVATE rpc_core)

add_executable(coroutine_pool_test test/coroutine_pool_test.cpp)
target_link_libraries(coroutine_pool_test PRIVATE rpc_core)

add_executable(pool_crash_test test/pool_crash_test.cpp)
target_link_libraries(pool_crash_test PRIVATE rpc_core)

add_executable(protocol_fast_path_test test/protocol_fast_path_test.cpp)
target_link_libraries(protocol_fast_path_test PRIVATE rpc_core)

add_executable(task_pool_hazard_stress_test test/task_pool_hazard_stress_test.cpp)
target_link_libraries(task_pool_hazard_stress_test PRIVATE rpc_core)

if(BUILD_TESTING)
    add_test(NAME protocol_fast_path_test COMMAND protocol_fast_path_test)
endif()
