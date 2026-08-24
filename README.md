# CoroRPC

CoroRPC 是一个基于 C++17 实现的轻量级 RPC 框架，面向 Linux x86-64 环境下的高并发、低延迟和 I/O 密集型请求。项目提供 Client、Server、TCP 长连接、多路复用、ZooKeeper 服务发现以及同步调用风格的协程接口。

## 我想说的话

对于我而言，RPC项目是一个同时可以进行性能优化，网络编程，以及涉及一些分布式内容的项目，设计的知识面比较广泛和全面。当前他可能并不不完善，但是我自认为进行了很多性能方面的优化，当前性能说的过去。协程方面是借鉴了腾讯的libco，没有采用 bthread 风格的跨线程协程调度，但是当然我们同样保留了用户线程的协程语义，同时通过后台协程线程充分利用了多核资源(这里的架构设计我认为还是有一定的小巧思)

总的来说，如果你是一个初学者，没有想要找一个RPC项目来看，我认为当前的项目比较合适：
1.他的架构相比于grpc,brpc这种工业级实现，他的架构较为简单
2.rpc应该有的功能，比如熔断器，幂等请求处理，都有所涉猎。
3.其中使用了大量的无锁编程，我认为有一些借鉴意义。
4.rpc项目会随着我的认识和学习深入不断更新，可以一起学习进步，可以随时找到我（O(∩_∩)O哈哈~）
5.如果您有任何宝贵意见和建议，欢迎提出(本人就是一个小菜鸡，轻点喷 ┭┮﹏┭┮)

最后，希望我们都能有所收获

## 主要功能

- C++17、Linux x86-64、epoll 和非阻塞 Socket；
- 非抢占式共享栈协程及同步调用接口；
- TCP 长连接和基于 `request_id` 的并发请求；
- 固定帧头、长度校验、半包和粘包处理；
- ZooKeeper 服务注册、发现和 watch 更新；
- 请求 deadline、取消、幂等请求合并、熔断、背压和优雅退出；
- Client/Server 示例程序，以及协议、协程池、TaskPool 和 RPC 压测程序。

## 环境要求

- Linux x86-64；
- GCC 或 Clang；
- C++17；
- CMake 3.16 或更高版本；
- ZooKeeper C client 多线程库；
- Java 11 或更高版本，用于运行项目附带的 ZooKeeper。

## 安装 ZooKeeper C client

Ubuntu 可以直接安装系统开发包：

```bash
sudo apt update
sudo apt install libzookeeper-mt-dev
```

也可以从 Apache ZooKeeper 源码单独编译并安装 C client：

```bash
cd apache-zookeeper-3.8.4/zookeeper-client/zookeeper-client-c
autoreconf -if
./configure --without-cppunit --prefix=/usr/local
make -j
sudo make install
sudo ldconfig
```

ZooKeeper C client 不再由 CoroRPC 的 CMake 自动编译，也不会链接仓库中的预编译 `.libs` 文件。

## 编译

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON
cmake --build build -j
```

如果 ZooKeeper C client 安装在非系统路径，通过 `RPC_ZOOKEEPER_ROOT` 指定安装前缀：

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON \
  -DRPC_ZOOKEEPER_ROOT=/path/to/zookeeper-prefix
cmake --build build -j
```

启用 AddressSanitizer 和 UndefinedBehaviorSanitizer：

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DRPC_ENABLE_SANITIZERS=ON \
  -DBUILD_TESTING=ON
cmake --build build-asan -j
```

## 启动 ZooKeeper Server

ZooKeeper Server 和前面安装的 ZooKeeper C client 是两个独立部分。C client 用于编译和运行 CoroRPC；Server 用于服务注册与发现。

方式一：使用项目附带的 ZooKeeper Server：

```bash
rpc_src/third_part/zookeeper/apache-zookeeper-3.8.4-bin/bin/zkServer.sh \
  start \
  rpc_src/third_part/zookeeper/apache-zookeeper-3.8.4-bin/conf/zoo.cfg
```

默认地址为 `127.0.0.1:2181`。

方式二：使用自行安装的 ZooKeeper Server 或已有 ZooKeeper 集群。此时不需要启动项目附带的 Server，只需把 `config/rpc_server.json` 中的 `zookeeper_hosts`，以及 Client 启动参数中的 ZooKeeper 地址改成实际地址。

## 启动 Server

```bash
./build/rpc_server config/rpc_server.json
```

Server 默认监听 `0.0.0.0:8989`，并向 ZooKeeper 注册 `EchoService.Echo`。该示例服务会把请求 body 原样返回。

可以通过传入其他配置文件修改监听地址、ZooKeeper 地址、线程数量和队列容量：

```bash
./build/rpc_server path/to/rpc_server.json
```

按 `Ctrl-C` 可触发优雅退出。

## 运行 Client

```bash
./build/rpc_client [zookeeper_hosts] [service_root] [call_count] [io_threads]
```

例如：

```bash
./build/rpc_client 127.0.0.1:2181 /rpc 32 1
```

参数依次表示 ZooKeeper 地址、服务根路径、请求数量和 Client I/O 线程数。Client 会调用 `EchoService.Echo` 并输出成功和失败数量。

## 测试

快速协议测试：

```bash
ctest --test-dir build --output-on-failure
```

当前测试程序位于 `test/`：

```bash
./build/protocol_fast_path_test
./build/coroutine_pool_test
./build/pool_crash_test
./build/task_pool_hazard_stress_test
```

其中 `task_pool_hazard_stress_test` 是并发压力测试，运行时间可能比协议测试更长，不默认加入 CTest。

## RPC 压测

先启动 ZooKeeper 和 `rpc_server`，再执行：

```bash
./build/brpc_style_test \
  [duration_seconds] \
  [discovery_wait_seconds] \
  [client_io_threads] \
  [measure_latency] \
  [payload_bytes]
```

512B 请求、50 个同步用户线程、8 个 Client shard、运行 6 秒的示例(brpc同款)：

```bash
./build/brpc_style_test 6 1 8 1 512
```

参数说明：

1. 测试时长，单位为秒；
2. 服务发现等待时间，单位为秒；
3. Client I/O 线程数；
4. 是否采集延迟，`0` 表示关闭，`1` 表示开启；
5. 请求 body 大小，单位为字节。

压测输出包括成功数、失败数、平均 QPS、峰值 QPS，以及开启延迟采集后的 P50、P95、P99 和最大延迟。

WSL 环境会受到调度和 CPU 资源波动影响，性能数据应结合测试环境、payload、线程数和测试时长一起记录。

## 目录说明

```text
rpc_src/       RPC 核心实现
test/          功能测试和性能测试
config/        Server 配置示例
CMakeLists.txt 构建入口
README.md      使用说明
```
