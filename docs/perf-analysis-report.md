# KamaCache-CPP perf 性能分析报告

> 使用 `ghz`、`bench_cache`、`perf record` 和火焰图对 KamaCache-CPP 的 gRPC 全栈路径与核心缓存命中路径进行分层分析。

## 1. 结论摘要

本次测试显示，KamaCache-CPP 的性能瓶颈具有明显分层特征：

| 层级 | 测试方式 | 主要观察 | 结论 |
|------|----------|----------|------|
| gRPC 全栈 | `ghz -> node_server -> KCacheGroup::Get()` | 服务端 CPU 主要消耗在 `futex_wake`、TCP send/recv 和 gRPC 同步服务路径 | 当前端到端 Get 命中压测下，KCache 业务逻辑不是主要热点 |
| KCache 裸调 | `bench_cache -> KCacheGroup::Get()` | `KCacheGroup::Get` 占 99%，`__pthread_mutex_unlock -> futex_wake` 占主导 | 绕过 gRPC 后，LRU 独占锁成为核心命中路径的主要瓶颈 |

因此，不应简单地说“瓶颈就是 gRPC”或“瓶颈就是 LRU”。更准确的表述是：

> 在当前 gRPC 全栈压测下，主要 CPU 时间被 gRPC 同步服务模型、线程唤醒和 TCP 路径覆盖；当绕过 gRPC 后，KCache 命中路径中的 LRU 单把互斥锁成为主要热点。

---

## 2. 测试环境

| 项目 | 实际值 |
|------|--------|
| OS | Ubuntu 20.04.6 LTS |
| Kernel | Linux 5.15.0-139-generic |
| GCC | 11.5.0 |
| CMake | 3.29.5 |
| perf | 5.15.178 |
| ghz | v0.117.0 |
| 构建类型 | `conan-release` + `-g -fno-omit-frame-pointer` |
| `node_server` | `with debug_info, not stripped` |
| `bench_cache` | `with debug_info, not stripped` |
| `perf_event_paranoid` | 1 |
| `kptr_restrict` | 0 |

虚拟机环境可能未暴露完整 PMU，因此本次采样使用软件事件：

```bash
-e cpu-clock
```

---

## 3. 方法说明

### 3.1 关键原则

`perf record` 必须和压测窗口重叠。

`perf record -p <PID> -- sleep 30` 只会采集这 30 秒内目标进程正在消耗 CPU 的调用栈。如果采样期间没有请求流量，采到的是空闲路径；如果服务尚未完成缓存组创建，采到的可能是 `Group not found` 或首次回源路径。

### 3.2 编译

```bash
cmake --preset conan-release -DCMAKE_CXX_FLAGS="-g -fno-omit-frame-pointer"
cmake --build --preset conan-release -j$(nproc)
```

验证：

```bash
file ./bin/node_server
file ./bin/bench_cache
```

本次两个二进制均为 `with debug_info, not stripped`。

---

## 4. 场景 A：gRPC 全栈 Get 命中路径

### 4.1 测试目标

测试路径：

```text
ghz
  -> gRPC / HTTP2 / protobuf
  -> KCacheServer::Get
  -> GetCacheGroup
  -> KCacheGroup::Get
  -> LRUCache::Get
  -> protobuf response
```

### 4.2 预热验证

服务启动后等待至少 6 秒，因为当前 `src/main.cpp` 会先启动 gRPC 服务线程，再 `sleep 5s` 后创建缓存组。

```bash
ghz --insecure \
  --proto ./src/proto/kcache.proto \
  --call kcache.pb.KCache/Get \
  --data '{"group":"default","key":"Tom"}' \
  --total=1 \
  localhost:8001
```

结果：

```text
Status code distribution:
  [OK]   1 responses
```

这说明本次压测不是 `Group not found` 路径，`default/Tom` 可访问。

### 4.3 perf 采样

```bash
PID=$(pgrep -f "node_server --port=8001")

perf record \
  -e cpu-clock \
  -F 99 \
  --call-graph fp \
  -o perf_grpc_hit.data \
  -p "$PID" \
  -- sleep 30
```

采样期间同步启动 `ghz`：

```bash
ghz --insecure \
  --proto ./src/proto/kcache.proto \
  --call kcache.pb.KCache/Get \
  --data '{"group":"default","key":"Tom"}' \
  --connections=4 \
  --concurrency=128 \
  --duration=30s \
  --skipFirst=1000 \
  localhost:8001
```

### 4.4 ghz 结果

| 指标 | 值 |
|------|----|
| Count | 461,341 |
| Total | 30.00 s |
| Requests/sec | 15,375.55 |
| Average | 5.87 ms |
| Fastest | 0.12 ms |
| Slowest | 57.17 ms |
| p50 | 5.23 ms |
| p90 | 10.37 ms |
| p95 | 12.64 ms |
| p99 | 18.33 ms |
| OK | 461,238 |
| Unavailable | 103 |

错误率约为 `0.022%`。错误内容为 `use of closed network connection`，比例很低，但最终结论应注明本次压测不是 100% 成功。

### 4.5 perf 总体热点

`perf record` 结果：

```text
Captured and wrote 1.928 MB perf_grpc_hit.data (1283 samples)
Total Lost Samples: 0
Samples: 1K of event 'cpu-clock'
```

总体热点：

| Children | Symbol / Path | 说明 |
|----------|---------------|------|
| 69.13% | `entry_SYSCALL_64_after_hwframe` | 系统调用入口 |
| 53.39% | `__x64_sys_futex` | futex 系统调用 |
| 51.36% | `futex_wake` | 线程唤醒 |
| 1.79% | `futex_wait` | 线程等待 |
| 8.89% | `__x64_sys_sendmsg` | TCP 发送 |
| 3.51% | `__x64_sys_recvmsg` | TCP 接收 |

gRPC 用户态热点分散在 `ServerCallData`、`SyncRequest`、`FilterStackCall`、`combiner` 等路径上，单个函数占比不高。

KCache 相关符号：

| Children | Self | Symbol |
|----------|------|--------|
| 0.39% | 0.08% | `kcache::KCacheServer::Get` |
| 0.16% | 0.16% | `kcache::GetCacheGroup` |
| 0.16% | 0.00% | `kcache::KCacheGroup::Get` |
| 未形成明显热点 | 未形成明显热点 | `kcache::LRUCache::Get` |

### 4.6 场景 A 结论

gRPC 全栈 Get 命中路径下，KCache 业务代码占比很低。服务端 CPU 时间主要消耗在：

- gRPC 同步服务线程调度 / completion 相关唤醒。
- `futex_wake` 系统调用路径。
- TCP send/recv 网络栈。
- gRPC HTTP/2 / protobuf / filter / combiner 用户态路径。

这里的 `futex_wake` 不应归因于 LRU 锁竞争。证据是：

- `futex_wake` 高，但 `futex_wait` 仅 1.79%。
- `KCacheGroup::Get` 只有 0.16%，`LRUCache::Get` 不明显。
- 可见的 gRPC 同步服务路径包括 `grpc::Server::SyncRequest`、`ServerCallData`、`WakeInsideCombiner` 等。

---

## 5. 场景 B：KCache 裸调用命中路径

### 5.1 测试目标

使用 `bench_cache` 直接调用 `KCacheGroup::Get()`，绕过 gRPC、protobuf、HTTP/2 和 TCP。

当前 `bench_cache` 的负载模型：

- 创建一个缓存组。
- 预填充 100 个 key。
- 多线程轮转访问这 100 个 key。
- 压测路径为缓存命中路径。

因此它代表“核心缓存命中路径”的性能上限参考，不是 gRPC 场景的一比一替代。

### 5.2 QPS 结果

| 场景 | QPS | 说明 |
|------|-----|------|
| gRPC 全栈，128 并发 | 15,375.55 | `ghz` 端到端 Get |
| KCache 裸调，1 线程 | 5,038,571 | 单线程核心命中路径 |
| KCache 裸调，4 线程 | 2,234,932 | 4 线程核心命中路径，带 perf |
| KCache 裸调，128 线程 | 1,438,578 | 高并发核心命中路径 |

裸调吞吐显著高于 gRPC 全栈，但二者负载模型不同，不能直接把倍率解释为“gRPC 单独消耗了这些性能”。更准确的解释是：绕过 RPC 框架、协议栈、网络和客户端压测工具后，核心缓存命中路径具备百万级 QPS 能力。

### 5.3 4 线程 perf 结果

采样命令：

```bash
perf record \
  -e cpu-clock \
  -F 99 \
  --call-graph fp \
  -o perf_bench_4t.data \
  -- ./bin/bench_cache --threads=4 --duration_sec=10
```

采样结果：

```text
Captured and wrote 0.368 MB perf_bench_4t.data (2235 samples)
Total Lost Samples: 0
Samples: 2K of event 'cpu-clock'
```

热点：

| Children | Self | Symbol / Path | 说明 |
|----------|------|---------------|------|
| 99.02% | 0.13% | `kcache::KCacheGroup::Get` | 裸调主路径 |
| 84.74% | 0.04% | `__pthread_mutex_unlock` | mutex unlock 触发 futex wake |
| 84.61% | 0.09% | `futex_wake` | 唤醒等待线程 |
| 12.57% | 0.63% | `__lll_lock_wait` | mutex lock 慢路径 |
| 11.54% | 0.18% | `futex_wait` | 线程等待锁 |
| 1.16% | 0.49% | `kcache::LRUCache::Get` | LRU 逻辑本身 |

关键调用链：

```text
kcache::KCacheGroup::Get
  -> kcache::LRUCache::Get
     -> std::mutex / pthread mutex
        -> __pthread_mutex_unlock
           -> __x64_sys_futex
              -> futex_wake

kcache::KCacheGroup::Get
  -> __lll_lock_wait
     -> __x64_sys_futex
        -> futex_wait
```

### 5.4 场景 B 结论

绕过 gRPC 后，热点明确落在 KCache 命中路径，且主要成本不是哈希查找或链表移动，而是 LRU 独占锁在多线程访问下的唤醒和等待成本。

4 线程下 `futex_wait` 已达 11.54%，128 线程 QPS 进一步下降到 143.86 万，说明单把 LRU mutex 会限制多线程扩展性。

---

## 6. `futex_wake` 的两种来源

本次测试中，场景 A 和场景 B 都出现了 `futex_wake`，但含义不同。

| 场景 | `futex_wake` 来源 | 解释 |
|------|------------------|------|
| gRPC 全栈 | gRPC 同步服务线程、completion、combiner、网络事件唤醒 | 框架和系统调用开销主导，KCache 代码不可见 |
| KCache 裸调 | `KCacheGroup::Get -> LRUCache::Get -> std::mutex` | LRU 独占锁竞争和线程唤醒成本主导 |

判断 `futex_wake` 不能只看函数名，必须看调用栈来源。

---

## 7. 优化方向

| 优先级 | 方向 | 依据 | 风险 / 备注 |
|--------|------|------|-------------|
| P0 | 先明确目标场景：端到端 RPC QPS 还是核心缓存命中 QPS | 两个场景的瓶颈不同 | 不同目标对应不同优化路线 |
| P1 | gRPC 参数和线程模型调优 | 场景 A 热点集中在 gRPC/系统调用路径 | 需要比较 sync server、callback API、CQ 数、线程数 |
| P2 | LRU 分片 | 场景 B 中 mutex/futex 主导 | 对核心命中路径收益明显，对当前 gRPC 全栈收益可能不明显 |
| P3 | 减少 `LRUCache::Get` 内部写操作和复制 | 当前 `Get` 会移动链表节点并复制 `ByteView` | 需要保持 LRU 语义，不能只换 shared lock |
| P4 | 做并发梯度和 CPU 使用率观测 | 当前跳过了并发梯度 | 有助于判断服务端是否打满 CPU |

建议下一步如果优化核心缓存路径，优先尝试 Sharded LRU：

- 按 key hash 分到 N 个 shard。
- 每个 shard 一个独立 LRU 和 mutex。
- 理论上热点 key 分散时锁竞争降低为约 `1/N`。
- 但如果压测始终访问单 key，分片收益有限，因为单 key 仍落在同一 shard。

---

## 8. 复现命令速查

### gRPC 全栈

```bash
./bin/node_server --port=8001 --node=A --log_level=warn
```

```bash
ghz --insecure \
  --proto ./src/proto/kcache.proto \
  --call kcache.pb.KCache/Get \
  --data '{"group":"default","key":"Tom"}' \
  --total=1 \
  localhost:8001
```

```bash
PID=$(pgrep -f "node_server --port=8001")
perf record -e cpu-clock -F 99 --call-graph fp \
  -o perf_grpc_hit.data \
  -p "$PID" \
  -- sleep 30
```

```bash
ghz --insecure \
  --proto ./src/proto/kcache.proto \
  --call kcache.pb.KCache/Get \
  --data '{"group":"default","key":"Tom"}' \
  --connections=4 \
  --concurrency=128 \
  --duration=30s \
  --skipFirst=1000 \
  localhost:8001
```

### KCache 裸调

```bash
./bin/bench_cache --threads=1 --duration_sec=10
```

```bash
perf record -e cpu-clock -F 99 --call-graph fp \
  -o perf_bench_4t.data \
  -- ./bin/bench_cache --threads=4 --duration_sec=10
```

```bash
./bin/bench_cache --threads=128 --duration_sec=10
```

### 查看报告

```bash
perf report -i perf_grpc_hit.data --stdio --children --sort=overhead,symbol
perf report -i perf_bench_4t.data --stdio --children --sort=overhead,symbol
```

### 火焰图

```bash
perf script -i perf_grpc_hit.data > out_grpc_hit.perf
~/FlameGraph/stackcollapse-perf.pl out_grpc_hit.perf > out_grpc_hit.folded
~/FlameGraph/flamegraph.pl out_grpc_hit.folded > flamegraph_grpc_hit.svg
```

---

## 9. 注意事项

- `perf` 百分比表示采样 CPU 时间比例，不能直接等价为 QPS 损耗比例。
- gRPC 全栈和 `bench_cache` 负载模型不同，QPS 倍率只能作为分层参考，不能直接归因到单个组件。
- 本次 gRPC 压测存在 `0.022%` `Unavailable`，应在报告中注明。
- `bench_cache` 当前访问 100 个预热 key；如果改成单 key 或更大 value，结果会变化。
- 128 线程可能远超虚拟机 vCPU 数，更多体现锁竞争和调度压力。
