# KCache

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/kerolt/kcache)

KCache 是一个类 Memcached 的分布式缓存系统，采用 C/S 架构，基于一致性哈希实现数据分片，使用 LRU 淘汰算法。节点间通过 gRPC 通信，基于 etcd 实现服务注册与发现。

## 特性

- **分布式路由** — 一致性哈希 + 虚拟节点 + 自适应负载均衡，节点增减时最小化数据迁移
- **防缓存击穿** — SingleFlight 合并并发请求，同一 key 只回源一次
- **熔断降级** — 三态熔断器 (Closed→Open→HalfOpen)，回源失败时自动走 Fallback 或返回过期缓存
- **TTL 过期** — 支持写入时指定过期时间，Get 时惰性删除
- **可观测性** — Prometheus 指标端点，暴露命中率、熔断状态、加载耗时等 15 项指标
- **资源保护** — gRPC 超时控制、连接池复用、KeepAlive 探活、并发流限流
- **强一致性** — Set 时广播 Invalidate 到其他节点，Delete 时全节点广播

## 运行环境

- Ubuntu 22.04 (Docker)
- GCC 11.4（C++17）
- CMake 3.22+
- Conan 2.x

## 项目依赖

| 库 | 版本 | 用途 |
|---|---|---|
| gflags | 2.2.2 | 命令行参数 |
| gtest | 1.16.0 | 单元测试 |
| protobuf | 3.21.12 | 序列化 |
| grpc | 1.54.3 | RPC 通信 |
| etcd-cpp-apiv3 | 0.15.4 | etcd 客户端 |
| fmt | 11.1.3 | 字符串格式化 |
| spdlog | 1.15.1 | 日志 |
| cpp-httplib | 0.20.1 | HTTP 服务 |
| nlohmann_json | 3.12.0 | JSON 解析 |

> 使用 Conan 作为包管理器。注意 `etcd-cpp-apiv3` 依赖 `libsystemd/255`，该版本在高版本内核 (>6.8) 有 bug，推荐在 Docker 或内核 ≤6.8 的环境构建。

## 架构

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  客户端1   │     │  客户端2   │     │ HTTP 网关 │
│ (SDK)    │     │ (SDK)    │     │ (SDK)    │
└────┬─────┘     └────┬─────┘     └────┬─────┘
     │                 │                 │
     └─────────────────┼─────────────────┘
                       │  一致性哈希路由
                       │
     ┌─────────────────┼─────────────────┐
     │                 │                 │
┌────▼────┐       ┌────▼────┐       ┌────▼────┐
│ Node A  │◄──────┤  etcd   ├──────►│ Node C  │
│ (gRPC)  │       │ 注册中心  │       │ (gRPC)  │
└────┬────┘       └─────────┘       └────┬────┘
     │                                    │
     └──────────────►┌────────┐◄──────────┘
                    │ Node B  │
                    │ (gRPC)  │
                    └────────┘
```

### 请求流程

**GET 流程：**

```
用户请求
  → 本地 LRU 缓存（命中直接返回）
  → SingleFlight 防击穿（同一 key 只允许一个线程回源）
  → 熔断检查（触发熔断则走 Fallback 降级）
  → gRPC 请求远程节点
  → 回源数据库（getter 回调）
  → 写入本地缓存并返回
```

**SET 流程：**

```
用户写入
  → 一致性哈希定位主节点
  → gRPC Set 写入主节点
  → 并发 Invalidate 广播到其余所有节点（使过期缓存失效）
  → 全部成功返回
```

**DELETE 流程：**

```
用户删除
  → 向所有存活节点广播 Delete
  → 各节点删除本地缓存
  → 全部完成返回
```

## 项目结构

```
.
├── include/kcache/              # SDK 公共头文件
│   └── client.h                 # KCacheClient 接口
├── src/
│   ├── cache/lru.cpp            # LRU 缓存（TTL、淘汰回调）
│   ├── client/client_sdk.cpp    # 客户端 SDK（服务发现、路由、熔断器池）
│   ├── consistent_hash/         # 一致性哈希（虚拟节点、自适应负载均衡）
│   ├── group/group.cpp          # 缓存组（Get/Set/Delete、Fallback）
│   ├── server/server.cpp        # gRPC 服务端（Prometheus 指标、安全加固）
│   ├── registry/registry.cpp    # etcd 服务注册（lease 心跳）
│   ├── proto/kcache.proto       # gRPC 协议定义
│   ├── include/kcache/
│   │   ├── cache.h              # ByteView + LRUCache 声明
│   │   ├── circuit_breaker.h    # 熔断器（三态流转）
│   │   ├── consistent_hash.h    # 一致性哈希声明
│   │   ├── group.h              # KCacheGroup 声明
│   │   ├── server.h             # KCacheServer 声明
│   │   ├── registry.h           # EtcdRegistry 声明
│   │   └── singleflight.h       # 防缓存击穿
│   └── main.cpp                 # 缓存节点入口
├── example/
│   └── http_gateway/            # HTTP REST 网关示例
│       └── http_gateway.cpp
├── test/
│   ├── test_lru.cpp             # LRU + TTL 测试
│   ├── test_consistent_hash.cpp # 一致性哈希测试
│   ├── test_group.cpp           # 缓存组 + SingleFlight 测试
│   └── test_circuit_breaker.cpp # 熔断器三态转换测试
├── Dockerfile
├── docker-compose.yml
├── CMakeLists.txt
├── conanfile.txt
└── LICENSE
```

## 构建与运行

### 1. 安装依赖

```sh
conan install . --build=missing -s build_type=Release
```

### 2. 配置项目

```sh
cmake --preset conan-release
```

### 3. 编译

```sh
cmake --build --preset conan-release -j
```

> 如果 CMake < 3.23，`--preset` 可能不生效，使用：
> ```sh
> cmake -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake \
>       -DCMAKE_BUILD_TYPE=Release -S . -B build -G Ninja
> cmake --build build -j
> ```

编译产物在 `bin/` 目录：
- `node_server` — 缓存节点
- `http_gateway` — HTTP 网关
- `test_lru` / `test_consistent_hash` / `test_group` / `test_circuit_breaker` — 测试

### 3. 启动 etcd

```sh
docker run -d --name etcd \
  -p 2379:2379 \
  quay.io/coreos/etcd:v3.5.0 \
  etcd --advertise-client-urls http://0.0.0.0:2379 \
       --listen-client-urls http://0.0.0.0:2379
```

### 4. 启动缓存节点

```sh
./bin/node_server --port=8001 --node=A
./bin/node_server --port=8002 --node=B
./bin/node_server --port=8003 --node=C
```

### 5. 启动 HTTP 网关（可选）

```sh
./bin/http_gateway --http_port=9000
```

## Docker 部署

### 构建镜像

```sh
docker build -t kcache:latest .
```

### 单节点

```sh
docker run -d \
  --name kcache-node \
  --network host \
  kcache:latest \
  /app/bin/node_server --port=8001 --node=A
```

### 集群一键启动

```sh
docker compose up -d
```

启动 5 个容器：`kcache-etcd` + `kcache-node-a/b/c` + `kcache-gateway`。

```sh
docker compose ps          # 查看状态
docker compose logs -f     # 查看日志
docker compose down        # 停止并清理
```

## HTTP API

网关提供 REST 接口，监听 `0.0.0.0:9000`：

### GET — 获取缓存

```sh
curl http://localhost:9000/api/cache/default/Tom
# → {"group":"default","key":"Tom","value":"400"}
```

### POST — 写入缓存

```sh
curl -X POST http://localhost:9000/api/cache/default/Kerolt \
  -d '{"value":"1219"}'
# → {"group":"default","key":"Kerolt","success":true,"value":"1219"}
```

### DELETE — 删除缓存

```sh
curl -X DELETE http://localhost:9000/api/cache/default/Kerolt
# → {"deleted":true,"group":"default","key":"Kerolt"}
```

## 命令行参数

### node_server

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--port` | 8001 | 节点 gRPC 端口 |
| `--node` | A | 节点标识符 |
| `--group` | default | 缓存组名称 |
| `--etcd_endpoints` | http://127.0.0.1:2379 | etcd 地址 |
| `--getter_timeout_ms` | 3000 | getter 超时（ms），0=不超时 |
| `--metrics_port` | 0 | Prometheus 指标端口，0=禁用 |
| `--cache_ttl_ms` | 0 | 缓存 TTL（ms），0=永不过期 |
| `--log_level` | info | 日志级别 |

### http_gateway

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--http_port` | 9000 | HTTP 监听端口 |
| `--etcd_endpoints` | http://127.0.0.1:2379 | etcd 地址 |
| `--service_name` | kcache | 缓存服务名 |

## Prometheus 指标

启用 `--metrics_port=8080` 后，访问 `http://localhost:8080/metrics` 暴露以下指标：

| 指标 | 类型 | 说明 |
|---|---|---|
| `kcache_local_hits` | Counter | 本地缓存命中 |
| `kcache_local_misses` | Counter | 本地缓存未命中 |
| `kcache_peer_hits` | Counter | 远端节点命中 |
| `kcache_peer_misses` | Counter | 远端节点未命中 |
| `kcache_loader_hits` | Counter | 回源成功 |
| `kcache_loader_errors` | Counter | 回源失败 |
| `kcache_circuit_breaks` | Counter | 熔断触发次数 |
| `kcache_fallback_hits` | Counter | 降级命中次数 |
| `kcache_getter_timeouts` | Counter | getter 超时次数 |
| `kcache_hit_ratio` | Gauge | 命中率 |
| `kcache_avg_load_duration_ms` | Gauge | 平均加载耗时 |
| `kcache_circuit_breaker_state` | Gauge | 熔断器状态 (0=Closed, 1=Open, 2=HalfOpen) |
| `kcache_cache_bytes` | Gauge | 当前缓存占用 |
| `kcache_cache_max_bytes` | Gauge | 缓存容量上限 |
| `kcache_cache_count` | Gauge | 缓存条目数 |

## 核心组件

### 客户端 SDK

```cpp
#include "kcache/client.h"

// 初始化（支持自定义熔断配置、RPC 超时、并发流限制）
CircuitBreakerConfig cb_cfg;
cb_cfg.failure_threshold = 5;
cb_cfg.recovery_timeout_ms = 5000;

KCacheClient client(
    "http://127.0.0.1:2379",           // etcd 地址
    "kcache",                            // 服务名
    cb_cfg,                              // 熔断配置
    std::chrono::milliseconds{200},      // RPC 超时
    256                                  // 最大并发流
);

auto value = client.Get("default", "Tom");      // 获取
client.Set("default", "Tom", "value");           // 写入
client.Delete("default", "Tom");                 // 删除
```

### 熔断器

三态流转，防止雪崩：

- **Closed** — 正常状态。滑动窗口内累计失败达 `failure_threshold` 次后进入 Open
- **Open** — 拒绝所有请求。等待 `recovery_timeout_ms` 后自动过渡到 HalfOpen
- **HalfOpen** — 放行最多 `half_open_max_calls` 个探测请求。探测成功 `success_threshold` 次则恢复 Closed，任一失败则重新 Open

```cpp
CircuitBreakerConfig cfg;
cfg.failure_threshold        = 5;     // 5 次失败触发熔断
cfg.recovery_timeout_ms      = 5000;  // 5 秒后尝试恢复
cfg.half_open_max_calls      = 2;     // 半开期最多 2 个探测请求
cfg.success_threshold        = 2;     // 2 次成功关闭熔断
cfg.failure_reset_timeout_ms = 10000; // 失效计数过期窗口
```

### SingleFlight

防止缓存击穿——同一 key 的并发请求只回源一次，其他请求等待并共享结果。支持等待超时、失败冷却期，避免故障 key 反复创建线程。

### 一致性哈希

- CRC32 IEEE 哈希函数（兼容 Go `crc32.ChecksumIEEE`）
- 虚拟节点机制，可配置 replica 数范围
- 后台线程监控负载，超过 25% 不均衡时自适应调整虚拟节点
- 线程安全（`shared_mutex` + 原子计数器）

### Fallback 降级

回源失败时按优先级降级：
1. 返回 stale_cache（过期缓存）
2. 调用 fallback getter（用户自定义降级函数）
3. 返回空

## 压测

单节点 gRPC Get 压测（使用 [ghz](https://ghz.sh/)）：

```sh
ghz --insecure \
  --proto ./src/proto/kcache.proto \
  --call kcache.pb.KCache/Get \
  --data '{"group":"default","key":"Tom"}' \
  --connections=4 --concurrency=128 \
  --duration=60s --skipFirst=1000 \
  localhost:8001
```

**结果摘要：**

```
Count:        1,062,812
Total:        60.01 s
Requests/sec: 17,710
Average:      5.29 ms
p50:          4.74 ms
p99:          15.69 ms
```

并发梯度测试：

```sh
for c in 16 32 64 128 256 512; do
  ghz --insecure \
    --proto ./src/proto/kcache.proto \
    --call kcache.pb.KCache/Get \
    --data '{"group":"default","key":"Tom"}' \
    --connections=4 --concurrency=$c \
    --duration=60s --skipFirst=1000 \
    localhost:8001
done
```

## 设计借鉴

- [Memcached](https://memcached.org/) — C/S 架构、客户端 SDK
- [GroupCache](https://github.com/golang/groupcache) — Group 概念、SingleFlight
- [7days-golang](https://github.com/geektutu/7days-golang) — 分布式缓存教程
- [KamaCache-Go](https://github.com/youngyangyang04/KamaCache-Go) — 本项目 Go 语言参考实现

## 许可证

MIT License. 详见 [LICENSE](LICENSE)。
