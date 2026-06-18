#ifndef KCACHE_CLIENT_H_
#define KCACHE_CLIENT_H_

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
// etcd服务注册中心，有新节点加入就会在这上面注册节点地址信息
#include <etcd/Client.hpp> // etcd官方客户端，用于拉取节点
#include <etcd/Watcher.hpp> // etcd监听器，观察节点变化

#include "kcache/consistent_hash.h"
#include <unordered_map>
#include <grpcpp/grpcpp.h>
#include "kcache/circuit_breaker.h"


namespace kcache {
// client作为网关服务，负责接收缓存请求计算缓存所在节点，并向该节点索要缓存数据
class KCacheClient {
public:
    // 构造函数：需要etcd服务集群的地址以及客户端名称
    // rpc_timeout_ms: 每次gRPC调用（Get/Set/Invalidate/Delete）的最长等待时间，
    //   避免远端节点假死导致调用线程无限阻塞，默认200ms
    // max_concurrent_streams: 每连接最大并发 HTTP/2 stream，0 表示不限制，推荐 256~512
    KCacheClient(const std::string& etcd_endpoints, const std::string& service_name = "kcache",
                 CircuitBreakerConfig cb_cfg = {},
                 std::chrono::milliseconds rpc_timeout = std::chrono::milliseconds{200},
                 int max_concurrent_streams = 0);
    ~KCacheClient();

    // 禁止拷贝和赋值
    KCacheClient(const KCacheClient&) = delete;
    KCacheClient& operator=(const KCacheClient&) = delete;

    // 获取缓存
    auto Get(const std::string& group, const std::string& key) -> std::optional<std::string>;

    // 设置缓存
    bool Set(const std::string& group, const std::string& key, const std::string& value);

    // 删除缓存
    bool Delete(const std::string& group, const std::string& key);

private:
    // 服务发现相关
    bool StartServiceDiscovery(); // 启用服务发现的流水线
    void HandleWatchEvents(const etcd::Response& resp); // etcd的回调函数，有节点上线/下线就触发这个函数
    bool FetchAllServices(); // 初始化时，一口气把当前所有的存活节点全拉下来
    auto ParseAddrFromKey(const std::string& key) -> std::string; // 字符串处理：把 etcd 里的 key（如 "/services/kcache/192.168.1.1:8080"）切出 IP 端口
    auto GetCacheNode(const std::string& key) -> std::string; // 路由寻址核心：输入业务 key，返回到底该去哪个 IP 地址拿数据
    // 新增：辅助函数，获取gRPC连接
    auto GetOrCreateChannel(const std::string& addr) -> std::shared_ptr<grpc::Channel>;
    // 清理已下线节点的 gRPC Channel 缓存
    void RemoveChannel(const std::string& addr);
    // 新增：per-node 熔断器池
    auto GetOrCreateBreaker(const std::string& addr) -> CircuitBreaker*;
private:
    std::string service_name_; // 服务前端标识
    // etcd服务组件
    std::shared_ptr<etcd::Client> etcd_client_; // 可以被多个模块复用所以使用share_ptr
    std::unique_ptr<etcd::Watcher> etcd_watcher_; // 后台线程独占，所以用unique_ptr

    std::unordered_set<std::string> cache_nodes_; // 记录存活节点
    std::mutex nodes_mutex_; // 保护一致性哈希和存活节点的互斥锁

    std::thread discovery_thread_; // 后台监听线程，用于运行etcd_watcher
    ConsistentHashMap consistent_hash_; // 一致性哈希路由器
    // 新增：连接池与互斥锁
    std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channel_pool_;
    std::mutex channel_mutex_;

    // 新增成员：节点per-node熔断池以及节点配置
    std::unordered_map<std::string, std::unique_ptr<CircuitBreaker>> breaker_pool_;
    CircuitBreakerConfig cb_cfg_;  // 保存配置，每个新节点复用

    // ★ gRPC 调用超时控制：防止对端假死导致调用线程无限阻塞，引发线程池耗尽与级联雪崩
    std::chrono::milliseconds rpc_timeout_ms_{200};

    // ★ 每连接最大并发流：限制单条 gRPC 连接上的并发 stream 数量（HTTP/2 流控）
    //   0 表示不限制（使用 gRPC 默认值 2147483647）
    int max_concurrent_streams_{0};
};

}  // namespace kcache

#endif  // KCACHE_CLIENT_H_
