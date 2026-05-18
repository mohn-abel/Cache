#include "kcache/client.h"

#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

#include "kcache.grpc.pb.h" // 由Protobuf协议文件生成的C++头文件

namespace kcache {

KCacheClient::KCacheClient(const std::string& etcd_endpoints, const std::string& service_name,
                           CircuitBreakerConfig cb_cfg, std::chrono::milliseconds rpc_timeout,
                           int max_concurrent_streams)
    : service_name_(service_name),
      cb_cfg_(cb_cfg),
      rpc_timeout_ms_(rpc_timeout),
      max_concurrent_streams_(max_concurrent_streams) {
    etcd_client_ = std::make_shared<etcd::Client>(etcd_endpoints); // 连接到etcd服务注册中心
    StartServiceDiscovery(); // 构造时开启服务发现
}

KCacheClient::~KCacheClient() {
    if (etcd_watcher_) {
        etcd_watcher_->Cancel(); // 监听器关机
    }
    if (discovery_thread_.joinable()) {
        discovery_thread_.join(); // 阻塞主线程，等待监听线程退出
    } // 线程解释：joinable()表示当前线程还在运行，join()会阻塞当前线程，直到当前线程执行完毕
}
// client客户端获取value
auto KCacheClient::Get(const std::string& group, const std::string& key) -> std::optional<std::string> {
    auto target_addr = GetCacheNode(key); // 获取目标缓存所在的真实节点IP
    if (target_addr.empty()) {
        spdlog::warn("No cache service available for key: {}", key);
        return std::nullopt;
    }

    // ★ 新增：熔断检查
    auto* breaker = GetOrCreateBreaker(target_addr);
    if (!breaker->Allow()) {
        spdlog::warn("[CircuitBreaker:client:{}] Open, skipping Get key={}", target_addr, key);
        return std::nullopt;  // 或尝试下一个节点（进阶方案）
    }

    auto channel = GetOrCreateChannel(target_addr); // 创建访问通道
    auto client = pb::KCache::NewStub(channel); // 根据通道创建客户端Stub
    if (!client) {
        spdlog::error("Failed to create gRPC stub for node: {}", target_addr);
        return std::nullopt;
    }
    // 组装请求
    pb::Request request;
    request.set_group(group); // 由protobuf自动生成的方法，发起RPC请求，发送要访问的缓存组名
    request.set_key(key); // 发送要访问的缓存键名

    pb::GetResponse response; // 获取到的回复
    grpc::ClientContext context; // 回复内容

    // ★ gRPC 超时控制：防止远端节点假死导致调用线程无限阻塞。
    // 超时后 gRPC 底层返回 DEADLINE_EXCEEDED，线程立即恢复 -> 触发熔断 -> 走 Fallback 降级。
    context.set_deadline(std::chrono::system_clock::now() + rpc_timeout_ms_);

    // Get由protobuf结合proto文件自动生成的，后续的Set、Invalidate、Delete同理
    auto status = client->Get(&context, request, &response); // 处理通信结果
    if (status.ok()) {
        breaker->RecordSuccess(); // 记录一次成功
        return response.value();
    } else {
        // NOT_FOUND（缓存未命中）是正常业务语义，不计入故障；
        // 其余（含 DEADLINE_EXCEEDED / UNAVAILABLE / INTERNAL 等）均计入 RecordFailure 触发熔断。
        if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
            spdlog::debug("Get key={} on node {}: NOT_FOUND (cache miss)", key, target_addr);
        } else {
            breaker->RecordFailure(); // 记录一次失败（含超时、网络故障等）
            spdlog::warn("Get failed on node {}: {} ({})", target_addr, status.error_message(),
                         static_cast<int>(status.error_code()));
        }
        return std::nullopt;
    }
}
// client客户端由写入value，并使现存的该缓存失效
bool KCacheClient::Set(const std::string& group, const std::string& key, const std::string& value) {
    auto target_addr = GetCacheNode(key); // 获取目标缓存所在的真实节点IP
    if (target_addr.empty()) {
        spdlog::warn("No cache service available for Set");
        return false;
    }

    // ★ 熔断检查
    auto* breaker = GetOrCreateBreaker(target_addr);
    if (!breaker->Allow()) {
        spdlog::warn("[CircuitBreaker:client:{}] Open, skipping Set key={}", target_addr, key);
        return false;
    }
    // 同get
    auto channel = GetOrCreateChannel(target_addr);
    auto client = pb::KCache::NewStub(channel);
    if (!client) {
        spdlog::error("Failed to create gRPC stub for node: {}", target_addr);
        return false;
    }
    // 组装请求
    pb::Request request; 
    request.set_group(group);
    request.set_key(key);
    request.set_value(value);

    pb::SetResponse response;
    grpc::ClientContext context;
    // ★ gRPC 超时控制
    context.set_deadline(std::chrono::system_clock::now() + rpc_timeout_ms_);
    auto status = client->Set(&context, request, &response); // 处理通信结果

    if (status.ok() && response.value()) {
        breaker->RecordSuccess(); // 记录一次成功
    } else {
        breaker->RecordFailure();
        spdlog::error("Failed to set value on node {}: {}", target_addr, status.error_message());
        return false;
    }

    // 广播其他节点发送缓存失效通知
    bool all_success = true;
    {
        std::lock_guard<std::mutex> lock(nodes_mutex_);
        for (const auto& addr : cache_nodes_) {
            if (addr != target_addr) {
                auto* peer_breaker = GetOrCreateBreaker(addr); // 获取该节点的熔断器
                if(!peer_breaker->Allow()){ // 该节点触发熔断，跳过该节点
                    spdlog::warn("Skip Invalidate on circuit-broken node {}", addr);
                    continue;
                }

                // 向非目标节点发送通信请求
                auto channel = GetOrCreateChannel(addr);
                auto client = pb::KCache::NewStub(channel);
                pb::InvalidateResponse response; // 使缓存失效信号
                grpc::ClientContext ctx;
                // ★ gRPC 超时控制
                ctx.set_deadline(std::chrono::system_clock::now() + rpc_timeout_ms_);

                auto status = client->Invalidate(&ctx, request, &response); // 处理通信结果
                if (status.ok() && response.value()) {
                    peer_breaker->RecordSuccess(); // 记录一次成功
                } else {
                    peer_breaker->RecordFailure();
                    all_success = false;
                    spdlog::warn("Failed to Invalidate key on node {}", addr);
                }
            }
        }
    }

    return all_success; // 其他节点缓存失效才算操作成功
}
// 删除指定缓存
bool KCacheClient::Delete(const std::string& group, const std::string& key) {
    // 组装请求
    pb::Request request;
    request.set_group(group);
    request.set_key(key);

    bool all_success = true;
    {
        std::lock_guard<std::mutex> lock(nodes_mutex_);
        if (cache_nodes_.empty()) {
            spdlog::warn("No cache service available for Delete");
            return false;
        }
        // 查找所有存活节点
        for (const auto& addr : cache_nodes_) {
            auto* peer_breaker = GetOrCreateBreaker(addr);
            if(!peer_breaker->Allow()){
                spdlog::warn("Circuit breaker open for node {}, skipping Delete", addr);
                all_success = false;
                continue;
            }

            auto channel = GetOrCreateChannel(addr);
            auto client = pb::KCache::NewStub(channel);
            grpc::ClientContext ctx;
            // ★ gRPC 超时控制
            ctx.set_deadline(std::chrono::system_clock::now() + rpc_timeout_ms_);
            pb::DeleteResponse response;

            auto status = client->Delete(&ctx, request, &response); // 处理通信结果
            if (status.ok() && response.value()) {
                peer_breaker->RecordSuccess();
            } else {
                peer_breaker->RecordFailure();
                all_success = false;
                spdlog::warn("Failed to delete key on node {}", addr);
            }
        }
    }
    return all_success;
}

bool KCacheClient::StartServiceDiscovery() {
    if (!FetchAllServices()) {
        return false;
    }
    // 开启后台监听线程，监听存活节点
    discovery_thread_ = std::thread{[this] {
        std::string prefix = "/services/" + service_name_ + "/";
        spdlog::debug("Starting etcd watcher for prefix: {}", prefix);
        etcd_watcher_ = std::make_unique<etcd::Watcher>(
            *etcd_client_, prefix, [this](etcd::Response resp) { HandleWatchEvents(resp); }, true);
        etcd_watcher_->Wait(); // 将线程设置成死循环
    }};
    return true;
}

void KCacheClient::HandleWatchEvents(const etcd::Response& resp) {
    std::lock_guard<std::mutex> lock{nodes_mutex_}; // 有人上下线就要加锁
    if (!resp.is_ok()) {
        spdlog::error("Failed to watching etcd: {}", resp.error_message());
        return;
    }
    
    for (const auto& event : resp.events()) {
        std::string key = event.kv().key(); // 提取发生变动的key
        std::string addr = ParseAddrFromKey(key); // 拿到发生变动的机器IP
        if (addr.empty()) {
            continue;
        }
        // 判断事件类型
        switch (event.event_type()) {
            // 有新服务器注册
            case etcd::Event::EventType::PUT: {
                // 没有这个IP才添加该服务器IP
                if (cache_nodes_.find(addr) == cache_nodes_.end()) {
                    cache_nodes_.insert(addr); // 记录这个地址
                    consistent_hash_.Add({addr}); // 一致性哈希路由上添加该节点，重新分配哈希环
                }
                spdlog::debug("Service added: {} (key: {})", addr, key);
                break;
            }
            // 服务器掉线
            case etcd::Event::EventType::DELETE_: {
                // 有这个IP才进行失效注销
                if (cache_nodes_.find(addr) != cache_nodes_.end()) {
                    cache_nodes_.erase(addr); // 移除该IP
                    consistent_hash_.Remove(addr); // 一致性哈希路由上删除该节点，重新分配哈希环
                    breaker_pool_.erase(addr); // 清理该节点的熔断器
                    spdlog::debug("Service removed: {} (key: {})", addr, key);
                }
                break;
            }
            default:
                spdlog::debug("Unknown event type: {} for key: {}", static_cast<int>(event.event_type()), key);
                break;
        }
    }
}
// 首次拉取所有存活的缓存节点
bool KCacheClient::FetchAllServices() {
    std::string prefix_key = "/services/" + service_name_ + "/"; // 确定拉取目录
    etcd::Response resp = etcd_client_->ls(prefix_key).get(); // 调用etcd客户端的ls命令，.get会阻塞等待结果返回

    if (!resp.is_ok()) {
        spdlog::error("Failed to get all services from etcd now: {}", resp.error_message());
        return false;
    }
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    // 遍历etcd上的每一个key
    for (const auto& key : resp.keys()) {
        std::string addr = ParseAddrFromKey(key); // 处理key，返回单纯的ip地址
        // 有效存活节点
        if (!addr.empty()) {
            cache_nodes_.insert(addr); // 记录存活节点
            consistent_hash_.Add({addr}); // 执行一致性哈希，并制造虚拟节点构成哈希环
            spdlog::debug("Discovered service at {}", addr);
        }
    }
    return true;
}
// 字符串处理工具
auto KCacheClient::ParseAddrFromKey(const std::string& key) -> std::string {
    // 例如service_name_是Kcache，则prefix = /services/Kcache/
    std::string prefix = "/services/" + service_name_ + "/";
    // 检验key的前缀是否刚好是prefix
    // 从 key 的第 0 个位置开始反向查找 prefix，如果找到了且刚好在第 0 位，说明是以它开头的。
    // 
    if (key.rfind(prefix, 0) == 0) {
        return key.substr(prefix.length()); // "/services/Kcache/192.168.1.10:8080" 变成了 "192.168.1.10:8080"
    }
    return "";
}
// 路由寻址：由一致性哈希找到目标真实节点
auto KCacheClient::GetCacheNode(const std::string& key) -> std::string {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    if (cache_nodes_.empty()) {
        return "";
    }

    std::string target_addr = consistent_hash_.Get(key); // 一致性哈希找到目标真实节点
    // 兜底策略：如果一致性哈希意外失败，强行返回当前存活节点列表里的第一个节点
    if (target_addr.empty()) {
        target_addr = *cache_nodes_.begin();
    }

    spdlog::debug("Routing key '{}' to node '{}'", key, target_addr);
    return target_addr;
}
// 新增辅助函数，新增或获取新的grpc连接
auto KCacheClient::GetOrCreateChannel(const std::string& addr) -> std::shared_ptr<grpc::Channel> {
    std::lock_guard<std::mutex> lock(channel_mutex_);
    
    // 如果连接池中已经有该节点的 Channel，直接复用返回
    auto it = channel_pool_.find(addr);
    if (it != channel_pool_.end()) {
        return it->second;
    }
    
    // 如果没有，则创建新的 Channel 并存入连接池
    // ★ 开启 gRPC KeepAlive：允许在 ××秒内没有收到对端响应时快速断开 TCP 连接，
    //   配合 set_deadline 形成双层超时保护（应用层 deadline + 传输层 KeepAlive）。
    //   即使应用层 deadline 未命中（罕见极端情况），KeepAlive 也会在超时后关闭连接，
    //   避免调用线程永久挂死在半开 TCP 连接上。
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS,              10000);  // 每 10s 发一次 KeepAlive ping
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,            3000);  // 3s 内未收到 pong 视为连接断开
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS,     1);  // 即使无活动 RPC 也允许发 KeepAlive ping
    args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA,       0);  // 允许无限 ping 而不被服务端 ban

    // ★ 每连接最大并发流：限制单条连接的并发 HTTP/2 stream 数
    if (max_concurrent_streams_ > 0) {
        args.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, max_concurrent_streams_);
    }

    auto channel = grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), args);
    channel_pool_[addr] = channel;
    return channel;
}

// 新增辅助函数：新增或获取熔断器
auto KCacheClient::GetOrCreateBreaker(const std::string& addr) -> CircuitBreaker* {
    
    auto it = breaker_pool_.find(addr);
    if (it != breaker_pool_.end()) {
        return it->second.get();
    }
    auto breaker = std::make_unique<CircuitBreaker>("client:" + addr, cb_cfg_);
    auto* ptr = breaker.get();
    breaker_pool_[addr] = std::move(breaker);
    return ptr;
}

}  // namespace kcache
