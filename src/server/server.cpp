#include "kcache/server.h"

#include <fmt/base.h>
#include <fmt/format.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <spdlog/spdlog.h>

#include <memory>
#include <thread>

#include "kcache.pb.h"
#include "kcache/group.h"

namespace kcache {

KCacheServer::KCacheServer(const std::string& addr, const std::string& svc_name, ServerOptions opts)
    : addr_(addr), svc_name_(svc_name), opts_(opts) {
    // 创建etcd注册器，连接到配置项里指定的第一个 etcd 节点，即默认地址http://127.0.0.1:2379
    etcd_register_ = std::make_unique<EtcdRegistry>(opts_.etcd_endpoints[0]);
    if (!etcd_register_->Register(svc_name_, addr_)) {
        throw std::runtime_error("[kcache] Failed to register service with etcd");
    }
}

auto KCacheServer::Get(grpc::ServerContext* context, const pb::Request* request, pb::GetResponse* response)
    -> grpc::Status {
    auto group = GetCacheGroup(request->group()); // 请求中封装了组名，键名以及值
    if (!group) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Group not found");
    }
    auto value = group->Get(request->key()); // 调用组内的get获取指定键名的值
    if (!value) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Key not found");
    }
    response->set_value(value->ToString()); // 将获取到的值返回
    return grpc::Status::OK;
}
// 同上
auto KCacheServer::Set(grpc::ServerContext* context, const pb::Request* request, pb::SetResponse* response)
    -> grpc::Status {
    auto group = GetCacheGroup(request->group());
    if (!group) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Group not found");
    }
    bool is_set = group->Set(request->key(), request->value());
    response->set_value(is_set);
    return grpc::Status::OK;
}

auto KCacheServer::Delete(grpc::ServerContext* context, const pb::Request* request, pb::DeleteResponse* response)
    -> grpc::Status {
    auto group = GetCacheGroup(request->group());
    if (!group) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Group not found");
    }
    bool is_delete = group->Delete(request->key());
    response->set_value(is_delete);
    return grpc::Status::OK;
}

auto KCacheServer::Invalidate(grpc::ServerContext* context, const pb::Request* request,
                              pb::InvalidateResponse* response) -> grpc::Status {
    auto group = GetCacheGroup(request->group());
    if (!group) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Group not found");
    }
    // Invalidate RPC 调用总是来自其他节点，调用 InvalidateFromPeer 避免循环传播
    bool is_invalidate = group->InvalidateFromPeer(request->key());
    response->set_value(is_invalidate);
    return grpc::Status::OK;
}

// 启动 gRPC 服务器
void KCacheServer::Start() {
    try {
        // 配置gRPC服务器选项
        grpc::ServerBuilder builder;

        // 设置最大消息大小，防止恶意客户端发送大数据包攻击服务器
        builder.SetMaxReceiveMessageSize(opts_.max_msg_size);
        builder.SetMaxSendMessageSize(opts_.max_msg_size);

        // 配置TLS或非安全连接
        if (opts_.tls) {
            auto creds = LoadTLSCredentials(opts_.cert_file, opts_.key_file);
            if (!creds) {
                throw std::runtime_error("Failed to load TLS credentials");
            }
            builder.AddListeningPort(addr_, creds);
        } else {
            builder.AddListeningPort(addr_, grpc::InsecureServerCredentials());
        }

        // 启用默认健康检查服务
        grpc::EnableDefaultHealthCheckService(true);
        // TCP 底层调优：开启 KeepAlive 探活机制
        builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_KEEPALIVE_TIME_MS, 30000));
        builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000));

        // ★ 资源配额：限制 gRPC 线程池最大线程数，防止并发飙升时 OOM
        if (opts_.max_threads > 0) {
            auto rq = grpc::ResourceQuota("kcache_server");
            rq.SetMaxThreads(opts_.max_threads);
            builder.SetResourceQuota(rq);
        }
        // ★ 完成队列数：每 CQ 一个轮询线程，过大浪费、过小竞争
        if (opts_.num_cqs > 0) {
            builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::NUM_CQS, opts_.num_cqs);
        }
        // ★ 每连接最大并发流：拒绝单连接超额请求（HTTP/2 流控）
        if (opts_.max_concurrent_streams > 0) {
            builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_MAX_CONCURRENT_STREAMS,
                                                              opts_.max_concurrent_streams));
        }

        // 注册服务
        builder.RegisterService(this);

        // 构建并启动服务器
        grpc_server_ = builder.BuildAndStart();
        if (!grpc_server_) {
            throw std::runtime_error("Failed to build and start gRPC server");
        }

        // 设置健康检查状态
        auto health_service = grpc_server_->GetHealthCheckService();
        if (health_service) {
            health_service->SetServingStatus(svc_name_, true);
        }

        is_stop_ = false;

        // 启动 Prometheus 指标 HTTP 服务（独立线程）
        StartMetricsServer();

        spdlog::info("gRPC Server start success at {}!", addr_);

        grpc_server_->Wait(); // 服务器进入死循环监听状态，直到调用 Stop() 才会被唤醒

    } catch (const std::exception& e) {
        spdlog::error("Failed to start gRPC Server: {}", e.what());
        throw;
    }
}

// 关闭 gRPC 服务器
void KCacheServer::Stop() {
    is_stop_ = true;
    if (metrics_server_) {
        metrics_server_->stop();
        metrics_server_.reset();
    }
    if (etcd_register_) {
        etcd_register_->Unregister();
        etcd_register_.reset();
    }
    if (grpc_server_) {
        grpc_server_->Shutdown();
        grpc_server_.reset();
    }
    spdlog::info("gRPC Server {} stopped.", addr_);
}

// 启动 Prometheus 指标 HTTP 服务
void KCacheServer::StartMetricsServer() {
    if (opts_.metrics_port <= 0) {
        spdlog::info("Metrics HTTP server disabled (metrics_port=0)");
        return;
    }

    metrics_server_ = std::make_unique<httplib::Server>();

    // 注册 /metrics 路由
    metrics_server_->Get("/metrics", [](const httplib::Request&, httplib::Response& res) {
        res.set_content(GenerateMetrics(), "text/plain; version=0.0.4; charset=utf-8");
    });

    // 在独立线程中启动 HTTP 服务器
    int port = opts_.metrics_port;
    std::thread([this, port] {
        if (!metrics_server_->listen("0.0.0.0", port)) {
            spdlog::error("Failed to start metrics HTTP server on port {}", port);
        }
    }).detach();

    spdlog::info("Metrics HTTP server started on port {}", port);
}

// 生成 Prometheus 格式指标文本
auto KCacheServer::GenerateMetrics() -> std::string {
    std::string out;
    auto names = GetAllGroupNames();

    for (const auto& name : names) {
        auto* group = GetCacheGroup(name);
        if (!group) continue;

        const auto& s = group->GetStatus();
        auto q = [&name](const char* metric) {
            return fmt::format("{}{{group=\"{}\"}}", metric, name);
        };

        // 计数器指标
        out += fmt::format("# HELP kcache_local_hits Total local cache hits\n");
        out += fmt::format("# TYPE kcache_local_hits counter\n");
        out += fmt::format("{} {}\n", q("kcache_local_hits"), s.local_hits.load());

        out += fmt::format("# HELP kcache_local_misses Total local cache misses\n");
        out += fmt::format("# TYPE kcache_local_misses counter\n");
        out += fmt::format("{} {}\n", q("kcache_local_misses"), s.local_misses.load());

        // 命中率（gauge）
        auto total_gets = s.local_hits.load() + s.local_misses.load();
        double hit_ratio = total_gets > 0 ? static_cast<double>(s.local_hits.load()) / total_gets : 0.0;
        out += fmt::format("# HELP kcache_hit_ratio Cache hit ratio\n");
        out += fmt::format("# TYPE kcache_hit_ratio gauge\n");
        out += fmt::format("{} {:.6f}\n", q("kcache_hit_ratio"), hit_ratio);

        out += fmt::format("# HELP kcache_peer_hits Total peer cache hits\n");
        out += fmt::format("# TYPE kcache_peer_hits counter\n");
        out += fmt::format("{} {}\n", q("kcache_peer_hits"), s.peer_hits.load());

        out += fmt::format("# HELP kcache_peer_misses Total peer cache misses\n");
        out += fmt::format("# TYPE kcache_peer_misses counter\n");
        out += fmt::format("{} {}\n", q("kcache_peer_misses"), s.peer_misses.load());

        out += fmt::format("# HELP kcache_loader_hits Total loader hits\n");
        out += fmt::format("# TYPE kcache_loader_hits counter\n");
        out += fmt::format("{} {}\n", q("kcache_loader_hits"), s.loader_hits.load());

        out += fmt::format("# HELP kcache_loader_errors Total loader errors\n");
        out += fmt::format("# TYPE kcache_loader_errors counter\n");
        out += fmt::format("{} {}\n", q("kcache_loader_errors"), s.loader_errors.load());

        // 平均加载延迟（毫秒）
        auto loads = s.loads.load();
        double avg_ms = loads > 0 ? static_cast<double>(s.load_duration.load()) / loads / 1e6 : 0.0;
        out += fmt::format("# HELP kcache_avg_load_duration_ms Average load duration in milliseconds\n");
        out += fmt::format("# TYPE kcache_avg_load_duration_ms gauge\n");
        out += fmt::format("{} {:.3f}\n", q("kcache_avg_load_duration_ms"), avg_ms);

        out += fmt::format("# HELP kcache_circuit_breaks Total circuit breaker triggers\n");
        out += fmt::format("# TYPE kcache_circuit_breaks counter\n");
        out += fmt::format("{} {}\n", q("kcache_circuit_breaks"), s.circuit_breaks.load());

        out += fmt::format("# HELP kcache_fallback_hits Total fallback hits\n");
        out += fmt::format("# TYPE kcache_fallback_hits counter\n");
        out += fmt::format("{} {}\n", q("kcache_fallback_hits"), s.fallback_hits.load());

        out += fmt::format("# HELP kcache_getter_timeouts Total getter timeouts\n");
        out += fmt::format("# TYPE kcache_getter_timeouts counter\n");
        out += fmt::format("{} {}\n", q("kcache_getter_timeouts"), s.getter_timeouts.load());

        // 熔断器状态（gauge: 1=Closed, 2=Open, 3=HalfOpen）
        auto cb_state = group->CircuitBreakerState();
        int cb_val = (cb_state == "Closed") ? 1 : (cb_state == "Open") ? 2 : 3;
        out += fmt::format("# HELP kcache_circuit_breaker_state Circuit breaker state (1=Closed, 2=Open, 3=HalfOpen)\n");
        out += fmt::format("# TYPE kcache_circuit_breaker_state gauge\n");
        out += fmt::format("{} {}\n", q("kcache_circuit_breaker_state"), cb_val);

        // LRU 缓存容量信息
        out += fmt::format("# HELP kcache_cache_bytes Current cache bytes in use\n");
        out += fmt::format("# TYPE kcache_cache_bytes gauge\n");
        out += fmt::format("{} {}\n", q("kcache_cache_bytes"), group->CacheBytes());

        out += fmt::format("# HELP kcache_cache_max_bytes Maximum cache bytes\n");
        out += fmt::format("# TYPE kcache_cache_max_bytes gauge\n");
        out += fmt::format("{} {}\n", q("kcache_cache_max_bytes"), group->CacheMaxBytes());

        out += fmt::format("# HELP kcache_cache_count Number of cache entries\n");
        out += fmt::format("# TYPE kcache_cache_count gauge\n");
        out += fmt::format("{} {}\n", q("kcache_cache_count"), group->CacheCount());

        out += "\n";
    }

    return out;
}

// TODO 实现加载 TLS 证书
auto KCacheServer::LoadTLSCredentials(const std::string& cert_file, const std::string& key_file)
    -> std::shared_ptr<grpc::ServerCredentials> {
    return grpc::SslServerCredentials(grpc::SslServerCredentialsOptions{});
}

}  // namespace kcache
