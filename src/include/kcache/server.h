#ifndef GRPC_SERVER_H_
#define GRPC_SERVER_H_

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <etcd/Client.hpp>
#include <httplib.h>

#include "kcache.grpc.pb.h"
#include "kcache.pb.h"
#include "kcache/registry.h" // 注册报道组件

namespace kcache {

struct ServerOptions {
    std::vector<std::string> etcd_endpoints;
    std::chrono::milliseconds dial_timeout;
    int max_msg_size;  // bytes
    bool tls;
    std::string cert_file;
    std::string key_file;
    int metrics_port;  // Prometheus 指标 HTTP 端口，0 表示禁用
    int max_threads;   // gRPC 线程池最大线程数，0 表示不限制（默认）
    int num_cqs;       // 完成队列数量，通常设为 CPU 核心数的 1~2 倍，0 表示使用 gRPC 默认值
    int max_concurrent_streams; // 每连接最大并发流，0 表示不限制（默认）

    // Default constructor to set default values
    ServerOptions()
        : etcd_endpoints({"http://127.0.0.1:2379"}), // 默认etcd地址
          dial_timeout(std::chrono::seconds(5)),
          max_msg_size(4 << 20),  // 默认接受最大4MB数据包
          tls(false),
          metrics_port(0),   // 默认禁用 metrics 端口
          max_threads(0),    // 默认不限制线程数
          num_cqs(0),        // 默认使用 gRPC 内置值
          max_concurrent_streams(0) {}  // 默认不限制每连接并发流
};

// Function type for options
using ServerOption = std::function<void(ServerOptions*)>;

// Option functions
inline auto WithEtcdEndpoints(const std::vector<std::string>& endpoints) -> ServerOption {
    return [endpoints](ServerOptions* o) { o->etcd_endpoints = endpoints; };
}

inline auto WithDialTimeout(std::chrono::milliseconds timeout) -> ServerOption {
    return [timeout](ServerOptions* o) { o->dial_timeout = timeout; };
}

inline auto WithTLS(const std::string& certFile, const std::string& keyFile) -> ServerOption {
    return [certFile, keyFile](ServerOptions* o) {
        o->tls = true;
        o->cert_file = certFile;
        o->key_file = keyFile;
    };
}

inline auto WithMetricsPort(int port) -> ServerOption {
    return [port](ServerOptions* o) { o->metrics_port = port; };
}

inline auto WithMaxThreads(int max_threads) -> ServerOption {
    return [max_threads](ServerOptions* o) { o->max_threads = max_threads; };
}

inline auto WithNumCqs(int num_cqs) -> ServerOption {
    return [num_cqs](ServerOptions* o) { o->num_cqs = num_cqs; };
}

inline auto WithMaxConcurrentStreams(int max_concurrent_streams) -> ServerOption {
    return [max_concurrent_streams](ServerOptions* o) { o->max_concurrent_streams = max_concurrent_streams; };
}
// 继承自自动生成的service，对其中的方法重写，final表示这个类是继承类的最底端，即其不能再有子类了
class KCacheServer final : public pb::KCache::Service {
public:
    KCacheServer(const std::string& addr, const std::string& svc_name, ServerOptions opts = ServerOptions{});
    ~KCacheServer() = default;
    // 服务端接受节点的Get请求
    auto Get(grpc::ServerContext* context, const pb::Request* request, pb::GetResponse* response)
        -> grpc::Status override;
    // 同上
    auto Set(::grpc::ServerContext* context, const pb::Request* request, pb::SetResponse* response)
        -> grpc::Status override;

    auto Delete(grpc::ServerContext* context, const pb::Request* request, pb::DeleteResponse* response)
        -> grpc::Status override;

    auto Invalidate(grpc::ServerContext* context, const pb::Request* request, pb::InvalidateResponse* response)
        -> grpc::Status override;

    void Start();

    void Stop();

private:
    // Helper for loading TLS credentials
    auto LoadTLSCredentials(const std::string& cert_file, const std::string& key_file)
        -> std::shared_ptr<grpc::ServerCredentials>;

    // 启动 Prometheus 指标 HTTP 服务
    void StartMetricsServer();

    // 生成 Prometheus 格式指标文本
    static std::string GenerateMetrics();

private:
    std::string addr_;
    std::string svc_name_;

    std::unique_ptr<grpc::Server> grpc_server_;
    std::unique_ptr<EtcdRegistry> etcd_register_;
    std::unique_ptr<httplib::Server> metrics_server_;

    std::atomic<bool> is_stop_;

    ServerOptions opts_;
};

}  // namespace kcache

#endif
