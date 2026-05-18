#include <chrono>
#include <csignal>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <fmt/base.h>
#include <fmt/core.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <spdlog/spdlog.h>

#include "kcache/cache.h"
#include "kcache/group.h"
#include "kcache/server.h"

using namespace kcache;

DEFINE_int32(port, 8001, "节点端口");
DEFINE_string(node, "A", "节点标识符");
DEFINE_string(group, "default", "缓存组名称");
DEFINE_string(log_level, "info", "日志级别， 可选值：trace, debug, info, warn, error, critical");
DEFINE_string(etcd_endpoints, "http://127.0.0.1:2379", "etcd地址");
DEFINE_int64(getter_timeout_ms, 3000, "getter超时时间(毫秒)，0表示不超时");
DEFINE_int32(metrics_port, 0, "Prometheus指标HTTP端口，0表示禁用");
DEFINE_int64(cache_ttl_ms, 0, "缓存条目TTL(毫秒)，0=永不过期；>0时回源写入的缓存将在TTL过后自动过期，作为Invalidate广播失败的兜底");

// 模拟数据库
std::unordered_map<std::string, std::string> db = {
    {"Tom", "400"},     {"Kerolt", "370"}, {"Jack", "296"}, {"Alice", "320"}, {"Bob", "280"},
    {"Charlie", "410"}, {"Diana", "390"},  {"Eve", "310"},  {"abcde", "789"}, {"hello", "879"}};

// 终止信号函数，按下ctrl+c将中断节点服务
std::function<void(int)> handler_wrapper;
void HandleCtrlC(int signum) { handler_wrapper(signum); }

int main(int argc, char** argv) {
    // 使用gflags接管命令行参数
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    // 初始化日志
    spdlog::set_level(spdlog::level::warn);
    spdlog::set_pattern("[knode][%^%l%$] %v");

    std::string addr = "localhost:" + std::to_string(FLAGS_port); // 拼装端口号
    std::string service_name = "kcache"; // 服务器默认名称
    spdlog::info("[node{}] start at: {}", FLAGS_node, addr);

    try {
        // 创建节点，同时注册到etcd
        ServerOptions opts;
        opts.etcd_endpoints = {FLAGS_etcd_endpoints}; // 承接命令行输入etcd_endpoints
        opts.metrics_port = FLAGS_metrics_port; // Prometheus 指标端口
        auto node = std::make_unique<KCacheServer>(addr, service_name, opts); // 创建节点服务器
        spdlog::info("[node{}] server created successfully", FLAGS_node);

        // 启动节点
        std::thread server_thread{[&] {
            spdlog::info("[node{}] starting service...", FLAGS_node);
            try {
                node->Start(); // 启用gRPC通信
            } catch (const std::exception& e) {
                spdlog::info("[node{}] failed to start service: {}", FLAGS_node, e.what());
                std::exit(1);
            }
        }};

        // 注册 Ctrl+C 信号处理器用来优雅关闭服务
        handler_wrapper = [&](int signal) {
            if (signal == SIGINT) {
                spdlog::info("[node{}] received Ctrl+C signal, shutting down service...", FLAGS_node);
                if (node) {
                    node->Stop();
                }
                spdlog::info("[node{}] service stopped", FLAGS_node);
                // 不直接 exit，而是要等其他工作线程完成清理工作
            }
        };
        signal(SIGINT, HandleCtrlC);

        std::this_thread::sleep_for(std::chrono::seconds(5));  // 等待服务器启动

        // 创建缓存组（可配置 getter 超时时间）
        auto& group = MakeCacheGroup(FLAGS_group, 2 << 20,
            [&](const std::string& key) -> ByteViewOptional {
            if (db.find(key) != db.end()) {
                spdlog::info(">_< search [{}] from db\n", key);
                return ByteView{db[key]};
            }
            spdlog::info(">_< Uh oh, there is not found [{}]\n", key);
            return std::nullopt;
            },
            nullptr,                                    // fallback（无降级函数）
            CircuitBreakerConfig{},                     // 熔断器默认配置
            FLAGS_getter_timeout_ms,                    // getter 超时时间
            FLAGS_cache_ttl_ms);                        // 缓存条目 TTL

        spdlog::info("[node{}] service running, press Ctrl+C to exit...", FLAGS_node);

        // 等待服务器线程
        if (server_thread.joinable()) {
            server_thread.join();
        }

    } catch (const std::exception& e) {
        spdlog::error("[node{}] exception occurred: {}", FLAGS_node, e.what());
        std::exit(1);
    }

    return 0;
}