#ifndef REGISTRY_H_
#define REGISTRY_H_

#include <atomic>
#include <etcd/Client.hpp>
#include <memory>
#include <thread>

namespace kcache {

class EtcdRegistry {
public:
    // 构造函数：etcd 的默认客户端端口
    EtcdRegistry(const std::string& endpoints = "http://127.0.0.1:2379") {
        etcd_client_ = std::make_unique<etcd::Client>(endpoints);
    }
    ~EtcdRegistry() = default;

    // 将服务名和地址写入 etcd，格式为 /services/{svc_name}/{addr}，并绑定一个租约（lease）。
    // 这样其他服务可以通过 etcd 查询到所有可用节点。
    bool Register(const std::string& svc_name, std::string addr);

    // 撤销租约并删除服务信息，确保节点下线时不会被其他服务继续发现。
    void Unregister();

private:
    // 辅助函数：启动时没有指定IP，自动获取本机的网卡IP
    auto GetLocalIP() -> std::string;

    void KeepAliveLoop(); // 后台心跳循环

    int64_t lease_id_{0}; // 租约ID
    std::unique_ptr<etcd::Client> etcd_client_; // 独占的etcd进程指针
    std::string key_;
    std::thread keepalive_thread_; // 后台心跳进程
    std::atomic<bool> is_stop_{false}; // 停机标志
};

}  // namespace kcache

#endif