#include "kcache/registry.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <unistd.h>

#include <chrono>

#include <fmt/core.h>
#include <spdlog/spdlog.h>
#include <etcd/KeepAlive.hpp>
#include <etcd/v3/Transaction.hpp>

namespace kcache {
// 执行注册
bool EtcdRegistry::Register(const std::string& svc_name, std::string addr) {
    std::string local_ip = GetLocalIP(); // 获取本机真实物理IP
    if (local_ip.empty()) {
        spdlog::error("Failed to get local IP");
        return false;
    }
    // 补全监听地址：如果用户传进来的是 ":8080"，我们帮他拼成 "192.168.1.10:8080"
    if (!addr.empty() && addr[0] == ':') {
        addr = local_ip + addr;
    }
    // 拼接 etcd 的最终 Key，比如 "/services/kcache/192.168.1.10:8080"
    key_ = "/services/" + svc_name + "/" + addr;

    // 创建租约，有效期是10秒，意味着如果10秒内没有续约则代表服务器下线
    auto lease_resp = etcd_client_->leasegrant(10).get();
    if (!lease_resp.is_ok()) {
        spdlog::error("Failed to create lease: {}", lease_resp.error_message());
        return false;
    }
    lease_id_ = lease_resp.value().lease(); // 拿到 etcd 颁发的租约 ID

    // 注册服务
    auto is_ok = etcd_client_->put(key_, addr, lease_id_).get(); // 将注册的服务器key_与租约绑定
    if (!is_ok.is_ok()) {
        spdlog::error("Failed to register [{}] to etcd: {}", key_, is_ok.error_message());
        return false;
    }

    // 启动续约线程
    keepalive_thread_ = std::thread{[this] { this->KeepAliveLoop(); }};
    spdlog::info("Etcd Service registered: {}", key_);
    return true;
}
// 注销
void EtcdRegistry::Unregister() {
    is_stop_ = true;
    if (keepalive_thread_.joinable()) {
        keepalive_thread_.join(); // 等待心跳线程完成
    }
    // 主动销毁租约
    if (lease_id_ > 0) {
        auto is_ok = etcd_client_->leaserevoke(lease_id_).wait();
        if (!is_ok) {
            throw std::runtime_error(fmt::format("[kcache] Failed to revoke lease: {}", lease_id_));
        } else {
            spdlog::debug("Lease {} revoked successfully", lease_id_);
        }
    }
    spdlog::info("Service unregistered: {}", key_);
}

auto EtcdRegistry::GetLocalIP() -> std::string {
    struct ifaddrs* ifaddr;
    if (getifaddrs(&ifaddr) == -1) {
        return "";
    }
    std::string ip;
    // 遍历所有的网络接口（比如 eth0, wlan0, lo）
    for (struct ifaddrs* ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
        // 只查找IPv4的地址(AF_INET)
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) continue;
        char buf[INET_ADDRSTRLEN];
        void* addr_ptr = &((struct sockaddr_in*)ifa->ifa_addr)->sin_addr;
        // 将二进制IP转换为可读字符
        inet_ntop(AF_INET, addr_ptr, buf, INET_ADDRSTRLEN);
        std::string candidate{buf};
        // 剔除本地回环地址，别人访问本地回环地址是连接不上的
        if (candidate != "127.0.0.1") {
            ip = candidate;
            break;
        }
    }
    // 释放 getifaddrs 分配的内存
    freeifaddrs(ifaddr);
    return ip;
}

void EtcdRegistry::KeepAliveLoop() {
    // 未收到停止信号就一直循环
    while (!is_stop_) {
        etcd::KeepAlive keepalive{*etcd_client_, 10, lease_id_};
        try {
            keepalive.Check();
        } catch (const std::exception& e) {
            spdlog::error("Keepalive exception: {}", e.what());
            keepalive.Cancel();
        } catch (...) {
            spdlog::error("Keepalive unknown exception");
            keepalive.Cancel();
        }

        // 等待间隔（租约时间的1/3）
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    spdlog::debug("KeepAlive loop exited for lease {}", lease_id_);
}

}  // namespace kcache