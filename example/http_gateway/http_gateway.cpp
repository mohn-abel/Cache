#include <string>
#include <thread>

#include <gflags/gflags.h>
#include <httplib.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "kcache/client.h"

// 使用gflags定义三个命令行参数，启动程序时可以使用--http_port=8080 --etcd_endpoints="192.168.1.1:2379动态改变配置
DEFINE_int32(http_port, 9000, "HTTP服务端口");
DEFINE_string(etcd_endpoints, "http://127.0.0.1:2379", "etcd地址");
DEFINE_string(service_name, "kcache", "缓存服务名称");

using namespace kcache;

class HttpGateway {
public:
    HttpGateway(int port, const std::string& etcd_addr, const std::string& svc_name) : port_(port) {
        // 初始化内部gRPC客户端，连接到etcd注册中心，拉取所有现存节点，并持续监听节点存活情况
        kcache_client_ = std::make_unique<KCacheClient>(etcd_addr, svc_name); 
        // 限制HTTP请求最大长度为4MB
        server_.set_payload_max_length(4 << 20);
        SetupRoutes(); // 注册路由表
    }

    ~HttpGateway() { server_.stop(); } // 只关闭网关服务端

    void Start() {
        spdlog::info("Starting HTTP Gateway on port {}", port_);
        server_.listen("0.0.0.0", port_); // "0.0.0.0" 代表监听本机的所有网卡，外部机器（前端、用户电脑）才能连得进来
    }

private:
    // 告诉系统什么样的URL应该交给哪个函数去处理
    void SetupRoutes() {
        // GET /api/cache/{group}/{key}
        // 注册服务端的GET请求处理函数，即处理来自用户的读缓存需求
        server_.Get(R"(/api/cache/([^/]+)/([^/]+))",
                    [this](const httplib::Request& req, httplib::Response& res) { HandleGet(req, res); });

        // POST /api/cache/{group}/{key}
        // 注册服务端的POST请求处理函数，
        server_.Post(R"(/api/cache/([^/]+)/([^/]+))",
                     [this](const httplib::Request& req, httplib::Response& res) { HandleSet(req, res); });

        // DELETE /api/cache/{group}/{key}
        // 注册服务端的DELETE请求处理函数
        server_.Delete(R"(/api/cache/([^/]+)/([^/]+))",
                       [this](const httplib::Request& req, httplib::Response& res) { HandleDelete(req, res); });
    }
    // 处理get请求
    // json是C++工业上流行的“快递盒”，别的语言也能够解析json，前端可能不会使用C++语言
    void HandleGet(const httplib::Request& req, httplib::Response& res) {
        std::string group = req.matches[1]; // 请求由group和key组成
        std::string key = req.matches[2];

        auto value = kcache_client_->Get(group, key);
        if (value) {
            nlohmann::json json_resp = {{"key", key}, {"value", *value}, {"group", group}}; // 创建json对象盒子装入缓存信息
            res.set_content(json_resp.dump() + "\n", "application/json"); // dump序列化，压扁缓存包装成一串字符串结果填入HTTP回复(Body)
        } else {
            SendError(res, 404, "Key not found or service unavailable"); // 回复错误
        }
    }
    // 处理set请求
    void HandleSet(const httplib::Request& req, httplib::Response& res) {
        std::string group = req.matches[1];
        std::string key = req.matches[2];

        nlohmann::json body; // 构建一个json盒子装解析HTTP请求后的数据
        // 如果前端传入的不是json文件就直接把纯文本包装在假json中
        try {
            body = nlohmann::json::parse(req.body); // 用户发过来的内容存在 req.body 字符串里，尝试用 parse 把它转成 JSON 对象
        } catch (...) {
            body = {{"value", req.body}}; // 如果发过来的不是json，则套一层{}作为json的格式
        }

        std::string value = body.value("value", ""); 
        if (value.empty()) {
            SendError(res, 400, "Value is required");
            return;
        }
        // 调用客户端写入
        bool success = kcache_client_->Set(group, key, value);
        if (success) {
            nlohmann::json json_resp = {{"key", key}, {"value", value}, {"group", group}, {"success", true}}; // 装载json盒子
            res.set_content(json_resp.dump() + "\n", "application/json"); // 发送json序列化
        } else {
            SendError(res, 500, "Failed to Set value");
        }
    }
    // 处理delete请求
    void HandleDelete(const httplib::Request& req, httplib::Response& res) {
        std::string group = req.matches[1];
        std::string key = req.matches[2];

        bool success = kcache_client_->Delete(group, key);
        if (success) {
            nlohmann::json json_resp = {{"key", key}, {"group", group}, {"deleted", true}};
            res.set_content(json_resp.dump() + "\n", "application/json");
        } else {
            SendError(res, 500, "Failed to delete key");
        }
    }
    // 辅助报错函数
    void SendError(httplib::Response& res, int code, const std::string& message) {
        nlohmann::json error = {{"error", message}, {"code", code}};
        res.status = code;
        res.set_content(error.dump(), "application/json");
    }

private:
    int port_; // 端口号
    httplib::Server server_; // 网关服务端
    std::unique_ptr<KCacheClient> kcache_client_; // 缓存客户端
};

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("[http-gateway][%^%l%$] %v");

    try {
        HttpGateway gateway(FLAGS_http_port, FLAGS_etcd_endpoints, FLAGS_service_name);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        gateway.Start();
    } catch (const std::exception& e) {
        spdlog::error("Gateway failed: {}", e.what());
        return 1;
    }

    return 0;
}
