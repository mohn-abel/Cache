#ifndef CACHE_H_
#define CACHE_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <chrono>

#include "kcache/cache.h"
#include "kcache/circuit_breaker.h"
#include "kcache/singleflight.h"

namespace kcache {
// 封装函数为变量，该函数要求满足返回值是ByteViewOptional，输入参数是key，即数据库访问函数
using DataGetter = std::function<ByteViewOptional(const std::string& key)>;
// 降级数据函数
using FallbackGetter = std::function<ByteViewOptional(const std::string& key)>;

struct GroupStatus {
    std::atomic_int64_t loads{0};            // 加载次数
    std::atomic_int64_t local_hits{0};       // 本地缓存命中次数
    std::atomic_int64_t local_misses{0};     // 本地缓存未命中次数
    std::atomic_int64_t peer_hits{0};        // 从对等节点获取成功次数
    std::atomic_int64_t peer_misses{0};      // 从对等节点获取失败次数
    std::atomic_int64_t loader_hits{0};      // 从加载器获取成功次数
    std::atomic_int64_t loader_errors{0};    // 从加载器获取失败次数
    std::atomic_int64_t load_duration{0};    // 加载总耗时（纳秒）
    std::atomic_int64_t circuit_breaks{0};   // 熔断触发次数
    std::atomic_int64_t fallback_hits{0};    // 降级命中次数（stale/fallback）
    std::atomic_int64_t getter_timeouts{0};  // getter 超时次数
};
// 缓存状态同步的操作指令
enum class SyncFlag {
    SET,
    DELETE,
    INVALIDATE,  // 缓存失效，只删除本地缓存，不通过getter重新加载
};

class KCacheGroup {
public:
    KCacheGroup() = default; 
    // 利用make_unique动态分配LRUCache内存空间
    KCacheGroup(std::string name, int64_t bytes, DataGetter getter,
                FallbackGetter fallback = nullptr,
                CircuitBreakerConfig cb_cfg = {},
                int64_t getter_timeout_ms = 3000,
                int64_t fail_cooldown_ms = 1000,
                int64_t cache_ttl_ms = 0,
                int64_t stale_ttl_ms = 0)
        : cache_(std::make_unique<LRUCache>(bytes)), 
          stale_cache_(std::make_unique<LRUCache>(bytes)),
          name_(name), getter_(getter),
          fallback_(fallback),
          breaker_(std::make_unique<CircuitBreaker>(name, cb_cfg)),
          getter_timeout_ms_(getter_timeout_ms),
          fail_cooldown_ms_(fail_cooldown_ms),
          cache_ttl_ms_(cache_ttl_ms),
          stale_ttl_ms_(stale_ttl_ms)
        {}
    // KCacheGroup是一个非常庞大的资源管理器，不是一个简单的数值
    // 禁用拷贝，内部unique_ptr以及singleflight有锁都是不支持拷贝赋值的
    KCacheGroup(const KCacheGroup&) = delete;
    auto operator=(const KCacheGroup& other) -> KCacheGroup& = delete;

    // 既然不支持拷贝赋值，所以将其所有权移交给新对象
    // 允许将一个缓存组的所有权移交给新对象，只转移指针而不拷贝数据
    KCacheGroup(KCacheGroup&& other) {
        cache_ = std::move(other.cache_);
        stale_cache_ = std::move(other.stale_cache_);
        name_ = std::move(other.name_);
        getter_ = std::move(other.getter_);
        fallback_ = std::move(other.fallback_);
        breaker_ = std::move(other.breaker_);
        getter_timeout_ms_ = other.getter_timeout_ms_;
        fail_cooldown_ms_ = other.fail_cooldown_ms_;
        cache_ttl_ms_ = other.cache_ttl_ms_;
        stale_ttl_ms_ = other.stale_ttl_ms_;
        is_close_ = other.is_close_.load();
        // 转移原子量统计数据
        status_.loads.store(other.status_.loads.load());
        status_.local_hits.store(other.status_.local_hits.load());
        status_.local_misses.store(other.status_.local_misses.load());
        status_.peer_hits.store(other.status_.peer_hits.load());
        status_.peer_misses.store(other.status_.peer_misses.load());
        status_.loader_hits.store(other.status_.loader_hits.load());
        status_.loader_errors.store(other.status_.loader_errors.load());
        status_.load_duration.store(other.status_.load_duration.load());
        status_.circuit_breaks.store(other.status_.circuit_breaks.load());
        status_.fallback_hits.store(other.status_.fallback_hits.load());
        status_.getter_timeouts.store(other.status_.getter_timeouts.load());
        // loader_ 内含 mutex，不可移动，保持默认构造
    }
    // 对=进行重载
    auto operator=(KCacheGroup&& other) -> KCacheGroup& {
        cache_ = std::move(other.cache_);
        stale_cache_ = std::move(other.stale_cache_);
        name_ = std::move(other.name_);
        getter_ = std::move(other.getter_);
        fallback_ = std::move(other.fallback_);
        breaker_ = std::move(other.breaker_);
        getter_timeout_ms_ = other.getter_timeout_ms_;
        fail_cooldown_ms_ = other.fail_cooldown_ms_;
        cache_ttl_ms_ = other.cache_ttl_ms_;
        stale_ttl_ms_ = other.stale_ttl_ms_;
        is_close_ = other.is_close_.load();
        // 转移原子量统计数据
        status_.loads.store(other.status_.loads.load());
        status_.local_hits.store(other.status_.local_hits.load());
        status_.local_misses.store(other.status_.local_misses.load());
        status_.peer_hits.store(other.status_.peer_hits.load());
        status_.peer_misses.store(other.status_.peer_misses.load());
        status_.loader_hits.store(other.status_.loader_hits.load());
        status_.loader_errors.store(other.status_.loader_errors.load());
        status_.load_duration.store(other.status_.load_duration.load());
        status_.circuit_breaks.store(other.status_.circuit_breaks.load());
        status_.fallback_hits.store(other.status_.fallback_hits.load());
        status_.getter_timeouts.store(other.status_.getter_timeouts.load());
        return *this; // 允许连等操作
    }
    // 通过这个入口查询数据，查缓存->防击穿->查分布式节点->查本地数据库
    auto Get(const std::string& key) -> ByteViewOptional;
    // 主动写入缓存组
    bool Set(const std::string& key, ByteView b);
    // 主动删除数据
    bool Delete(const std::string& key);

    // 处理来自其他节点的失效请求
    bool InvalidateFromPeer(const std::string& key);

    // 获取熔断器当前状态名
    std::string CircuitBreakerState() const { return breaker_->StateName(); }

    // 指标访问器
    const std::string& GetName() const { return name_; }
    const GroupStatus& GetStatus() const { return status_; }
    int64_t CacheBytes() const { return cache_->Bytes(); }
    int64_t CacheMaxBytes() const { return cache_->MaxBytes(); }
    int64_t CacheCount() const { return cache_->Count(); }

private:
    auto Load(const std::string& key) -> ByteViewOptional;
    auto LoadData(const std::string& key) -> SingleFlightResult;

private:
    auto Fallback(const std::string& key) ->ByteViewOptional;

private:
    std::unique_ptr<LRUCache> cache_; // 使用智能指针自动管理内存释放，并且在拷贝和移动时更方便
    std::unique_ptr<LRUCache> stale_cache_; // 过期缓存
    std::string name_; // 缓存组的标识符
    std::atomic<bool> is_close_{false}; // 缓存组是否开启
    DataGetter getter_; // 缓存未命中时备用数据库查询函数
    FallbackGetter fallback_; // 熔断降级函数
    std::unique_ptr<CircuitBreaker> breaker_; // 熔断器
    SingleFlight loader_; // 防击穿,确保同一个key查询时只有一个线程去查询
    GroupStatus status_; // 记录该组运行状态
    int64_t getter_timeout_ms_{3000}; // getter 超时时间
    int64_t fail_cooldown_ms_{1000};  // SingleFlight 失败冷却期（ms）
    int64_t cache_ttl_ms_{0};         // 缓存条目 TTL（ms），0 = 永不过期
    int64_t stale_ttl_ms_{0};         // stale 缓存 TTL（ms），0 = 永不过期
};


// 为什么要设计组:在大型服务器中,缓存的数据种类不止一种,不同种类占用的容量不一致,所以可以通过MakeCacheGroup创建不同的缓存组
// 全局函数
auto MakeCacheGroup(const std::string& name, int64_t bytes, DataGetter getter,
                    FallbackGetter fallback = nullptr,
                    CircuitBreakerConfig cb_cfg = {},
                    int64_t getter_timeout_ms = 3000,
                    int64_t cache_ttl_ms = 0,
                    int64_t stale_ttl_ms = 0) -> KCacheGroup&;
auto GetCacheGroup(const std::string& name) -> KCacheGroup*; // 获取指定缓存组的实例指针
std::vector<std::string> GetAllGroupNames(); // 获取所有缓存组名称

}  // namespace kcache

#endif /* CACHE_H_ */
