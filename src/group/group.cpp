#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>

#include <fmt/base.h>
#include <spdlog/spdlog.h>

#include "kcache/cache.h"
#include "kcache/group.h"

// 为何需要缓存分组
// 1.命名隔离,处理同ID不同组别的情况
// 2.资源隔离,内存资源按需分配,不同的种类的值占用的内存大小不一致
// 3.数据库隔离,不同种类的数据可能分别存放在不同种类的数据库
// 4.风险隔离,防止某组数据遭遇攻击而瘫痪其他组别
namespace kcache {
// 全局哈希表,用于存放缓存组名字与具体缓存组的映射
std::unordered_map<std::string, KCacheGroup> cache_groups;
// 全局读写锁：GetCacheGroup / GetAllGroupNames 只需读锁，MakeCacheGroup 需要写锁
std::shared_mutex mtx;
// 创建缓存组
auto MakeCacheGroup(const std::string& name, int64_t bytes, DataGetter getter,
                    FallbackGetter fallback,
                    CircuitBreakerConfig cb_cfg,
                    int64_t getter_timeout_ms,
                    int64_t cache_ttl_ms,
                    int64_t stale_ttl_ms) -> KCacheGroup& {
    // 安全检查:缺少数据库查询函数直接退出
    if (getter == nullptr) {
        spdlog::critical("no getter function!");
        std::exit(1);
    }
    std::unique_lock lock{mtx};  // 写锁
    // 原地构建一个group临时对象,并使用move将所有权移交给哈希表
    cache_groups[name] = std::move(KCacheGroup{name, bytes, getter, fallback, cb_cfg,
                                               getter_timeout_ms, 1000, cache_ttl_ms,
                                               stale_ttl_ms});
    return cache_groups[name];
}
// 获取缓存组
auto GetCacheGroup(const std::string& name) -> KCacheGroup* {
    std::shared_lock lock{mtx};  // 读锁，多线程可并发进入
    auto it = cache_groups.find(name);
    if (it == cache_groups.end()) {
        return nullptr;
    }
    return &it->second;
}

std::vector<std::string> GetAllGroupNames() {
    std::shared_lock lock{mtx};  // 读锁
    std::vector<std::string> names;
    names.reserve(cache_groups.size());
    for (const auto& [name, _] : cache_groups) {
        names.push_back(name);
    }
    return names;
}

auto KCacheGroup::Get(const std::string& key) -> ByteViewOptional {
    // 拦截器:缓存组关闭,或请求为空,直接拒绝请求并存入日志
    if (is_close_) {
        spdlog::error("Cache group [{}] is closed!!!", name_);
        return std::nullopt;
    }

    if (key == "") {
        spdlog::warn("The key [{}] is empty, you can't get a empty key from cache group", key);
        return std::nullopt;
    }

    // 先从本地缓存中获取
    auto ret = cache_->Get(key);
    if (ret) {
        ++status_.local_hits;  // 本地命中缓存次数+1
        return ret;
    }

    ++status_.local_misses;  // 本地未命中缓存次数+1
    return Load(key); // 缓存未命中,所以调用Load查询本地数据库
}

bool KCacheGroup::Set(const std::string& key, ByteView b) {
    // 拦截器
    if (is_close_) {
        spdlog::error("Cache group [{}] is closed!!!", name_);
        return false;
    }
    if (key.empty()) {
        spdlog::warn("The key [{}] is empty, you can't set it into cache group", key);
        return false;
    }
    // 主动写入应同步刷新 stale，避免后续降级路径返回旧值。
    cache_->Set(key, b);
    if (stale_ttl_ms_ > 0) {
        stale_cache_->Set(key, b, stale_ttl_ms_);
    } else {
        stale_cache_->Set(key, b);
    }
    spdlog::debug("key:{} is set value:{}", key, b.ToString());
    return true;
}

bool KCacheGroup::Delete(const std::string& key) {
    if (is_close_) {
        spdlog::error("Cache group [{}] is closed!!!", name_);
        return false;
    }
    if (key.empty()) {
        spdlog::warn("The key [{}] is empty, you can't delete it from cache group", key);
        return false;
    }
    cache_->Delete(key);
    stale_cache_->Delete(key);
    spdlog::debug("key:{} is deleted", key);
    return true;
}
// 处理来自其他节点的数据失效
// 场景:游戏服务器中有三个节点ABC,张三登录时访问节点A,节点A查询张三的游戏数据发现其有100游戏币
// 张三进行了充值,充值请求被发送到了节点B,节点B将数据库中的游戏币修改为400,张三进行界面刷新,又回到了节点A,节点A访问本节点缓存为100游戏币
// 此时就出现了数据混乱,所以必须在其他节点修改数据库中数据后,将所有节点的该数据无效化,因为缓存的优先级是高于数据库的,会优先访问缓存
// 避免导致后续的数据错乱
bool KCacheGroup::InvalidateFromPeer(const std::string& key) {
    if (is_close_) {
        spdlog::error("Cache group [{}] is closed!!!", name_);
        return false;
    }
    if (key.empty()) {
        spdlog::warn("The key [{}] is empty, you can't invalidate it from cache group", key);
        return false;
    }

    // 来自其他节点的失效请求，删除 fresh 和 stale，避免降级路径返回旧副本。
    cache_->Delete(key);
    stale_cache_->Delete(key);
    spdlog::debug("Invalidated key [{}] from local cache (from peer)", key);
    return true;
}
// 降级
auto KCacheGroup::Fallback(const std::string& key) -> ByteViewOptional {
    // 优先返回 stale cache（过期但有效的旧数据）
    auto stale = stale_cache_->Get(key);
    if (stale) {
        ++status_.fallback_hits;
        spdlog::warn("[Fallback:{}] key={} served from stale cache", name_, key);
        return stale;
    }
    // 其次调用用户提供的 fallback getter
    if (fallback_) {
        auto val = fallback_(key);
        if (val) {
            ++status_.fallback_hits;
            spdlog::warn("[Fallback:{}] key={} served from fallback getter", name_, key);
        }
        return val;
    }
    spdlog::error("[Fallback:{}] key={} no fallback available", name_, key);
    return std::nullopt;
}

auto KCacheGroup::Load(const std::string& key) -> ByteViewOptional {
    // 熔断器检查：Open 状态直接走降级
    if (!breaker_->Allow()) {
        ++status_.circuit_breaks;
        spdlog::warn("[CircuitBreaker:{}] Open, key={} -> fallback", name_, key);
        return Fallback(key);
    }

    // singleflight 的作用是让同一个 key 的并发回源只发生一次：
    // - 第一个进入的线程是“先锋线程”，真正执行 LoadData(key)；
    // - 后续进入的线程是“等待线程”，复用先锋线程的结果。
    //
    // 这里把 getter_timeout_ms_ 也传给 singleflight，目的是避免先锋线程卡住时，
    // 后续等待线程永久阻塞在 shared_future::get() 上。
    //
    // getter_timeout_ms_ <= 0 时表示不限制等待时间，保持向后兼容；
    // getter_timeout_ms_ > 0 时，等待线程最多等待这个时长，超时后返回 nullopt，
    // 下面会统一记录失败并走 Fallback(key) 降级。
    auto wait_timeout = getter_timeout_ms_ > 0
                            ? std::chrono::milliseconds(getter_timeout_ms_)
                            : std::chrono::milliseconds::zero();
    // singleFlight失败冷却期，在这期间不允许创建已失败键的先锋线程了
    auto cooldown = fail_cooldown_ms_ > 0
                            ? std::chrono::milliseconds(fail_cooldown_ms_)
                            : std::chrono::milliseconds::zero();
    // 回源计时：每进入一次真实回源都记一次 loads，并累计耗时（纳秒），
    // 供 kcache_loads / kcache_avg_load_duration_ms 指标使用。
    auto load_start = std::chrono::steady_clock::now();
    auto ret = loader_.Do(key, [&] { return LoadData(key); }, wait_timeout, cooldown);
    auto load_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::steady_clock::now() - load_start)
                       .count();
    ++status_.loads;
    status_.load_duration += load_ns;
    // 1.真故障处理
    if (ret.is_error) {
        if (ret.should_trip_breaker) {
            breaker_->RecordFailure();
        }
        spdlog::error("Failed to load data for key: {}", key);
        // getter 真故障（超时/异常/数据库连接失败），走降级
        return Fallback(key);
    }
    // 2.正常处理
    breaker_->RecordSuccess();

    if(!ret.data){
        // key 不存在：回源未命中，走降级。
        // 注意：local_misses 已在 Get() 命中失败时计过一次，这里不再重复自增，
        // 否则会把同一次未命中重复计数、污染 hit_ratio。
        spdlog::warn("Data for key [{}] not found in local getter", key);
        return Fallback(key); // key 不存在也应走降级，但不触发熔断
    }
    // 回源命中：统一在此记入 loader_hits（区别于 local_hits 本地缓存命中）
    ++status_.loader_hits;
    // 成功获取数据，存入 fresh 和 stale 后返回。
    if (stale_ttl_ms_ > 0) {
        stale_cache_->Set(key, ret.data.value(), stale_ttl_ms_);
    } else {
        stale_cache_->Set(key, ret.data.value());
    }
    if(cache_ttl_ms_ > 0){
        cache_->Set(key, ret.data.value(), cache_ttl_ms_);
    } else {
        cache_->Set(key, ret.data.value());
    }
    return ret.data;
}
// 从本地数据库查询（带超时控制）
auto KCacheGroup::LoadData(const std::string& key) -> SingleFlightResult {
    spdlog::info("Try to load key [{}] from local", key);

    // 超时为 0 表示不做超时控制（向后兼容）
    if (getter_timeout_ms_ <= 0) {
        try {
            auto val = getter_(key);
            if (!val) {
                // getter 正常返回但未找到数据：NOT_FOUND，不是错误
                return {std::nullopt, false, SingleFlightErrorSource::None, false}; // 未找到，非异常
            }
            // 回源命中由上层 Load() 统一记入 loader_hits，这里不再误记为 local_hits
            return {val, false, SingleFlightErrorSource::None, false};
        } catch (...) {
            // getter 抛异常：真正的数据库故障
            ++status_.loader_errors;
            spdlog::error("[{}] getter exception for key={}", name_, key);
            return {std::nullopt, true, SingleFlightErrorSource::PioneerFailure, true};
        }
    }

    // 这里不能继续使用 std::async(std::launch::async) 做超时兜底：
    // 当 wait_for(timeout) 超时后，如果 future 离开作用域，标准库通常会在
    // future 析构时等待 async 任务结束。这样表面上 wait_for 返回了 timeout，
    // 实际调用线程仍会被慢 getter 拖住，达不到"按超时时间返回"的目的。
    //
    // 因此这里改为 promise + detached thread：
    // - 当前线程只等待 promise 对应的 future，到 timeout 就返回 nullopt；
    // - getter 在线程中继续跑，结束后把结果或异常写入 promise；
    // - promise 用 shared_ptr 捕获，保证当前函数超时返回后，后台线程仍能安全写结果；
    // - 线程只捕获 getter 的拷贝和 key 的拷贝，不捕获 this，避免 KCacheGroup 生命周期结束后
    //   后台线程继续访问已销毁对象。
    //
    // 注意：detach 不能强制杀死已经卡住的 getter，只是保证请求线程不再被它阻塞。
    // 真正的资源取消仍应由底层 DB/RPC/HTTP 客户端的 deadline/cancel 机制完成。
    auto prom = std::make_shared<std::promise<SingleFlightResult>>();
    auto fut = prom->get_future();
    auto getter = getter_;
    auto key_copy = key;
    
    try {
        std::thread([prom, getter = std::move(getter), key_copy = std::move(key_copy)]() mutable {
            try {
                auto val = getter(key_copy);
                // getter 正常完成时，把结果交给等待中的 LoadData()。
                // 如果 LoadData() 已经因为超时返回，promise 仍然可以安全接收这个结果；
                // 只是结果不会再被当前请求使用。
                prom->set_value({val, false, SingleFlightErrorSource::None, false});
            } catch (...) {
                // getter 抛出的异常不能跨线程自动传播，必须写入 promise。
                // 如果主线程仍在等待，fut.get() 会重新抛出；如果已经超时返回，
                // 这里记录异常到 shared state 后随 promise 生命周期自然释放。
                prom->set_exception(std::current_exception());
            }
        }).detach();
    } catch (const std::system_error& e) {
        spdlog::error("[{}] failed to start getter worker for key={}: {}", name_, key, e.what());
        return {std::nullopt, true, SingleFlightErrorSource::PioneerFailure, true};
    }

    // 只在当前请求线程等待 timeout 时长；后台 getter 若更慢，会继续在 detached thread 中运行。
    auto timeout = std::chrono::milliseconds(getter_timeout_ms_);
    if (fut.wait_for(timeout) == std::future_status::ready) {
        // getter 在超时前完成：取出结果。
        try {
            auto result = fut.get();
            if (!result.data) {
                // getter 正常返回 nullopt：数据不存在，不是错误
                return {std::nullopt, false, SingleFlightErrorSource::None, false};
            }
            // 回源命中由上层 Load() 统一记入 loader_hits，这里不再误记为 local_hits
            return result;
        } catch (...) {
            // getter 在 detached 线程中抛异常：真正的数据库故障
            ++status_.loader_errors;
            spdlog::error("[{}] getter exception for key={}", name_, key);
            return {std::nullopt, true, SingleFlightErrorSource::PioneerFailure, true};
        }
    }

    // getter 在限定时间内没有返回：这是真正的故障（超时）
    // 上层 Load() 会通过 is_error 判断后调用 RecordFailure() 触发熔断统计，
    // 并走 Fallback() 降级；
    // 同时 SingleFlight::Do() 会把 nullopt 广播给仍在等待该 key 的线程。
    ++status_.getter_timeouts;
    spdlog::warn("[{}] getter timeout for key={} (timeout={}ms)", name_, key, getter_timeout_ms_);
    return {std::nullopt, true, SingleFlightErrorSource::PioneerFailure, true};
}

}  // namespace kcache
