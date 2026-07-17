#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <unordered_map>

#include "kcache/cache.h"
#include "kcache/circuit_breaker.h"
#include "kcache/group.h"

using namespace kcache;

// ─── CircuitBreaker 单元测试 ───────────────────────────────────────────────

// 初始状态为 Closed，Allow() 始终返回 true
TEST(CircuitBreakerTest, InitialStateClosed) {
    CircuitBreaker cb("test_init");
    EXPECT_EQ(cb.StateName(), "Closed");
    EXPECT_TRUE(cb.Allow());
}

// 连续失败达到阈值后进入 Open 状态
TEST(CircuitBreakerTest, OpensAfterFailureThreshold) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 3;
    CircuitBreaker cb("test_open", cfg);

    cb.RecordFailure();
    cb.RecordFailure();
    EXPECT_EQ(cb.StateName(), "Closed");  // 未达阈值

    cb.RecordFailure();
    EXPECT_EQ(cb.StateName(), "Open");    // 达到阈值，熔断
    EXPECT_FALSE(cb.Allow());
}

// Open 状态超过恢复时间后，Allow() 返回 true 并切换到 HalfOpen
TEST(CircuitBreakerTest, TransitionsToHalfOpenAfterTimeout) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 1;
    cfg.recovery_timeout_ms = 50;  // 50ms 恢复
    CircuitBreaker cb("test_halfopen", cfg);

    cb.RecordFailure();
    EXPECT_EQ(cb.StateName(), "Open");
    EXPECT_FALSE(cb.Allow());

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    EXPECT_TRUE(cb.Allow());  // 超时后放行探测请求
    EXPECT_EQ(cb.StateName(), "HalfOpen");
}

// HalfOpen 状态下成功足够次数后恢复 Closed
TEST(CircuitBreakerTest, RecoversToClosed) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 1;
    cfg.recovery_timeout_ms = 50;
    cfg.success_threshold = 2;
    cfg.half_open_max_calls = 3;
    CircuitBreaker cb("test_recover", cfg);

    cb.RecordFailure();
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    cb.Allow();  // 触发切换到 HalfOpen

    cb.RecordSuccess();
    EXPECT_EQ(cb.StateName(), "HalfOpen");  // 还差一次
    cb.RecordSuccess();
    EXPECT_EQ(cb.StateName(), "Closed");    // 恢复
}

// HalfOpen 状态下探测失败，重新回到 Open
TEST(CircuitBreakerTest, HalfOpenFailureReopens) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 1;
    cfg.recovery_timeout_ms = 50;
    CircuitBreaker cb("test_reopen", cfg);

    cb.RecordFailure();
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    cb.Allow();  // 切换到 HalfOpen

    cb.RecordFailure();  // 探测失败
    EXPECT_EQ(cb.StateName(), "Open");
}

// 失败-成功交替模式下，成功不应清零失败计数，熔断应最终触发
TEST(CircuitBreakerTest, FailureCountSurvivesSuccess) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 5;
    cfg.failure_reset_timeout_ms = 60000;  // 大窗口，保证测试期间不过期
    CircuitBreaker cb("test_fail_survives", cfg);

    // 模式: F, F, F, F, S → 4次失败，1次成功（旧实现在此清零，新实现保留）
    cb.RecordFailure();
    cb.RecordFailure();
    cb.RecordFailure();
    cb.RecordFailure();
    EXPECT_EQ(cb.StateName(), "Closed");  // 未达阈值5

    cb.RecordSuccess();
    EXPECT_EQ(cb.StateName(), "Closed");  // 成功不清零，仍 Closed

    // 再失败1次，累计达到5，触发熔断
    cb.RecordFailure();
    EXPECT_EQ(cb.StateName(), "Open");  // 累计5次失败触发熔断
}

// 失败窗口过期后，下一次成功才清零
TEST(CircuitBreakerTest, FailureCountResetsAfterWindowExpiry) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 5;
    cfg.failure_reset_timeout_ms = 80;  // 80ms 窗口
    CircuitBreaker cb("test_window_expire", cfg);

    cb.RecordFailure();
    cb.RecordFailure();
    cb.RecordFailure();
    EXPECT_EQ(cb.StateName(), "Closed");

    // 等待窗口过期
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 窗口过期后成功，清零计数
    cb.RecordSuccess();
    EXPECT_EQ(cb.StateName(), "Closed");

    // 清零后再失败，从头计数
    cb.RecordFailure();
    cb.RecordFailure();
    EXPECT_EQ(cb.StateName(), "Closed");  // 只累加到2
}

// 模拟客户端 per-node 熔断器行为
TEST(ClientCircuitBreakerTest, PerNodeBreakerIndependent) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 3;
    
    // 模拟两个节点的独立熔断器
    CircuitBreaker breaker_a("client:nodeA", cfg);
    CircuitBreaker breaker_b("client:nodeB", cfg);
    
    // nodeA 连续失败熔断
    for (int i = 0; i < 3; ++i) breaker_a.RecordFailure();
    EXPECT_EQ(breaker_a.StateName(), "Open");
    
    // nodeB 不受影响
    EXPECT_EQ(breaker_b.StateName(), "Closed");
    EXPECT_TRUE(breaker_b.Allow());
    
    // nodeA 熔断后拒绝请求
    EXPECT_FALSE(breaker_a.Allow());
}

// ─── KCacheGroup 熔断+降级集成测试 ────────────────────────────────────────

class CircuitBreakerGroupTest : public ::testing::Test {
protected:
    void SetUp() override {
        db_ = {{"k1", "v1"}, {"k2", "v2"}};
        getter_call_count_ = 0;
        fallback_call_count_ = 0;
        getter_healthy_ = true;
        getter_throws_ = false;
    }

    // 模拟可控健康状态的 getter
    // - getter_healthy_ = false: 返回 nullopt（模拟后端数据不存在，不是故障）
    // - getter_throws_ = true:  抛出异常（模拟真正的数据库故障：连接失败、超时等）
    DataGetter MakeGetter() {
        return [this](const std::string& key) -> ByteViewOptional {
            ++getter_call_count_;
            if (getter_throws_) throw std::runtime_error("mock db connection failure");
            if (!getter_healthy_) return std::nullopt;
            auto it = db_.find(key);
            if (it != db_.end()) return ByteView{it->second};
            return std::nullopt;
        };
    }

    // 降级 getter，返回固定兜底值
    FallbackGetter MakeFallback() {
        return [this](const std::string& key) -> ByteViewOptional {
            ++fallback_call_count_;
            return ByteView{"fallback_value"};
        };
    }

    std::unordered_map<std::string, std::string> db_;
    std::atomic_int getter_call_count_{0};
    std::atomic_int fallback_call_count_{0};
    bool getter_healthy_{true};
    bool getter_throws_{false};
};

// getter 正常时，熔断器保持 Closed，正常返回数据
TEST_F(CircuitBreakerGroupTest, NormalOperationNoBreakerTrip) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 3;
    KCacheGroup group("grp_normal", 1024, MakeGetter(), MakeFallback(), cfg);

    auto r = group.Get("k1");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ToString(), "v1");
    EXPECT_EQ(group.CircuitBreakerState(), "Closed");
    EXPECT_EQ(fallback_call_count_.load(), 0);
}

// getter 连续抛异常达到阈值后，熔断器打开，后续请求走 fallback getter
TEST_F(CircuitBreakerGroupTest, BreakerOpensAndFallbackActivates) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 3;
    cfg.recovery_timeout_ms = 60000;  // 不自动恢复
    getter_throws_ = true;  // 用异常模拟数据库连接失败，这才是真故障

    KCacheGroup group("grp_trip", 1024, MakeGetter(), MakeFallback(), cfg, 3000, 0);

    // 触发 3 次失败，打开熔断器
    for (int i = 0; i < 3; ++i) {
        auto r = group.Get("k1");
        // 无 stale cache，走 fallback getter
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->ToString(), "fallback_value");
    }

    EXPECT_EQ(group.CircuitBreakerState(), "Open");

    // 熔断后继续请求，直接走 fallback，不再调用 getter
    int getter_calls_before = getter_call_count_.load();
    auto r = group.Get("k1");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ToString(), "fallback_value");
    EXPECT_EQ(getter_call_count_.load(), getter_calls_before);  // getter 未被调用
}

// getter 先成功（写入 stale cache），后抛异常时，降级返回 stale 数据
TEST_F(CircuitBreakerGroupTest, StaleDataServedWhenGetterFails) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 3;
    cfg.recovery_timeout_ms = 60000;

    // cache_ttl_ms=30ms：主缓存过期后强制走回源；stale_ttl_ms=0：stale 不过期。
    KCacheGroup group("grp_stale", 1024, MakeGetter(), nullptr, cfg, 3000, 0, 30, 0);

    // 先成功获取，写入 stale cache
    auto r1 = group.Get("k1");
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->ToString(), "v1");

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // 模拟后端故障（抛异常 = 数据库连接失败）
    getter_throws_ = true;

    // 连续失败触发熔断
    for (int i = 0; i < 3; ++i) {
        group.Get("k1");
    }
    EXPECT_EQ(group.CircuitBreakerState(), "Open");

    // 熔断后，stale cache 兜底
    auto r2 = group.Get("k1");
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->ToString(), "v1");  // 来自 stale cache
}

// stale cache 有独立 TTL，超过兜底窗口后不再返回旧值
TEST_F(CircuitBreakerGroupTest, StaleDataExpiresAfterStaleTTL) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 1;
    cfg.recovery_timeout_ms = 60000;

    // 主缓存 20ms 过期，stale cache 40ms 过期。
    KCacheGroup group("grp_stale_ttl", 1024, MakeGetter(), nullptr, cfg, 3000, 0, 20, 40);

    auto r1 = group.Get("k1");
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->ToString(), "v1");

    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    getter_throws_ = true;
    auto r2 = group.Get("k1");
    EXPECT_FALSE(r2.has_value());
}

// 熔断恢复：Open -> HalfOpen -> Closed
TEST_F(CircuitBreakerGroupTest, BreakerRecovery) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 2;
    cfg.recovery_timeout_ms = 80;
    cfg.success_threshold = 1;
    cfg.half_open_max_calls = 2;
    getter_throws_ = true;  // 用异常模拟真故障

    KCacheGroup group("grp_recovery", 1024, MakeGetter(), MakeFallback(), cfg, 3000, 0);

    // 触发熔断
    group.Get("k1");
    group.Get("k1");
    EXPECT_EQ(group.CircuitBreakerState(), "Open");

    // 等待恢复超时
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 恢复后端
    getter_throws_ = false;
    getter_healthy_ = true;
    // 探测请求成功，熔断器关闭
    group.Get("k1");
    EXPECT_EQ(group.CircuitBreakerState(), "Closed");
}

// SingleFlight 冷却期拒绝新回源时，应走降级但不能被熔断器当作成功。
// 尤其在 HalfOpen 探测阶段，冷却拒绝不是后端恢复信号，不能误关闭熔断器。
TEST_F(CircuitBreakerGroupTest, SingleFlightCooldownRejectedDoesNotCloseHalfOpenBreaker) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 1;
    cfg.recovery_timeout_ms = 30;
    cfg.success_threshold = 1;
    cfg.half_open_max_calls = 1;
    getter_throws_ = true;

    KCacheGroup group("grp_cooldown_halfopen", 1024, MakeGetter(), MakeFallback(),
                      cfg, 3000, 300);

    auto r1 = group.Get("k1");
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->ToString(), "fallback_value");
    EXPECT_EQ(group.CircuitBreakerState(), "Open");

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto r2 = group.Get("k1");
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->ToString(), "fallback_value");
    EXPECT_EQ(group.CircuitBreakerState(), "HalfOpen");
}

// ─── 安全性测试：合法 nullopt 不触发熔断（防恶意攻击） ────────────────

// 核心安全修复验证：getter 正常返回 nullopt（数据库中不存在该 key）
// 不应触发熔断器的 RecordFailure()，因为这不是数据库故障。
// 恶意用户无法通过大量不重复的 key 瘫痪正常节点。
TEST_F(CircuitBreakerGroupTest, NotFoundKeyDoesNotTripBreaker) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 3;  // 阈值 3，非常低
    cfg.recovery_timeout_ms = 60000;

    // getter_healthy_ 为 true，但 key 在 db_ 中不存在 → 返回 nullopt（正常语义）
    KCacheGroup group("grp_not_found", 1024, MakeGetter(), MakeFallback(), cfg);

    // 用数据库中不存在的 key 连续请求 10 次（远超 failure_threshold）
    for (int i = 0; i < 10; ++i) {
        auto r = group.Get("key_not_in_db");
        ASSERT_TRUE(r.has_value());  // fallback 兜底
        EXPECT_EQ(r->ToString(), "fallback_value");
    }

    // 关键断言：熔断器必须保持 Closed，不能因为 key 不存在而误触发
    EXPECT_EQ(group.CircuitBreakerState(), "Closed");
    // fallback 被调用了（降级在 Load() 中 nullopt 时总会走），但熔断器未记录失败
    EXPECT_GT(fallback_call_count_.load(), 0);
    // circuit_breaks 计数器应为 0
    EXPECT_EQ(group.GetStatus().circuit_breaks.load(), 0);
}

// getter 抛异常（真故障）才触发熔断，与 nullopt 截然不同
TEST_F(CircuitBreakerGroupTest, ExceptionTripsBreakerNotFoundDoesNot) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 3;
    cfg.recovery_timeout_ms = 60000;

    // 第一阶段：返回 nullopt（key 不存在），不应触发熔断
    getter_healthy_ = true;
    getter_throws_ = false;
    KCacheGroup group("grp_compare", 1024, MakeGetter(), MakeFallback(), cfg, 3000, 0);

    for (int i = 0; i < 5; ++i) {
        group.Get("key_not_in_db");
    }
    EXPECT_EQ(group.CircuitBreakerState(), "Closed");  // 仍然 Closed

    // 第二阶段：getter 开始抛异常（模拟数据库宕机），应触发熔断
    getter_throws_ = true;
    for (int i = 0; i < 3; ++i) {
        group.Get("key_not_in_db");
    }
    EXPECT_EQ(group.CircuitBreakerState(), "Open");  // 现在才打开
}

// ─── Getter 超时控制测试 ─────────────────────────────────────────────

// 慢 getter 超时后走降级
TEST_F(CircuitBreakerGroupTest, GetterTimeoutTriggersFallback) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 10;  // 高阈值，确保不会先触发熔断

    // 创建一个会阻塞 200ms 的慢 getter。
    // 这里不捕获测试 fixture：当前实现超时后会让 getter 在线程中继续跑完，
    // 如果捕获 this，测试函数提前结束后后台线程可能访问已销毁的 fixture。
    DataGetter slow_getter = [](const std::string& key) -> ByteViewOptional {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (key == "k1") return ByteView{"late_value"};
        return std::nullopt;
    };

    FallbackGetter fallback = [](const std::string& key) -> ByteViewOptional {
        return ByteView{"fallback"};
    };

    // 设置超时为 50ms，getter 需要 200ms，必定超时
    KCacheGroup group("grp_timeout", 1024, slow_getter, fallback, cfg, 50);

    auto start = std::chrono::steady_clock::now();
    auto r = group.Get("k1");
    auto elapsed = std::chrono::steady_clock::now() - start;

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ToString(), "fallback");  // 超时后走降级
    // 旧 std::async 实现会在 future 析构时等慢 getter 结束，实际耗时接近 200ms；
    // 修复后请求线程应在 50ms 超时附近返回，给一些调度余量但必须明显小于 200ms。
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 150);
    EXPECT_EQ(group.GetStatus().getter_timeouts.load(), 1);
}

// 超时为 0 时不做超时控制，等待 getter 完成
TEST_F(CircuitBreakerGroupTest, ZeroTimeoutNoLimit) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 10;

    DataGetter slow_getter = [this](const std::string& key) -> ByteViewOptional {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        auto it = db_.find(key);
        if (it != db_.end()) return ByteView{it->second};
        return std::nullopt;
    };

    KCacheGroup group("grp_no_timeout", 1024, slow_getter, nullptr, cfg, 0);
    auto r = group.Get("k1");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ToString(), "v1");  // 不超时，正常返回
}

// 连续超时触发熔断
TEST_F(CircuitBreakerGroupTest, RepeatedTimeoutOpensBreaker) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 3;

    DataGetter slow_getter = [](const std::string& key) -> ByteViewOptional {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        return std::nullopt;
    };

    FallbackGetter fallback = [](const std::string& key) -> ByteViewOptional {
        return ByteView{"fallback"};
    };

    // 超时 50ms，getter 需要 500ms，每次都超时
    // fail_cooldown_ms = 0：禁用冷却期，允许同一 key 连续重试以触发熔断
    KCacheGroup group("grp_timeout_breaker", 1024, slow_getter, fallback, cfg, 50, 0);

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < 3; ++i) {
        auto r = group.Get("k1");
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->ToString(), "fallback");
    }
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(group.CircuitBreakerState(), "Open");  // 超时3次触发熔断
    // 旧 std::async 实现会被三个 500ms getter 拖到约 1500ms；
    // 修复后应接近 3 * 50ms 返回，这里保留调度余量。
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 700);
    EXPECT_EQ(group.GetStatus().getter_timeouts.load(), 3);
}

// 同一 key 的先锋线程超时只应计一次熔断失败，等待线程共享失败结果但不重复累计 breaker
TEST_F(CircuitBreakerGroupTest, WaiterTimeoutsDoNotMultiplyBreakerFailures) {
    CircuitBreakerConfig cfg;
    cfg.failure_threshold = 2;  // 若等待线程重复计数，本次并发会直接把 breaker 打开
    cfg.recovery_timeout_ms = 60000;

    DataGetter slow_getter = [](const std::string& key) -> ByteViewOptional {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        return ByteView{"late_value"};
    };

    FallbackGetter fallback = [](const std::string& key) -> ByteViewOptional {
        return ByteView{"fallback"};
    };

    KCacheGroup group("grp_waiter_timeout_breaker", 1024, slow_getter, fallback, cfg, 50, 0);

    constexpr int kThreads = 6;
    std::vector<std::thread> ths;
    std::vector<bool> ok(kThreads, false);
    ths.reserve(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        ths.emplace_back([&, i] {
            auto r = group.Get("hot_key");
            ok[i] = r.has_value() && r->ToString() == "fallback";
        });
    }
    for (auto& t : ths) t.join();

    for (bool v : ok) EXPECT_TRUE(v);
    EXPECT_EQ(group.GetStatus().getter_timeouts.load(), 1);
    EXPECT_EQ(group.CircuitBreakerState(), "Closed");
}
