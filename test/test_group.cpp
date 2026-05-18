// SPDX-License-Identifier: MIT
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kcache/cache.h"
#include "kcache/group.h"

using namespace kcache;

class CacheGroupTest : public ::testing::Test {
protected:
    void SetUp() override {
        db_ = {{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}}; // 构建的测试数据库
        call_count_.clear();
        // 数据库查询函数
        getter_ = [this](const std::string& key) -> ByteViewOptional {
            // 统计回源调用次数
            call_count_[key]++;
            auto it = db_.find(key);
            if (it != db_.end()) {
                return ByteView{it->second}; // 数据库中只是字符，所以需要构造函数封装一下
            }
            return std::nullopt;
        };
    }

    std::unordered_map<std::string, std::string> db_;
    std::unordered_map<std::string, int> call_count_; // 数据库查询次数
    DataGetter getter_;
};

// 首次回源成功后，二次 Get 命中本地缓存，不再调用 getter
TEST_F(CacheGroupTest, GetCachesOnHit) {
    KCacheGroup group("group_basic", 1024, getter_); // 构建一个基础缓存组
    // 第一次调用本地缓存为空，去数据库查询
    auto r1 = group.Get("key1");
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->ToString(), "value1");
    EXPECT_EQ(call_count_["key1"], 1);

    // 第二次应命中本地缓存，直接在缓存中拿数据，不再回源
    auto r2 = group.Get("key1");
    ASSERT_TRUE(r2.has_value()); // 断言，失败的话会终结程序
    EXPECT_EQ(r2->ToString(), "value1");
    EXPECT_EQ(call_count_["key1"], 1);
}

// 不存在的 key 返回空值（nullopt）
TEST_F(CacheGroupTest, GetReturnsNulloptForMissing) {
    KCacheGroup group("group_missing", 1024, getter_);
    auto r = group.Get("not_exist");
    EXPECT_FALSE(r.has_value()); // 期望不存在该数据，失败的话会向日志报错
    EXPECT_EQ(call_count_["not_exist"], 1); // 本地数据库查询次数
}

// 先 Set 再 Get，不触发 getter
TEST_F(CacheGroupTest, SetThenGetDoesNotCallGetter) {
    KCacheGroup group("group_set", 1024, getter_);

    EXPECT_TRUE(group.Set("manual", ByteView{"v"}));
    auto r = group.Get("manual");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ToString(), "v");
    EXPECT_EQ(call_count_["manual"], 0); // 未向本地数据库查询
}

// 删除后再次 Get 会触发一次回源
TEST_F(CacheGroupTest, DeleteRemovesCache) {
    KCacheGroup group("group_delete", 1024, getter_);

    ASSERT_TRUE(group.Get("key2").has_value()); // 将key2从数据库提升至缓存
    EXPECT_TRUE(group.Delete("key2")); // 删除该缓存对

    call_count_["key2"] = 0;  // 重置统计
    auto r = group.Get("key2"); // 再次获取该缓存对
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ToString(), "value2");
    EXPECT_EQ(call_count_["key2"], 1);
}

// 调用 InvalidateFromPeer 仅删除本地缓存，下一次 Get 才会回源
TEST_F(CacheGroupTest, InvalidateFromPeerOnlyDeletesLocal) {
    KCacheGroup group("group_invalidate", 1024, getter_);

    ASSERT_TRUE(group.Get("key3").has_value());
    // 模拟来自其他节点的失效
    EXPECT_TRUE(group.InvalidateFromPeer("key3"));

    call_count_["key3"] = 0;
    auto r = group.Get("key3");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ToString(), "value3");
    // 失效后应回源一次
    EXPECT_EQ(call_count_["key3"], 1);
}

// 空 key 在 Get/Set/Delete/InvalidateFromPeer 均返回 false
TEST_F(CacheGroupTest, EmptyKeyIsRejected) {
    KCacheGroup group("group_empty", 1024, getter_);

    EXPECT_FALSE(group.Get("").has_value());
    EXPECT_FALSE(group.Set("", ByteView{"x"}));
    EXPECT_FALSE(group.Delete(""));
    EXPECT_FALSE(group.InvalidateFromPeer(""));
}

// 多线程并发对同一 key 的 Get，只回源一次，其它线程复用结果
TEST_F(CacheGroupTest, SingleFlightAvoidsDuplicateLoads) {
    // getter 增加少量延迟，放大并发窗口
    DataGetter slow_getter = [this](const std::string& key) -> ByteViewOptional {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        call_count_[key]++;
        auto it = db_.find(key);
        if (it != db_.end()) return ByteView{it->second};
        return std::nullopt;
    };

    KCacheGroup group("group_sf", 1024, slow_getter);

    const int N = 16;
    std::vector<std::thread> ths;
    ths.reserve(N);
    std::vector<bool> ok(N, false);

    for (int i = 0; i < N; ++i) {
        ths.emplace_back([&, i] {
            auto r = group.Get("key1");
            ok[i] = r.has_value() && r->ToString() == "value1";
        }); // 创建线程并直接开始工作，lambda中使用[&,i]是为了保证线程跟上循环的速度，避免由于循环过快导致i已经变成10而进程1才刚开始
    }
    for (auto& t : ths) t.join(); // 等待所有16个线程完成

    for (int i = 0; i < N; ++i) EXPECT_TRUE(ok[i]); // 由于存在互斥锁，所以每个线程都成功读取到“value1”
    // 并发同 key 只应回源一次
    EXPECT_EQ(call_count_["key1"], 1);
}

// 先锋线程迟迟不返回时，等待线程应按超时退出，并允许后续请求重新成为先锋线程
TEST(SingleFlightTest, WaiterTimeoutForgetsStalledCall) {
    SingleFlight loader;
    std::atomic_bool release_pioneer{false};
    std::atomic_int pioneer_runs{0};
    std::promise<void> pioneer_started_signal;
    auto pioneer_started = pioneer_started_signal.get_future();
    
    // 【修改 1】：类型改为 SingleFlightResult
    SingleFlightResult pioneer_result;

    std::thread pioneer([&] {
        pioneer_result = loader.Do(
            "hot",
            // 【修改 2】：Lambda 返回值打包为 {data, is_error}
            [&]() -> SingleFlightResult {
                pioneer_runs.fetch_add(1);
                pioneer_started_signal.set_value();
                while (!release_pioneer.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
                return {ByteView{"late_value"}, false}; 
            },
            std::chrono::milliseconds(50));
    });

    if (pioneer_started.wait_for(std::chrono::milliseconds(200)) != std::future_status::ready) {
        release_pioneer.store(true);
        pioneer.join();
        FAIL() << "pioneer call did not start in time";
    }

    // 【修改 3】：类型改为 SingleFlightResult
    SingleFlightResult waiter_result;
    std::chrono::milliseconds waiter_elapsed{0};
    std::atomic_bool waiter_done{false};
    std::thread waiter([&] {
        auto start = std::chrono::steady_clock::now();
        waiter_result = loader.Do(
            "hot",
            // 【修改 4】：Lambda 返回值打包
            []() -> SingleFlightResult {
                return {ByteView{"waiter_should_not_run"}, false};
            },
            std::chrono::milliseconds(50));
        waiter_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
        waiter_done.store(true);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(180));
    if (!waiter_done.load()) {
        release_pioneer.store(true);
        pioneer.join();
        waiter.join();
        FAIL() << "waiter was blocked by the stalled pioneer call";
    }
    waiter.join();

    auto retry_result = loader.Do(
        "hot",
        // 【修改 5】：Lambda 返回值打包
        []() -> SingleFlightResult {
            return {ByteView{"retry_value"}, false};
        },
        std::chrono::milliseconds(50));

    release_pioneer.store(true);
    pioneer.join();

    // 【修改 6】：断言需要访问 .data 字段来判断是否有值
    EXPECT_FALSE(waiter_result.data.has_value());
    EXPECT_TRUE(waiter_result.is_error); // 等待线程超时退出，根据你的设计 is_error 为 true
    EXPECT_LT(waiter_elapsed.count(), 150);
    EXPECT_EQ(pioneer_runs.load(), 1);

    ASSERT_TRUE(retry_result.data.has_value());
    EXPECT_EQ(retry_result.data->ToString(), "retry_value");

    ASSERT_TRUE(pioneer_result.data.has_value());
    EXPECT_EQ(pioneer_result.data->ToString(), "late_value");
}

// 移动构造后，已缓存的值仍可直接命中，不重复回源
TEST_F(CacheGroupTest, MoveConstructorPreservesCache) {
    KCacheGroup g1("group_move_ctor", 1024, getter_);
    ASSERT_TRUE(g1.Get("key1").has_value());

    KCacheGroup g2(std::move(g1)); // 使用move将g1缓存组的所有权直接移交给g2，g1内部成员信息被擦除
    auto r = g2.Get("key1");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ToString(), "value1");
    EXPECT_EQ(call_count_["key1"], 1);  // 未再次回源
}

// 移动赋值后亦保持缓存命中
TEST_F(CacheGroupTest, MoveAssignmentPreservesCache) {
    KCacheGroup g1("group_move_assign_src", 1024, getter_);
    KCacheGroup g2("group_move_assign_dst", 1024, getter_);
    ASSERT_TRUE(g1.Get("key2").has_value());
    // 发生交接的是group这个类的内部成员，如name_，cache_，getter_
    g2 = std::move(g1); // g2这个缓存组替代g1，g2的组名变为了group_move_assign_src，而原来g2的组名销毁，g1为空

    auto r = g2.Get("key2");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ToString(), "value2");
    EXPECT_EQ(call_count_["key2"], 1);
}
// 测试底层数据库数据完整性
TEST_F(CacheGroupTest, BatchGetAcrossKeys) {
    KCacheGroup group("group_batch", 1024, getter_);
    for (const auto& kv : db_) {
        auto r1 = group.Get(kv.first);
        ASSERT_TRUE(r1.has_value());
        EXPECT_EQ(r1->ToString(), kv.second);
        auto r2 = group.Get(kv.first);
        ASSERT_TRUE(r2.has_value());
        EXPECT_EQ(r2->ToString(), kv.second);
        EXPECT_EQ(call_count_[kv.first], 1);
    }
}

// 全局方法测试，测试MakeCacheGroup
TEST(CacheGroupGlobalTest, MakeCacheGroupCreatesUsableGroup) {
    std::unordered_map<std::string, std::string> db = {{"gkey", "gvalue"}};
    auto getter = [&db](const std::string& key) -> ByteViewOptional {
        auto it = db.find(key);
        if (it != db.end()) return ByteView{it->second};
        return std::nullopt;
    };

    auto& group = MakeCacheGroup("global_test_group", 1024, getter); // 构建一个缓存组
    auto r = group.Get("gkey");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->ToString(), "gvalue");
}

TEST(CacheGroupGlobalTest, GetCacheGroupLookup) {
    auto getter = [](const std::string&) -> ByteViewOptional { return std::nullopt; }; // 将getter设置为不返回任何值
    MakeCacheGroup("lookup_group", 512, getter);

    auto* found = GetCacheGroup("lookup_group");
    ASSERT_NE(found, nullptr); // 断言找到的绝不是nullptr

    auto* not_found = GetCacheGroup("not_exist_group"); // 查找一个并没创建的组
    EXPECT_EQ(not_found, nullptr); // 未找到
}

// 不同名称的 group 相互独立，交叉访问失败
TEST(CacheGroupGlobalTest, MultipleNamedGroupsAreIndependent) {
    // 创建两套不同的组
    auto getter1 = [](const std::string& key) -> ByteViewOptional {
        if (key == "k1") return ByteView{"v1"};
        return std::nullopt;
    };
    auto getter2 = [](const std::string& key) -> ByteViewOptional {
        if (key == "k2") return ByteView{"v2"};
        return std::nullopt;
    };

    auto& g1 = MakeCacheGroup("g1", 256, getter1);
    auto& g2 = MakeCacheGroup("g2", 256, getter2);

    auto r1 = g1.Get("k1");
    auto r2 = g2.Get("k2");
    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r1->ToString(), "v1");
    EXPECT_EQ(r2->ToString(), "v2");

    // 交叉访问应失败
    EXPECT_FALSE(g1.Get("k2").has_value());
    EXPECT_FALSE(g2.Get("k1").has_value());
}

// 指标访问器测试
TEST_F(CacheGroupTest, MetricAccessorsWork) {
    KCacheGroup group("group_metrics", 2048, getter_);

    // GetName
    EXPECT_EQ(group.GetName(), "group_metrics");

    // CacheMaxBytes
    EXPECT_EQ(group.CacheMaxBytes(), 2048);

    // 初始状态：缓存为空
    EXPECT_EQ(group.CacheBytes(), 0);
    EXPECT_EQ(group.CacheCount(), 0);

    // Get 后缓存有数据
    auto r = group.Get("key1");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(group.CacheCount(), 1);
    EXPECT_GT(group.CacheBytes(), 0);

    // GetStatus 初始计数器检查
    const auto& s = group.GetStatus();
    EXPECT_EQ(s.local_hits.load(), 1);      // key1 第二次访问才算命中
    // key1 第一次 Get 时本地未命中
    EXPECT_GE(s.local_misses.load(), 1);
}

// GetAllGroupNames 测试
TEST(CacheGroupGlobalTest, GetAllGroupNamesReturnsCreatedGroups) {
    auto getter = [](const std::string&) -> ByteViewOptional { return std::nullopt; };
    MakeCacheGroup("names_test_a", 256, getter);
    MakeCacheGroup("names_test_b", 256, getter);

    auto names = GetAllGroupNames();
    EXPECT_GE(names.size(), 2u);

    // 检查两个名字都在结果中
    bool found_a = false, found_b = false;
    for (const auto& n : names) {
        if (n == "names_test_a") found_a = true;
        if (n == "names_test_b") found_b = true;
    }
    EXPECT_TRUE(found_a);
    EXPECT_TRUE(found_b);
}
