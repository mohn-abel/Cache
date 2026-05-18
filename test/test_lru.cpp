#include <gtest/gtest.h> // google_test测试工具

#include <chrono>
#include <optional> // C++17新特性：薛定谔的盒子
#include <string>
#include <thread>
#include <vector>

#include "kcache/cache.h"
// 基础功能测试
TEST(LRUCacheTest, TestGet) {
    // 以前查缓存查不到通常会返回NULL或者-1，容易出现bug
    // C++17新特性:std::optional就像一个盲盒，查数据都会返回一个盲盒
    // 没查到则返回std::nullopt
    // 查到了就可以调用ret.value()返回其中数据

    kcache::LRUCache cache{100, nullptr}; // 新建LRU缓存对象
    auto ret = cache.Get("1");  // 取Key值为1的数据
    // EXPECT_EQ：EXPECT期望值ret是否EQ(=)std::nullopt
    EXPECT_EQ(ret, std::nullopt); // 由于只创建了内存容量为100的LRU缓存，但是并没有填数据，所以期望应当是空

    cache.Set("abcdefg", kcache::ByteView{"abcdefg"}); // 存键值对abcdefg-abcdefg
    ret = cache.Get("abcdefg"); // 取缓存对
    // EXPECT_NE：EXPECT期望值ret是否NE(!=)std::nullopt
    EXPECT_NE(ret, std::nullopt); // 判断是否填入数据
    EXPECT_EQ(ret.value().ToString(), "abcdefg"); // 判断缓存是否命中

    // 同上
    cache.Set("11", kcache::ByteView{"22"}); 
    ret = cache.Get("11"); 
    EXPECT_NE(ret, std::nullopt);
    EXPECT_EQ(ret.value().ToString(), "22");
    // 同上
    cache.Set("123456789", kcache::ByteView{"123456789"});
    ret = cache.Get("123456789");
    EXPECT_NE(ret, std::nullopt);
    EXPECT_EQ(ret.value().ToString(), "123456789");
}
// LRU淘汰机制：淘汰最久未使用
TEST(LRUCacheTest, TestRemoveOldest) {
    kcache::LRUCache cache{40, nullptr}; // 创建容量大小为40字节的LRU缓存对象
    // 塞入总计40字节的键值对
    cache.Set("12345", kcache::ByteView{"abcde"}); // 最老节点
    cache.Set("67890", kcache::ByteView{"fghij"});
    cache.Set("xxxxx", kcache::ByteView{"11111"});
    cache.Set("yyyyy", kcache::ByteView{"22222"}); // 最新节点

    // 这个时候应该已经满了
    // 再加入新的缓存，原来最旧的缓存 {"12345", "abcde"}会被淘汰
    cache.Set("zzzzz", kcache::ByteView{"33333"});
    // 检验12345是否还存在
    auto ret = cache.Get("12345");
    EXPECT_EQ(ret, std::nullopt);
    // 检验是否多淘汰了节点
    ret = cache.Get("67890");
    EXPECT_EQ(ret.value().ToString(), "fghij");
}

TEST(LRUCacheTest, TestEvictedFunc) {
    std::vector<kcache::Entry> kvs;
    auto evicted_func = [&](std::string key, const kcache::ByteView& value) {
        std::cout << "test evicted function...\n";
        kvs.emplace_back(kcache::Entry{key, value});
    };
    kcache::LRUCache cache{10, evicted_func};

    // 容量只有10，也就是在完成下面四次插入后，key1 和 k2 会被淘汰
    cache.Set("key1", kcache::ByteView{"123456"});
    cache.Set("k2", kcache::ByteView{"v2"});
    cache.Set("k3", kcache::ByteView{"v3"});
    cache.Set("k4", kcache::ByteView{"v4"});

    std::vector<kcache::Entry> expected{{"key1", kcache::ByteView{"123456"}}, {"k2", kcache::ByteView{"v2"}}};
    EXPECT_EQ(kvs, expected);
}

// TTL 过期机制：条目在指定时间后自动失效
TEST(LRUCacheTest, TestTTLExpiry) {
    kcache::LRUCache cache{100, nullptr};

    // 写入带 50ms TTL 的条目
    cache.Set("ttl_key", kcache::ByteView{"ttl_value"}, 50);
    auto ret = cache.Get("ttl_key");
    EXPECT_NE(ret, std::nullopt);
    EXPECT_EQ(ret.value().ToString(), "ttl_value");

    // 等待 60ms，确保过期
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // 再次读取，应返回空（TTL 已过期，自动清除）
    ret = cache.Get("ttl_key");
    EXPECT_EQ(ret, std::nullopt);
    // Count 应为 0（过期条目已被 Get 懒惰删除）
    EXPECT_EQ(cache.Count(), 0);
}

// TTL 过期不影响无 TTL 的条目
TEST(LRUCacheTest, TestTTLNoExpiryForZeroTTL) {
    kcache::LRUCache cache{100, nullptr};

    // 无 TTL 写入（默认 TTL=0）
    cache.Set("permanent", kcache::ByteView{"forever"});
    // 带 TTL 写入
    cache.Set("ephemeral", kcache::ByteView{"temp"}, 30);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto ret1 = cache.Get("permanent");
    EXPECT_NE(ret1, std::nullopt);
    EXPECT_EQ(ret1.value().ToString(), "forever");

    auto ret2 = cache.Get("ephemeral");
    EXPECT_EQ(ret2, std::nullopt); // 应已过期
}
