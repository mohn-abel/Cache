#ifndef LRU_H_
#define LRU_H_

#include <chrono>
#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace kcache {
// 万能的数据打包盒，即"值"
struct ByteView {
    std::vector<char> data_{}; // 创建一个存储char型的向量，能存一切二进制文件
    // 构造函数
    ByteView(const std::string& str) {
        data_.resize(str.size()); // 扩大向量至与输入的字符串一样大
        std::copy(str.begin(), str.end(), data_.begin()); // 将字符串挨个拷贝进向量中
    }
    // 获取字符串长度，C++11的写法，等同于int64_t Len() const
    auto Len() const -> int64_t { return data_.size(); }
    // 转换为文本，写法等同于std::string ToString() const
    auto ToString() const -> std::string { return std::string(data_.begin(), data_.end()); }
};

using ByteViewOptional = std::optional<ByteView>; // 薛定谔的盒子：其中能装的类别就是ByteView结构体类型的数据包
// 档案袋，双向链表存储缓存实际的键值对信息
struct Entry {
    std::string key_; // 键
    ByteView value_; // 值
    // 过期时间戳（毫秒，基于 steady_clock）。0 表示永不过期
    int64_t expire_at_ms_ = 0;
    // 构造函数
    // std::move直接将变量k底层的内存控制权给key_，缩短赋值时间
    Entry(std::string k, const ByteView& v) : key_(std::move(k)), value_(v) {}

    // 带 TTL 的构造函数：now_ms + ttl_ms 计算出过期时间
    Entry(std::string k, const ByteView& v, int64_t now_ms, int64_t ttl_ms)
        : key_(std::move(k)), value_(v),
          expire_at_ms_(ttl_ms > 0 ? now_ms + ttl_ms : 0) {}

    // 重载运算符==：判断两个档案袋是不是一致的
    auto operator==(const Entry& entry) const -> bool {
        // key与value必须一一对应，c++不知道怎么对比两个自定义的Entry
        return key_ == entry.key_ && value_.ToString() == entry.value_.ToString();
    }
};
// LRU缓存类
class LRUCache {
    // std::function,C++11引入，函数也可以像变量一样被存起来
    using EvictedFunc = std::function<void(std::string, ByteView)>;
    // 迭代器
    using ListElementIter = std::list<Entry>::iterator;

public:
    // 构造函数
    LRUCache(int64_t max_bytes, const EvictedFunc& evicted_func = nullptr)
        : max_bytes_(max_bytes), evicted_func_(evicted_func) {}

    auto Get(const std::string& key) -> ByteViewOptional; // 取
    void Set(const std::string& key, const ByteView& value); // 存（无 TTL）
    void Set(const std::string& key, const ByteView& value, int64_t ttl_ms); // 存（带 TTL，毫秒）
    void Delete(const std::string& key); // 删
    void RemoveOldest(); // 淘汰

    // 指标访问器
    int64_t Bytes() const { return bytes_; }
    int64_t MaxBytes() const { return max_bytes_; }
    int64_t Count() const; // 缓存条目数（需加锁）

private:
    // 获取当前单调时钟毫秒值
    static int64_t NowMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    int64_t bytes_ = 0; // 当前缓存已使用了多少字节
    int64_t max_bytes_; // 给定的内存上限
    EvictedFunc evicted_func_; // 回调函数，触发淘汰时的信号
    // 两大数据结构，双向链表+哈希表
    std::unordered_map<std::string, ListElementIter> cache_; // 哈希表，存储键(key)-值(迭代器，Entry在链表中的位置)
    std::list<Entry> list_; // 双向链表，节点是Entry，节点中存储key-value对
    mutable std::mutex mtx_;
};

}  // namespace kcache

#endif /* LRU_H_ */