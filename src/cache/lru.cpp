#include "kcache/cache.h"

#include <mutex>

namespace kcache {
// 函数的返回值是一个装有kcache::ByteView数据类型的盒子
auto LRUCache::Get(const std::string& key) -> ByteViewOptional {
    std::unique_lock lock{mtx_}; 
    // 哈希表查找
    if (cache_.find(key) == cache_.end()) {
        return std::nullopt; // 未找到返回空盒子
    }
    auto ele = cache_[key]; // 由哈希表获取键值对在链表中的位置
    // 先拷贝值，避免 erase(ele) 销毁 Entry 后引用悬空
    auto value = ele->value_;
    auto expire_at = ele->expire_at_ms_;

    // TTL 过期检查：如果设置了过期时间且已经过期，则删除该条目并返回空
    if (expire_at != 0 && NowMs() >= expire_at) {
        bytes_ -= ele->key_.size() + value.Len();
        list_.erase(ele);
        cache_.erase(key);
        // 过期的删除不触发淘汰回调（非容量淘汰，而是生命周期到期）
        return std::nullopt;
    }

    list_.erase(ele); // 删除原位置上的元素
    list_.emplace_front(key, value); // 在头部填入该缓存对
    // 保留原有的 TTL（emplace_front 调用的是无 TTL 构造函数，需手动恢复）
    if (expire_at != 0) {
        list_.front().expire_at_ms_ = expire_at;
    }
    cache_[key] = list_.begin(); // 更新哈希表中键值对在链表中的位置
    return value;
}
// 存
void LRUCache::Set(const std::string& key, const ByteView& value) {
    Set(key, value, 0);  // 默认无 TTL，委托给带 TTL 的重载
}

void LRUCache::Set(const std::string& key, const ByteView& value, int64_t ttl_ms) {
    std::unique_lock lock{mtx_}; 
    // 更新现有缓存
    if (cache_.find(key) != cache_.end()) {
        // remove old
        auto ele = cache_[key]; // 获取该缓存在链表中的位置
        bytes_ += value.Len() - ele->value_.Len(); // 计算已使用缓存大小
        list_.erase(ele);
    } else {
        bytes_ += key.size() + value.Len(); // 计算已使用缓存大小
    }
    // insert new（带 TTL 信息）
    int64_t now = NowMs();
    list_.emplace_front(key, value, now, ttl_ms); // 将该缓存移至链表头部，这行代码相当于直接将缓存对存入了
    cache_[key] = list_.begin(); // 更新节点在链表中的位置

    // 当 LRUCache 中还有缓存时，如果此时 LRUCache 中的容量超过规定大小，就不断将最久未使用的缓存淘汰
    while (max_bytes_ != 0 && bytes_ > max_bytes_ && !list_.empty()) {
        RemoveOldest();
    }
}
// 手动指定删除
void LRUCache::Delete(const std::string& key) {
    std::unique_lock lock{mtx_}; 
    // 未找到缓存
    if (cache_.find(key) == cache_.end()) {
        return;
    }
    auto elem_iter = cache_[key]; // 获取位置
    auto value = elem_iter->value_; // 直接取 value 字段
    list_.erase(elem_iter); // 链表中移除该节点
    cache_.erase(key); // 哈希表中移除该映射
    bytes_ -= key.size() + value.Len(); // 更新已用缓存
    // 触发报警，当前环境可能用不上，但是在跨系统时的报警对于数据保存是十分重要的
    // 例如，缓存溢出了，淘汰的数据实际上还有用，将其留个备份淘汰至垃圾桶中，不至于一被淘汰数据就直接丢失
    if (evicted_func_) {
        evicted_func_(key, value);
    }
}
// 淘汰机制
void LRUCache::RemoveOldest() {
    if (list_.empty()) {
        return;
    }
    // 先拷贝键值再弹出，避免结构化绑定引用在 pop_back() 后悬空
    auto& back_entry = list_.back();
    std::string key = back_entry.key_;
    ByteView value = back_entry.value_;
    cache_.erase(key);
    list_.pop_back();
    bytes_ -= key.size() + value.Len();
    // 报警
    if (evicted_func_) {
        evicted_func_(key, value);
    }
}

// 缓存条目数
int64_t LRUCache::Count() const {
    std::lock_guard lock{mtx_};
    return static_cast<int64_t>(cache_.size());
}

}  // namespace kcache
