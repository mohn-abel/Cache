// 通过线程安全的缓存机制 ，确保对同一个键（key）的多次并发请求只会执行一次实际操作，其他请求会等待并复用结果
// 解决缓存击穿问题
#ifndef SINGLEFLIGHT_H_
#define SINGLEFLIGHT_H_

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "kcache/cache.h"

namespace kcache {

struct SingleFlightResult {
    std::optional<ByteView> data = std::nullopt;
    bool is_error = false; // 记录是否发生真正的底层故障
};

class SingleFlight {
    using Result = SingleFlightResult;
    // 将函数封装为变量作为参数传入给另一个函数，只要函数返回值是Result，不需要参数，就可以作为变量输入Do函数中，
    using Func = std::function<Result()>;

public:
    // 对外入口：
    // - 同一个 key 第一次进入的线程会成为"先锋线程"，执行 func()；
    // - 同一个 key 后续进入的线程会成为"等待线程"，等待并复用先锋线程的结果。
    //
    // wait_timeout 只限制"等待线程"等待已有调用的时间：
    // - wait_timeout <= 0：保持旧行为，等待线程一直等到先锋线程完成；
    // - wait_timeout > 0：等待线程最多等待该时长，超时后返回 nullopt，让上层走降级。
    //
    // 注意：wait_timeout 不会中断正在执行 func() 的先锋线程。
    // 先锋线程要想按时退出，需要 func() 内部自己实现超时或取消逻辑。
    //
    // ★ 失败冷却期 (fail_cooldown_ms)：
    // - 当先锋线程因超时或异常返回 nullopt 后，该 key 会被加入"失败冷却名单"；
    // - 冷却期内，任何新的请求都不会为同一 key 创建新的先锋线程，直接返回 nullopt；
    // - 目的是防止同一个 key 在熔断器尚未 Open 的短暂窗口内反复创建新的
    //   detach 线程，导致线程泄漏。配合熔断器形成双层保护：
    //   (1) 冷却期阻止同一 key 重复成为先锋线程；
    //   (2) 熔断器在全局层面阻止所有 key 进入回源。
    Result Do(const std::string& key, Func func,
              std::chrono::milliseconds wait_timeout = std::chrono::milliseconds::zero(),
              std::chrono::milliseconds fail_cooldown = std::chrono::milliseconds{1000}) {
        std::unique_lock<std::mutex> glock(mtx_); // 保护 map_

        // ★ 失败冷却期检查：
        // 如果该 key 最近一次回源失败（先锋线程超时或异常），且在冷却期内，
        // 则拒绝创建新的先锋线程，直接返回 nullopt。
        // 配合熔断器形成双层保护:
        //   (1) 冷却期阻止同一 key 重复成为先锋线程，防止 detach 线程泄漏；
        //   (2) 熔断器在全局层面阻止所有 key 进入回源。
        if (fail_cooldown.count() > 0) {
            auto now = std::chrono::steady_clock::now();
            if (auto fit = failed_keys_.find(key); fit != failed_keys_.end()) {
                if (now - fit->second < fail_cooldown) {
                    glock.unlock();
                    return {std::nullopt, false}; // 冷却期内，直接降级
                }
                failed_keys_.erase(fit); // 冷却期已过，允许重试
            }
        }

        // 等待线程路径：map_ 中已经有同 key 调用，说明已有先锋线程正在回源。
        // 这里复制 shared_ptr 后立即释放全局锁，避免等待该 key 的结果时阻塞其他 key。
        if (auto it = map_.find(key); it != map_.end()) {
            auto existing_call = it->second;
            glock.unlock();
            return WaitForResult(key, existing_call, wait_timeout);
        }

        // 先锋线程路径：当前 key 还没有进行中的调用。
        // 先把 Call 放入 map_，后续同 key 请求才能找到它并等待同一个 future。
        auto new_call = std::make_shared<Call>();
        map_[key] = new_call;
        glock.unlock();

        // RAII 清理 guard：
        // 无论 func() 正常返回还是抛异常，只要先锋线程走出 Do()，都要清理 map_ 中的记录。
        //
        // 这里必须比较 it->second == new_call 后再删除：
        // 如果等待线程已经超时并调用 Forget(key, new_call)，后续请求可能会为同一个 key
        // 创建一个新的 Call。旧先锋线程较晚结束时，只能清理自己的旧 Call，
        // 不能误删后来创建的新 Call。
        auto cleanup = [this, key, new_call]() {
            std::lock_guard<std::mutex> lock(mtx_);
            auto it = map_.find(key);
            if (it != map_.end() && it->second == new_call) {
                map_.erase(it);
            }
        };
        // 利用 shared_ptr 自定义 deleter 模拟 finally 语义，确保 cleanup 在作用域结束时一定执行。
        std::shared_ptr<void> guard(nullptr, [cleanup](void*) { cleanup(); });

        try {
            Result val = func();
            // 先锋线程正常完成：把结果写入 promise，所有正在等待 fut 的线程都会被唤醒。
            // val 可以是 nullopt，表示回源失败；上层会根据 nullopt 走 fallback。
            // ★ 安全修复：不再对 clean nullopt 调用 MarkFailed。
            // LoadData 已经通过 is_error 出参区分"数据不存在"与"真故障"，
            // clean nullopt 是正常的业务语义（key 不存在），不应触发冷却期。
            // 只有下面 catch 块中的异常才代表真正的回源故障，需要冷却。
            new_call->prom.set_value(val);
            return val;
        } catch (...) {
            // 先锋线程执行 func() 抛异常时，也必须通知等待线程。
            // 如果不 set_exception，等待线程会一直卡在 fut.get()/wait_for() 关联的 shared state 上。
            //
            // 等待线程拿到异常后会在 WaitForResult() 中统一转换为 nullopt，
            // 让上层 Load() 继续按"回源失败"处理。
            MarkFailed(key); // ★ getter 抛异常 → 加入冷却名单
            new_call->prom.set_exception(std::current_exception());
            return {std::nullopt, true}; // 统一返回 nullopt，上层 Load() 走熔断 + Fallback
        }
    }

private:
    struct Call {
        std::promise<Result> prom;          // 先锋线程的结果广播器
        std::shared_future<Result> fut = prom.get_future().share(); // 收音机线程的结果接收器
    };
    // 等待线程读取先锋线程结果。
    Result WaitForResult(const std::string& key, const std::shared_ptr<Call>& call,
                         std::chrono::milliseconds wait_timeout) {
        try {
            // 设置了等待超时，且先锋线程没有在指定时间内完成：
            // 当前等待线程主动放弃复用这次 singleflight，返回 nullopt 给上层降级。
            //
            // Forget() 会把旧 Call 从 map_ 中移除，使后续请求有机会重新成为先锋线程。
            // 这会牺牲“同一 key 永远只回源一次”的严格合并，但可以避免某个卡住的先锋线程
            // 把同 key 的后续请求全部永久拖住。
            if (wait_timeout.count() > 0 &&
                call->fut.wait_for(wait_timeout) != std::future_status::ready) {
                Forget(key, call);
                return {std::nullopt, true}; // 超时属于error
            }

            // 没有设置等待超时，或先锋线程已经完成：读取共享结果。
            // 如果先锋线程 set_exception，这里会抛出，并在 catch 中转换为 nullopt。
            return call->fut.get();
        } catch (...) {
            // 先锋线程失败（func 抛异常或上层超时返回 nullopt），收音机线程统一返回 nullopt，
            // 由上层 Load() 走熔断 + Fallback 降级。
            return {std::nullopt, true}; // 异常属于error
        }
    }

    // 等待线程超时后遗忘旧调用。
    // 只有 map_ 中保存的还是当前 call 时才删除；如果旧调用已经被清理，
    // 或者后续请求已经创建了新的 Call，这里不能破坏新的调用关系。
    void Forget(const std::string& key, const std::shared_ptr<Call>& call) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = map_.find(key);
        if (it != map_.end() && it->second == call) {
            map_.erase(it);
        }
    }

    // ★ 标记 key 回源失败，将其加入冷却名单。
    // 冷却期内同一 key 的新请求不会再创建先锋线程。
    void MarkFailed(const std::string& key) {
        // 调用方已持有 mtx_，无需再次加锁
        failed_keys_[key] = std::chrono::steady_clock::now();
    }

    std::mutex mtx_;
    std::unordered_map<std::string, std::shared_ptr<Call>> map_; // key → 进行中的调用

    // ★ 失败冷却名单：记录每个 key 最近一次失败的时间。
    // key 一旦失败，在 fail_cooldown 时间内不再成为先锋线程。
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> failed_keys_;
};

}  // namespace kcache

#endif /* SINGLEFLIGHT_H_ */
