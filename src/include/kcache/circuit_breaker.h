#ifndef CIRCUIT_BREAKER_H_
#define CIRCUIT_BREAKER_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

#include <spdlog/spdlog.h>

namespace kcache
{

    enum class CircuitState{
        Closed, // 正常，允许请求通过
        Open,   // 熔断，拒绝请求
        HalfOpen, // 半开，允许部分请求
    };

struct CircuitBreakerConfig {
    // 触发熔断的失败次数阈值（在滑动窗口内累计）
    int64_t failure_threshold = 5;
    // 熔断后多久进入 HalfOpen 状态（毫秒）
    int64_t recovery_timeout_ms = 5000;
    // HalfOpen 状态下允许通过的探测请求数
    int64_t half_open_max_calls = 2;
    // HalfOpen 状态下成功多少次后关闭熔断
    int64_t success_threshold = 2;
    // 失败计数器重置时间窗口（毫秒）：
    // - 在此窗口内发生的失败会被累积计数，一次成功不会清零
    // - 超过此窗口没有新失败后，下一次成功会重置计数器
    // - 解决 "失败-成功-失败-成功" 交替模式下熔断失效的问题
    int64_t failure_reset_timeout_ms = 10000;
};

// 熔断类
class CircuitBreaker {
public:
    // 禁止隐式转换
    explicit CircuitBreaker(std::string name, CircuitBreakerConfig cfg = {})
        : name_(std::move(name)), cfg_(cfg) {}

    // 是否允许请求通过，处理三种状态流转
    bool Allow() {
        auto state = state_.load();
        // 未触发熔断
        if (state == CircuitState::Closed) {
            return true;
        }
        // 触发熔断
        if (state == CircuitState::Open) {
            // 检查是否到了恢复时间
            auto now = NowMs();
            // 检查是否超过恢复时间
            if (now - open_time_ms_.load() >= cfg_.recovery_timeout_ms) {
                // 尝试切换到 HalfOpen
                CircuitState expected = CircuitState::Open;
                // 原子操作切换状态
                if (state_.compare_exchange_strong(expected, CircuitState::HalfOpen)) {
                    // 重置计数
                    half_open_calls_.store(0);
                    half_open_successes_.store(0);
                    spdlog::warn("[CircuitBreaker:{}] -> HalfOpen", name_);
                }
                return true;
            }
            return false;
        }
        // HalfOpen：限制探测请求数
        if (state == CircuitState::HalfOpen) {
            auto calls = half_open_calls_.fetch_add(1);
            return calls < cfg_.half_open_max_calls;
        }
        return false;
    }

    // 记录一次成功
    void RecordSuccess() {
        auto state = state_.load();
        if (state == CircuitState::Closed) {
            // 不在 Closed 状态下简单清零 consecutive_failures_
            // 只有在距离最后一次失败超过 failure_reset_timeout_ms 后才清零
            // 防止 "失败-成功-失败-成功" 交替模式下的熔断失效：
            //   例如: F,F,F,F,S → 此前 consecutive_failures_=4，
            //   如果不重置，后续再失败就会累加到5触发熔断
            auto now = NowMs();
            auto last_fail = last_failure_time_ms_.load();
            if (last_fail == 0 || (now - last_fail) >= cfg_.failure_reset_timeout_ms) {
                consecutive_failures_.store(0);
            }
            return;
        }
        // 半开状态下请求成功
        if (state == CircuitState::HalfOpen) {
            auto succ = half_open_successes_.fetch_add(1) + 1;
            // 检查半开状态下请求成功次数是否抵达恢复阈值
            if (succ >= cfg_.success_threshold) {
                state_.store(CircuitState::Closed);
                // 失败计数清零
                consecutive_failures_.store(0);
                last_failure_time_ms_.store(0);
                spdlog::info("[CircuitBreaker:{}] -> Closed (recovered)", name_);
            }
        }
    }

    // 记录一次失败
    void RecordFailure() {
        auto state = state_.load();
        if (state == CircuitState::HalfOpen) {
            // 探测失败，重新打开熔断
            open_time_ms_.store(NowMs());
            state_.store(CircuitState::Open);
            spdlog::warn("[CircuitBreaker:{}] HalfOpen probe failed -> Open", name_);
            return;
        }
        // 正常请求时失败
        if (state == CircuitState::Closed) {
            auto now = NowMs();
            auto last_fail = last_failure_time_ms_.load();
            // 如果距离上次失败超过重置窗口，说明之前的失败已经"过期"，先清零再计数
            if (last_fail > 0 && (now - last_fail) >= cfg_.failure_reset_timeout_ms) {
                consecutive_failures_.store(0);
            }
            last_failure_time_ms_.store(now);
            auto failures = consecutive_failures_.fetch_add(1) + 1;
            if (failures >= cfg_.failure_threshold) {
                state_.store(CircuitState::Open); // 切换至熔断开状态
                open_time_ms_.store(NowMs());
                spdlog::error("[CircuitBreaker:{}] -> Open (failures={})", name_, failures);
            }
        }
    }

    CircuitState State() const { return state_.load(); }

    std::string StateName() const {
        switch (state_.load()) {
            case CircuitState::Closed:   return "Closed";
            case CircuitState::Open:     return "Open";
            case CircuitState::HalfOpen: return "HalfOpen";
        }
        return "Unknown";
    }

private:
    // 获取当前时间戳
    static int64_t NowMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    std::string name_;
    CircuitBreakerConfig cfg_;

    std::atomic<CircuitState> state_{CircuitState::Closed};
    std::atomic_int64_t consecutive_failures_{0};
    std::atomic_int64_t open_time_ms_{0};
    std::atomic_int64_t half_open_calls_{0};
    std::atomic_int64_t half_open_successes_{0};
    // 记录最近一次失败的时间戳，用于失败计数的时间窗口衰减
    std::atomic_int64_t last_failure_time_ms_{0};
};

} // namespace kcache
#endif