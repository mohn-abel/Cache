// 绕过 gRPC 框架，直接压测 KCache 核心逻辑的 QPS
//
// 编译：cmake --build build/Release -j$(nproc) --target bench_cache
// 运行：./bin/bench_cache [--threads=4] [--duration_sec=10]
//
// 对比：
//   gRPC 单节点：   ~17,800 QPS  (perf 分析见 docs/perf-analysis-report.md)
//   KCache 裸调：   预期 > 500,000 QPS

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "kcache/cache.h"
#include "kcache/group.h"

DEFINE_int32(threads, 4, "压测线程数");
DEFINE_int32(duration_sec, 5, "每线程运行秒数");
DEFINE_int32(capacity_mb, 64, "缓存容量 (MB)");

using namespace kcache;

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    gflags::SetUsageMessage(
        "KCache 裸调用压测 — 绕过 gRPC，直接测试 LRU + Group 层 QPS\n"
        "用法: ./bin/bench_cache [--threads=4] [--duration_sec=5]");

    const int64_t max_bytes = static_cast<int64_t>(FLAGS_capacity_mb) * 1024 * 1024;

    // ── 1. 创建缓存组（getter 模拟"数据库查询" ──
    auto& group = MakeCacheGroup(
        "bench",
        max_bytes,
        // getter: 模拟回源，返回 "source_value"
        [](const std::string& key) -> ByteViewOptional {
            return ByteView("source_value_for_" + key);
        });

    std::cout << "=== KCache 裸调用压测 (绕过 gRPC) ===\n";
    std::cout << "线程数: " << FLAGS_threads << "\n";
    std::cout << "每线程时长: " << FLAGS_duration_sec << "s\n";
    std::cout << "缓存容量: " << FLAGS_capacity_mb << "MB\n\n";

    // ── 2. 预热：预填充 100 个 key，确保压测走的是缓存命中路径 ──
    const int kPrepopKeys = 100;
    for (int i = 0; i < kPrepopKeys; ++i) {
        std::string key = "key_" + std::to_string(i);
        group.Set(key, ByteView("value_" + std::to_string(i)));
    }
    std::cout << "预热完成: 预填充 " << kPrepopKeys << " 个 key\n\n";

    // ── 3. 多线程压测 ──
    std::atomic<int64_t> total_ops{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> workers;

    auto start = std::chrono::steady_clock::now();

    for (int t = 0; t < FLAGS_threads; ++t) {
        workers.emplace_back([&, t]() {
            int64_t local_ops = 0;
            int key_idx = 0;
            auto thread_start = std::chrono::steady_clock::now();
            auto deadline = thread_start + std::chrono::seconds(FLAGS_duration_sec);

            while (std::chrono::steady_clock::now() < deadline) {
                // 在预热过的 100 个 key 之间轮转
                std::string key = "key_" + std::to_string(key_idx % kPrepopKeys);
                auto val = group.Get(key);  // ← 核心被测函数

                // 编译屏障: 阻止编译器优化掉 Get() 调用
                asm volatile("" : : "m"(val) : "memory");

                ++local_ops;
                ++key_idx;
            }

            total_ops += local_ops;
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - thread_start).count();
            double thread_qps = local_ops * 1000.0 / elapsed;
            printf("  线程 %2d: %8ld ops  |  %7.0f QPS\n", t, local_ops, thread_qps);
        });
    }

    for (auto& w : workers) w.join();

    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    double avg_qps = total_ops * 1000.0 / total_ms;

    // ── 4. 输出结果 ──
    printf("\n──────────────────────────────────\n");
    printf("  总操作数:  %ld\n", total_ops.load());
    printf("  总耗时:    %ld ms\n", total_ms);
    printf("  平均 QPS:  %.0f\n\n", avg_qps);
    printf("  ← gRPC 单节点: ~17,800 QPS\n");
    printf("  ← KCache 裸调: %.0f QPS\n", avg_qps);
    printf("  Δ = %.1fx 倍\n", avg_qps / 17800.0);
    printf("──────────────────────────────────\n");

    return 0;
}
