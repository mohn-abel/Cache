add_executable(bench_cache bench_cache.cpp)
target_link_libraries(bench_cache PRIVATE kcache_core gflags::gflags spdlog::spdlog fmt::fmt)
