enable_testing()

# 测试 lru 缓存
add_executable(test_lru "./test_lru.cpp")
target_link_libraries(test_lru PRIVATE GTest::gtest_main kcache_core)

# 测试一致性哈希
add_executable(test_cons_hash "./test_consistent_hash.cpp")
target_link_libraries(test_cons_hash PRIVATE GTest::gtest_main kcache_core)

# 测试 cache group
add_executable(test_group "./test_group.cpp")
target_link_libraries(test_group PRIVATE GTest::gtest_main kcache_core)

# 测试熔断器和降级策略
add_executable(test_circuit_breaker "./test_circuit_breaker.cpp")
target_link_libraries(test_circuit_breaker PRIVATE GTest::gtest_main kcache_core)
