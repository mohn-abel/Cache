cmake_minimum_required(VERSION 3.20) # 最低版本要求
project(KCache) # 项目名称

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置可执行文件输出目录为 /bin
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

# 头文件位置：
include_directories(${CMAKE_SOURCE_DIR}/src/include # src/include 项目内部头文件
                    ${CMAKE_SOURCE_DIR}/src/proto)  # src/proto 生成的 proto 和 grpc

# 查找依赖，加载外部库
find_package(GTest CONFIG REQUIRED)
find_package(gflags CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(etcd-cpp-apiv3 CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(httplib CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
# 扫描子目录下的CmakeLists，将控制权移交子目录的cmakelists
add_subdirectory(src) # 核心车间
add_subdirectory(test) # 质检车间
add_subdirectory(example) # 应用车间
