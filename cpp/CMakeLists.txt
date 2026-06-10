cmake_minimum_required(VERSION 3.25)

project(quantforge-engine CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── Optimization flags ───────────────────────────────────────────────
# Release: maximum speed. Debug: sanitizers + gdb info.
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG -flto")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3 -fsanitize=address,undefined")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -march=native")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# ── Conan integration ────────────────────────────────────────────────
if(EXISTS "${CMAKE_BINARY_DIR}/conanbuildinfo.cmake")
    include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
    conan_basic_setup(TARGETS)
else()
    # Conan 2 CMakeDeps
    find_package(Boost REQUIRED COMPONENTS system thread)
    find_package(spdlog REQUIRED)
    find_package(nlohmann_json REQUIRED)
    find_package(protobuf CONFIG REQUIRED)
    find_package(OpenSSL REQUIRED)
    find_package(hiredis QUIET)
    find_package(RdKafka QUIET)
endif()

# ── Protobuf generated sources ───────────────────────────────────────
# Both .pb.cc files are genarted by scrtips/build_cpp.sh (step 1) before
# CMake rune. if either is missing the build scrtips failed -run it first.
set(PROTO_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/MarketDataFeed.pb.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/quantforge.pb.cc
)

foreach (_f ${PROTO_SRCS})
    if(NOT EXISTS "${_f}")
        message(FATAL_ERROR
        "Required genrated file not found: ${_f}\n"
        "Run './scripts/build_cpp.sh' first it calls protoc to genrate these files.")
    endif()
endforeach()    

# ── Source files ─────────────────────────────────────────────────────
set(ENGINE_SOURCES
    main/main.cpp
    market_data/upstox_ws_handler.cpp
    market_data/tick_normalizer.cpp
    strategy/strategy_manager.cpp
    strategy/plugins/momentum_strategy.cpp
    risk/pre_trade_risk.cpp
    oms/oms.cpp
    ems/ems.cpp
    transport/upstox_rest_client.cpp
    transport/kafka_publisher.cpp
    transport/redis_client.cpp
    portfolio/portfolio_engine.cpp
    ${PROTO_SRCS}
)

add_executable(quantforge-engine ${ENGINE_SOURCES})

target_include_directories(quantforge-engine PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
    ${Protobuf_INCLUDE_DIRS}
    $<TARGET_PROPERTY:nlohmann_json::nlohmann_json,INTERFACE_INCLUDE_DIRECTORIES>
)

# ── Link libraries ───────────────────────────────────────────────────
target_link_libraries(quantforge-engine PRIVATE
    Boost::system
    Boost::thread
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    protobuf::protobuf     
    OpenSSL::SSL
    OpenSSL::Crypto
    pthread
)

if(hiredis_FOUND)
    target_link_libraries(
        quantforge-engine PRIVATE hiredis::hiredis
    )
    target_compile_definitions(
        quantforge-engine PRIVATE QUANTFORGE_REDIS_ENABLED
    )
endif()

if(RdKafka_FOUND)
    target_link_libraries(
        quantforge-engine PRIVATE RdKafka::rdkafka++
    )
    target_compile_definitions(
        quantforge-engine PRIVATE QUANTFORGE_KAFKA_ENABLED
    )
endif()

# ── Compiler warnings ────────────────────────────────────────────────
target_compile_options(quantforge-engine PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wno-unused-parameter
    #$<$<CONFIG:Release>:-Werror>
)

# ── Install ──────────────────────────────────────────────────────────
install(
    TARGETS quantforge-engine
    DESTINATION bin
)