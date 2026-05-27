// common/logger.hpp — Thin spdlog wrapper with component tagging.
//
// All engine components call LOG_INFO / LOG_WARN / LOG_ERROR / LOG_CRITICAL.
// Uses the global "qf" spdlog logger initialised in main.cpp.
// Thread-safe — spdlog guarantees per-message atomicity.
//

#pragma once

#include <memory>
#include <string>

#include <spdlog/async.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace qf {
namespace log {

// Called once in main.cpp before any other code.
inline void init(const std::string &log_dir = "logs",
                 spdlog::level::level_enum level = spdlog::level::info) {
  try {
    spdlog::init_thread_pool(8192, 2); // 8K queue, 2 background threads

    auto console = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();

    auto file = std::make_shared<spdlog::sinks::daily_file_sink_mt>(
        log_dir + "/engine.log", 0,
        0); // rotate at midnight

    console->set_level(level);
    file->set_level(spdlog::level::trace);

    std::vector<spdlog::sink_ptr> sinks{console, file};

    auto logger = std::make_shared<spdlog::async_logger>(
        "qf", sinks.begin(), sinks.end(), spdlog::thread_pool(),
        spdlog::async_overflow_policy::overrun_oldest);

    logger->set_level(spdlog::level::trace);

    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%l] [%t] %v");

    spdlog::set_default_logger(logger);

    spdlog::flush_every(std::chrono::seconds(1));
  } catch (const spdlog::spdlog_ex &ex) {
    fprintf(stderr, "Logger init failed: %s\n", ex.what());
  }
}

inline void flush() { spdlog::default_logger()->flush(); }

inline void shutdown() { spdlog::shutdown(); }

} // namespace log
} // namespace qf

// Convenience macros — zero overhead when level is disabled.
#define LOG_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define LOG_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define LOG_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define LOG_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define LOG_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define LOG_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)