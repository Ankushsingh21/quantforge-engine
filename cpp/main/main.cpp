// main/main.cpp — QuantForge C++ Engine Entry Point
//
// Start sequence:
// 1. Parse config/engine.yaml
// 2. Load access token from .token file (written by Python auth.py)
// 3. Build all components
// 4. Start IO thread (market-data WebSocket on Core 0)
// 5. Start TickNormalizer thread (Core 1)
// 6. Start PortfolioEngine fill thread (Core 3)
// 7. Start all strategy threads (Core 2+)
// 8. Block on SIGINT / SIGTERM
// 9. Graceful shutdown

#include <atomic>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include <boost/asio.hpp>
#include <nlohmann/json.hpp>

// yaml-cpp is used to parse engine.yaml; if only nlohmann is available
// the config is read as JSON (rename engine.yaml to engine.json)
// For full YAML support, add yaml-cpp to conanfile.txt.
// Here we include a simple YAML->JSON shim via the config helper.

#include "../common/disruptor.hpp"
#include "../common/logger.hpp"
#include "../common/spsc_queue.hpp"
#include "../common/thread_utils.hpp"
#include "../common/types.hpp"

#include "../market_data/candle_builder.hpp"
#include "../market_data/tick_normalizer.hpp"
#include "../market_data/upstox_ws_handler.hpp"

#include "../risk/pre_trade_risk.hpp"

#include "../ems/ems.hpp"
#include "../oms/oms.hpp"

#include "../portfolio/portfolio_engine.hpp"

#include "../strategy/strategy_manager.hpp"

#include "../transport/kafka_publisher.hpp"
#include "../transport/redis_client.hpp"

namespace qf {

// — Signal handling —
static std::atomic<bool> g_shutdown{false};

static void signal_handler(int sig) {

  LOG_WARN("[Engine] Signal {} received — initiating shutdown", sig);

  g_shutdown.store(true, std::memory_order_release);
}

// — Config loading —

std::string load_token(const std::string &token_file = ".token") {

  std::ifstream f(token_file);

  if (!f.is_open()) {

    // Try the Python quantforge directory

    std::ifstream f2("../quantforge/.token");

    if (f2.is_open()) {

      std::string tok;

      std::getline(f2, tok);

      while (!tok.empty() && (tok.back() == '\n' || tok.back() == '\r')) {
        tok.pop_back();
      }

      return tok;
    }

    throw std::runtime_error("Access token file not found: " + token_file +
                             "\nRun: cd ../quantforge  && python src/auth.py");
  }

  std::string tok;

  std::getline(f, tok);

  while (!tok.empty() && (tok.back() == '\n' || tok.back() == '\r')) {
    tok.pop_back();
  }

  return tok;
}

// Simple YAML config parser (reads key: value pairs, no nesting).
// In production, replace with yaml-cpp.

EngineConfig load_config(const std::string &yaml_path) {
  EngineConfig cfg;

  // Defaults for i5-9300H (4 cores / 8 threads)

  cfg.threads.market_data_io = 0;
  cfg.threads.tick_normalizer = 1;
  cfg.threads.portfolio = 2;
  cfg.threads.fill_processor = 2;
  cfg.threads.risk_gate = 3;
  cfg.threads.oms = 3;
  cfg.threads.kafka_publisher = 4;
  cfg.threads.heartbeat = 5;

  // Try to parse the YAML file line by line for top-level keys

  std::ifstream f(yaml_path);

  if (!f.is_open()) {

    LOG_WARN("[Config] {} not found — using defaults", yaml_path);

    return cfg;
  }

  std::string line;

  while (std::getline(f, line)) {

    // Skip comments and empty lines

    if (line.empty() || line[0] == '#')
      continue;

    auto colon = line.find(':');

    if (colon == std::string::npos)
      continue;

    std::string key = line.substr(0, colon);
    std::string val = line.substr(colon + 1);

    // Trim whitespace

    auto trim = [](std::string &s) {
      size_t b = s.find_first_not_of(" \t\r\n");
      size_t e = s.find_last_not_of(" \t\r\n");

      s = (b == std::string::npos) ? "" : s.substr(b, e - b + 1);
    };

    trim(key);
    trim(val);

    if (key == "redis_url")
      cfg.redis_url = val;

    if (key == "kafka_brokers")
      cfg.kafka_brokers = val;

    if (key == "paper_trading")
      cfg.paper_trading = (val == "true");

    if (key == "dry_run")
      cfg.dry_run = (val == "true");
  }

  LOG_INFO("[Config] Loaded from {} — paper_trading={}", yaml_path,
           cfg.paper_trading);

  return cfg;
}

} // namespace qf

// — main —

int main(int argc, char *argv[]) {

  using namespace qf;

  // — Logging —

  std::filesystem::create_directories("logs");

  qf::log::init("logs", spdlog::level::info);

  LOG_INFO("=======================================");
  LOG_INFO("   QuantForge C++ Engine v1.0.0");
  LOG_INFO("=======================================");

  // — Signal handlers —

  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  try {

    // — Config —

    std::string config_path = (argc > 1) ? argv[1] : "config/engine.yaml";

    EngineConfig cfg = load_config(config_path);

    // — Access token —

    cfg.upstox_access_token = load_token();

    LOG_INFO("[Engine] Access token loaded (len={})",
             cfg.upstox_access_token.size());

    // — Queues & shared buffers —

    TickQueue tick_queue;    // WS-handler -> TickNormalizer
    FillQueue fill_queue;    // OMS -> PortfolioEngine
    TickDisruptor disruptor; // TickNormalizer -> Strategy threads

    // — Kafka publisher (optional) —

    KafkaPublisher kafka(cfg.kafka_brokers);

    if (kafka.is_healthy()) {
      LOG_INFO("[Engine] Kafka publisher ready — brokers={}",
               cfg.kafka_brokers);
    } else {
      LOG_WARN("[Engine] Kafka not available — tick/fill publishing disabled");
    }

    // — Redis client (optional) —

    RedisClient redis(cfg.redis_url);

    if (redis.is_connected()) {
      LOG_INFO("[Engine] Redis connected — {}", cfg.redis_url);
    } else {
      LOG_WARN("[Engine] Redis not available — hot state publishing disabled");
    }

    // — Infrastructure components —

    boost::asio::io_context ioc;

    auto work_guard = boost::asio::make_work_guard(ioc);

    CandleBuilder candle_builder;

    PortfolioEngine portfolio(fill_queue, cfg);

    PreTradeRiskEngine risk;

    risk.configure(cfg);

    ExecutionManagementSystem ems(ioc, cfg);

    OrderManagementSystem oms(ems, fill_queue, cfg);

    ems.set_fill_callback(

        [&oms](const std::string &exoid, double p, int q,
               const std::string &tid) { oms.on_fill(exoid, p, q, tid); });

    oms.set_fill_callback(

        [&](const Fill &fill) {
          risk.update_price(fill.instrument_token, fill.price);

          kafka.publish_fill(fill); // async to Kafka order.fills
        });

    portfolio.set_update_callback(

        [&kafka, &redis](const PortfolioSnapshot &snap) {
          kafka.publish_portfolio(snap);

          // Write each position to Redis

          for (const auto &[_, pos] : snap.positions) {

            redis.publish_position(std::string(pos.strategy_id), pos);
          }

          redis.flush();
        });

    TickNormalizer normalizer(tick_queue, disruptor, candle_builder, cfg);

    normalizer.set_kafka_fn(

        [&risk, &kafka, &redis](const NormalizedTick &t) {
          risk.update_price(t.instrument_token, t.ltp);

          kafka.publish_tick(t); // async, non-blocking
          redis.publish_tick(t); // pipeline write to Redis
        });

    StrategyManager strategy_mgr(disruptor, portfolio, risk, oms, cfg);

    candle_builder.register_callback(

        [&strategy_mgr](const Candle &c) { strategy_mgr.dispatch_candle(c); });

    UpstoxWSHandler ws_handler(ioc, tick_queue, cfg);

    // Default instruments from config strategies

    std::vector<std::string> instrument_keys;

    for (const auto &scfg : cfg.strategies) {

      for (uint64_t tok : scfg.instruments) {

        // Build token -> key map
        // (simplified: use token as string key)

        instrument_keys.push_back(std::to_string(tok));
      }
    }

    // — Start order —

    LOG_INFO("[Engine] Starting components...");

    portfolio.start();
    strategy_mgr.start_all();
    normalizer.start();

    // Subscribe market data

    if (!instrument_keys.empty()) {

      ws_handler.subscribe(instrument_keys, "full");
    } else {

      LOG_WARN("[Engine] No instruments configured — subscribe manually");
    }

    ws_handler.start();

    // IO thread (WebSocket) — pinned to Core 0

    std::thread io_thread(

        [&ioc, &cfg] {
          setup_hot_thread(cfg.threads.market_data_io, 80, "qf-io");

          LOG_INFO("[Engine] IO thread started on core {}",
                   cfg.threads.market_data_io);

          ioc.run();

          LOG_INFO("[Engine] IO thread exited");
        });

    // — Heartbeat / monitor loop (main thread) —

    LOG_INFO("[Engine] Running. Press Ctrl+C to stop.");

    uint64_t beat = 0;

    while (!g_shutdown.load(std::memory_order_acquire)) {

      std::this_thread::sleep_for(std::chrono::seconds(10));

      ++beat;

      auto snap = portfolio.get_snapshot();

      LOG_INFO("[Heartbeat #{}] Ticks={} OpenOrders={} "
               "Capital={:.2f} RPnL={:.2f} UPnL={:.2f}",

               beat, normalizer.ticks_processed(), oms.open_order_count(),
               snap.available_capital, snap.total_realized_pnl,
               snap.total_unrealized_pnl);
    }

    // — Graceful shutdown —

    LOG_INFO("[Engine] Shutting down...");

    oms.cancel_all_open();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ws_handler.stop();

    work_guard.reset();

    ioc.stop();

    normalizer.stop();

    strategy_mgr.stop_all();

    portfolio.stop();

    candle_builder.flush_all();

    kafka.flush(5000);

    redis.flush();

    if (io_thread.joinable())
      io_thread.join();

    LOG_INFO("[Engine] Clean shutdown complete.");

    qf::log::flush();

    qf::log::shutdown();

  } catch (const std::exception &e) {

    LOG_CRITICAL("[Engine] Fatal: {}", e.what());

    qf::log::flush();

    qf::log::shutdown();

    return 1;
  }

  return 0;
}