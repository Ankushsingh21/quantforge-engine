// strategy/strategy_manager.hpp
//
// Manages the lifecycle of all loaded strategy plugins.
//
// For each strategy in engine.yaml it:
//   1. dlopen's the .so plugin
//   2. Spawns a dedicated thread (pinned to configured core)
//   3. Registers a Disruptor consumer cursor
//   4. Dispatches on_tick / on_candle callbacks

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../common/disruptor.hpp"
#include "../common/spsc_queue.hpp"
#include "../common/types.hpp"

#include "strategy_interface.hpp"

namespace qf {

class PortfolioEngine;
class PreTradeRiskEngine;
class OrderManagementSystem;

struct LoadedStrategy {
  std::unique_ptr<IStrategy> instance;
  void *dl_handle{nullptr};

  StrategyConfig config;
  StrategyContext context;

  std::thread thread;
  std::atomic<bool> running{false};

  int64_t disruptor_seq{-1}; // consumer cursor
};

class StrategyManager {
public:
  StrategyManager(TickDisruptor &disruptor, PortfolioEngine &portfolio,
                  PreTradeRiskEngine &risk, OrderManagementSystem &oms,
                  const EngineConfig &cfg);

  ~StrategyManager();

  // Load and start all strategies from config.
  void start_all();

  // Stop all strategies and join threads.
  void stop_all();

  // Deliver candle to all strategies subscribed to that instrument.
  void dispatch_candle(const Candle &candle);

  // Deliver fill to the owning strategy.
  void dispatch_fill(const Fill &fill);

  // Halt a specific strategy (risk-triggered).
  void halt_strategy(const std::string &id, const std::string &reason);

  // Update runtime parameters for a running strategy.
  void update_params(const std::string &id, const ParamMap &params);

private:
  void load_strategy(const StrategyConfig &cfg);

  void run_strategy_thread(LoadedStrategy &s);

  StrategyContext build_context(LoadedStrategy &s);

  TickDisruptor &disruptor_;
  PortfolioEngine &portfolio_;
  PreTradeRiskEngine &risk_;
  OrderManagementSystem &oms_;

  const EngineConfig &cfg_;

  std::vector<std::unique_ptr<LoadedStrategy>> strategies_;

  mutable std::mutex mtx_;

  // Per-token last-tick cache.
  // Strategies call ctx.get_last_tick(token) to read the most recent tick
  // for any subscribed instrument without re-deriving it from the disruptor.
  //
  // Write: unique_lock (one strategy thread updates on every consumed tick).
  // Read:  shared_lock (all other strategies read concurrently, zero
  // contention).
  std::unordered_map<uint64_t, NormalizedTick> last_tick_cache_;

  mutable std::shared_mutex last_tick_mtx_;
};

} // namespace qf