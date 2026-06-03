// portfolio/portfolio_engine.hpp
//
// Tracks real-time positions, P&L, and margin usage.
// Updated by the PortfolioEngine fill loop (called from FillQueue consumer).
// PortfolioSnapshot is exposed to PreTradeRiskEngine for capital checks.
//
// Thread-safety: write path is single-threaded (PortfolioEngine fill loop).
//                read path (get_snapshot) returns a copy protected by
//                shared_mutex.
//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "../common/logger.hpp"
#include "../common/spsc_queue.hpp"
#include "../common/types.hpp"

namespace qf {

class PortfolioEngine {
public:
  explicit PortfolioEngine(FillQueue &fill_queue, const EngineConfig &cfg);

  ~PortfolioEngine();

  // Start the fill-processing thread.
  void start();
  void stop();

  // --- Read API (any thread) ----------------------------

  [[nodiscard]]
  PortfolioSnapshot get_snapshot() const;

  [[nodiscard]]
  Position get_position(uint64_t token) const;

  [[nodiscard]]
  double available_capital() const;

  // --- Write API (internal, called from fill-processing thread)

  void process_fill(const Fill &fill);

  // Update mark-to-market prices from tick stream.
  void update_price(uint64_t token, double price);

  // Called at start-of-day to load positions from REST.
  void load_positions(const std::vector<Position> &positions);

  // Called at end-of-day to reset all positions.
  void reset_for_eod();

  // Register a callback for portfolio updates
  // (e.g. Kafka publisher).

  using UpdateCallback = std::function<void(const PortfolioSnapshot &)>;

  void set_update_callback(UpdateCallback cb) { update_cb_ = std::move(cb); }

private:
  void fill_loop();
  void emit_update();

  FillQueue &fill_queue_;
  const EngineConfig &cfg_;

  mutable std::shared_mutex snapshot_mtx_;
  PortfolioSnapshot snapshot_;

  std::thread thread_;
  std::atomic<bool> stop_{false};

  UpdateCallback update_cb_;

  // Starting capital loaded from config / REST
  double starting_capital_{0.0};
};

} // namespace qf