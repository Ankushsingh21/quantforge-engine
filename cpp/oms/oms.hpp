// oms/oms.hpp - Order Management System
//
// Responsibilities:
//   - Receive approved OrderRequests from strategies (via risk gate)
//   - Assign internal order IDs, track lifecycle via state machine
//   - Submit to exchange via EMS (async REST)
//   - Handle ACK, FILL, REJECT, CANCEL callbacks from EMS / broker responses
//   - Publish fills to FillQueue -> PortfolioEngine and StrategyManager

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "../common/logger.hpp"
#include "../common/spsc_queue.hpp"
#include "../common/types.hpp"
#include "fill_deduplicator.hpp"
#include "order_pool.hpp"

namespace qf {

// Forward declaration
class ExecutionManagementSystem;

class OrderManagementSystem {
public:
  using FillCallback = std::function<void(const Fill &)>;

  using RejectCallback = std::function<void(const std::string &order_id,
                                            const std::string &reason)>;

  explicit OrderManagementSystem(ExecutionManagementSystem &ems,
                                 FillQueue &fill_queue,
                                 const EngineConfig &cfg);

  // Strategy-facing API

  // Submit an order (after risk gate passes). Returns true if accepted.
  bool submit(const OrderRequest &req);

  // Cancel an open order by internal ID.
  bool cancel(const std::string &order_id);

  // Exchange callback API (called by EMS/webhook parser)

  // Exchange acknowledged order -> OPEN state
  void on_order_ack(const std::string &upstox_order_id,
                    const std::string &internal_order_id);

  // Partial or complete fill
  void on_fill(const std::string &upstox_order_id, double fill_price,
               int fill_qty, const std::string &trade_id);

  // Exchange rejected order
  void on_reject(const std::string &upstox_order_id, const std::string &reason);

  // Monitoring
  [[nodiscard]]
  size_t open_order_count() const;

  void log_open_orders() const;

  // Register callbacks
  void set_fill_callback(FillCallback cb) { fill_cb_ = std::move(cb); }

  void set_reject_callback(RejectCallback cb) { reject_cb_ = std::move(cb); }

  // EOD: cancel all open orders
  void cancel_all_open();

  void reset_for_eod() { dedup_.reset(); }

private:
  std::string generate_order_id();

  ExecutionManagementSystem &ems_;
  FillQueue &fill_queue_;
  const EngineConfig &cfg_;

  OrderPool<10000> pool_;

  // Internal-ID -> PooledOrder*
  std::unordered_map<std::string, PooledOrder *> orders_;

  // Upstox exchange order ID -> internal ID
  std::unordered_map<std::string, std::string> exchange_to_internal_;

  mutable std::mutex orders_mtx_;

  FillCallback fill_cb_;
  RejectCallback reject_cb_;

  FillDeduplicator dedup_;

  std::atomic<uint64_t> seq_{0};

  // monotonic counter for order IDs
  bool paper_mode_;
};

} // namespace qf