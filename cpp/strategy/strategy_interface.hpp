// strategy/strategy_interface.hpp
//
// Abstract base class for all QuantForge strategies.
// Compiled as a shared library (.so) and loaded at runtime via dlopen.
// Each .so must export `create_strategy()` and `destroy_strategy()`.
//
// Thread-safety contract:
//   All callbacks (on_tick, on_candle, …) are called from the SAME
//   strategy thread – no internal synchronisation needed.
//   `submit_order` is thread-safe and may be called from any callback.
//

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "../common/logger.hpp"
#include "../common/types.hpp"

namespace qf {

// Forward declarations (full types in their own headers)
class PortfolioEngine;
class PreTradeRiskEngine;

// Context injected by StrategyManager – gives strategy access to engine
// services.
struct StrategyContext {

  std::function<bool(const OrderRequest &)> submit_order;

  std::function<bool(const std::string &)> cancel_order;

  std::function<Position(uint64_t)> get_position;

  std::function<double()> get_available_capital;

  std::function<NormalizedTick(uint64_t)> get_last_tick;

  std::function<uint64_t()> now_ns; // monotonic clock

  const StrategyConfig *config{nullptr};
};

// ── IStrategy: the core interface ───────────────────────────────────

class IStrategy {
public:
  virtual ~IStrategy() = default;

  // ── Lifecycle ──────────────────────────────────────────────────

  virtual void on_start(const StrategyContext &ctx) = 0;

  virtual void on_stop() = 0;

  // ── Market data callbacks ──────────────────────────────────────

  virtual void on_tick(const NormalizedTick &tick) = 0;

  virtual void on_candle(const Candle &candle) = 0;

  // ── Order lifecycle callbacks ─────────────────────────────────

  virtual void on_order_fill(const Fill &fill) = 0;

  virtual void on_order_reject(const std::string &order_id,
                               const std::string &reason) = 0;

  // ── Control ────────────────────────────────────────────────────

  virtual void on_kill_signal() = 0;

  virtual void update_params(const ParamMap &params) = 0;

  // ── Metadata ───────────────────────────────────────────────────

  virtual StrategyInfo info() const = 0;

  // ── Non-virtual helpers (use context) ──────────────────────────

  bool submit_order(const OrderRequest &req) {
    if (!ctx_)
      return false;

    return ctx_->submit_order(req);
  }

  bool cancel_order(const std::string &order_id) {
    if (!ctx_)
      return false;

    return ctx_->cancel_order(order_id);
  }

  Position get_position(uint64_t token) const {
    if (!ctx_)
      return {};

    return ctx_->get_position(token);
  }

  double available_capital() const {
    if (!ctx_)
      return 0.0;

    return ctx_->get_available_capital();
  }

  uint64_t now_ns() const {
    if (!ctx_)
      return 0;

    return ctx_->now_ns();
  }

  void inject_context(const StrategyContext &ctx) { ctx_ = &ctx; }

  [[nodiscard]]
  const std::string &strategy_id() const noexcept {
    return id_;
  }

  void set_strategy_id(const std::string &id) { id_ = id; }

protected:
  const StrategyContext *ctx_{nullptr};

  std::string id_;
};

// ── Plugin ABI ─────────────────────────────────────────────────────
//
// Each .so must export these two C symbols.
//

extern "C" {

using CreateStrategyFn = IStrategy *(*)();

using DestroyStrategyFn = void (*)(IStrategy *);
}

} // namespace qf