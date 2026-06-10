// strategy/plugins/momentum_strategy.hpp
//
// EMA-crossover momentum strategy with ATR-based position sizing,
// dynamic stop-loss/take-profit management, and EOD auto-flatten.
//
// Compiled as a shared library:
// g++ -shared -fPIC -O3 momentum_strategy.cpp -o momentum_v2.so

#pragma once

#include <deque>
#include <string>
#include <unordered_map>

#include "../../common/types.hpp"
#include "../strategy_interface.hpp"

namespace qf {

// ── Price series helper ─────────────────────────────────────────────

class PriceSeries {
public:
  explicit PriceSeries(size_t max_len = 200) : max_len_(max_len) {}

  void push(double price) {
    data_.push_back(price);

    if (data_.size() > max_len_)
      data_.pop_front();
  }

  [[nodiscard]]
  double ema(size_t period) const noexcept {
    if (data_.size() < period)
      return 0.0;

    const double k = 2.0 / (period + 1.0);

    double val = data_[data_.size() - period];

    for (size_t i = data_.size() - period + 1; i < data_.size(); ++i) {
      val = data_[i] * k + val * (1.0 - k);
    }

    return val;
  }

  [[nodiscard]]
  double atr(size_t period, const std::deque<double> &highs,
             const std::deque<double> &lows) const noexcept;

  [[nodiscard]]
  size_t size() const noexcept {
    return data_.size();
  }

  [[nodiscard]]
  double back() const noexcept {
    return data_.empty() ? 0.0 : data_.back();
  }

  [[nodiscard]]
  double front() const noexcept {
    return data_.empty() ? 0.0 : data_.front();
  }

  [[nodiscard]]
  bool ready(size_t n) const noexcept {
    return data_.size() >= n;
  }

private:
  size_t max_len_;
  std::deque<double> data_;
};

// ── Momentum strategy ───────────────────────────────────────────────

class MomentumStrategy : public IStrategy {
public:
  MomentumStrategy();

  // IStrategy interface

  void on_start(const StrategyContext &ctx) override;

  void on_stop() override;

  void on_tick(const NormalizedTick &tick) override;

  void on_candle(const Candle &candle) override;

  void on_order_fill(const Fill &fill) override;

  void on_order_reject(const std::string &oid, const std::string &r) override;

  void on_kill_signal() override;

  void update_params(const ParamMap &params) override;

  StrategyInfo info() const override;

private:
  // ── Signal logic ───────────────────────────────────────────────

  void evaluate_signal(uint64_t token);

  void try_entry(uint64_t token, const NormalizedTick &tick);

  void manage_position(uint64_t token, const NormalizedTick &tick);

  void eod_flatten_all();

  // ── Order helpers ──────────────────────────────────────────────

  void place_market(uint64_t token, Side side, int qty, const std::string &tag);

  int compute_qty(uint64_t token, double atr_value) const;

  // ── Per-instrument state ───────────────────────────────────────

  struct InstrumentState {

    PriceSeries closes{200};

    std::deque<double> highs;
    std::deque<double> lows;

    double last_ltp{0};

    double entry_price{0};
    double stop_price{0};
    double target_price{0};

    std::string open_order_id;

    bool in_position{false};

    Side position_side{Side::BUY};

    int position_qty{0};

    uint64_t last_signal_ts{0};
  };

  std::unordered_map<uint64_t, InstrumentState> states_;

  // ── Strategy parameters (from config, hot-reloadable)

  int ema_fast_{9};
  int ema_slow_{21};

  int atr_period_{14};

  double atr_sl_multiplier_{1.5};
  double atr_tp_multiplier_{3.0};

  double risk_pct_{0.01}; // 1% of capital per trade

  int max_positions_{3};

  double min_signal_strength_{0.002}; // 0.2% gap between EMAs

  int eod_flatten_hhmm_{1520}; // 15:20 IST

  // ── Engine state ───────────────────────────────────────────────

  bool alive_{true};

  uint64_t start_ts_ns_{0};
};

} // namespace qf

// ── Plugin ABI ──────────────────────────────────────────────────────

extern "C" {

qf::IStrategy *create_strategy() { return new qf::MomentumStrategy(); }
void destroy_strategy(qf::IStrategy *s) { delete s; }
}