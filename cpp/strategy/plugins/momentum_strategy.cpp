// strategy/plugins/momentum_strategy.cpp
//
// Full implementation of the EMA-crossover momentum strategy.
//

#include "momentum_strategy.hpp"

#include <algorithm>
#include <cmath>
#include <ctime>

namespace qf {

// --- PriceSeries helpers --------------------------------------------

double PriceSeries::atr(size_t period, const std::deque<double> &highs,
                        const std::deque<double> &lows) const noexcept {
  if (data_.size() < period + 1 || highs.size() < period ||
      lows.size() < period)
    return 0.0;

  const size_t n = data_.size();

  double sum = 0.0;

  for (size_t i = n - period; i < n; ++i) {
    double tr = highs[i] - lows[i];

    if (i > 0) {
      tr = std::max(tr, std::abs(highs[i] - data_[i - 1]));

      tr = std::max(tr, std::abs(lows[i] - data_[i - 1]));
    }

    sum += tr;
  }

  return sum / period;
}

//-------momentum strategy------------------
MomentumStrategy::MomentumStrategy() = default;
StrategyInfo MomentumStrategy::info() const {
  return {"momentum_v2", "EMA Crossover Momentum", "2.0.0",
          "Dual-EMA crossover with ATR stops, 1% risk-per-trade sizing"};
}

void MomentumStrategy::on_start(const StrategyContext &ctx) {
  inject_context(ctx);

  start_ts_ns_ = now_ns();
  alive_ = true;

  // Load parameters from config
  if (ctx.config) {
    update_params(ctx.config->params);
  }

  LOG_INFO("[Momentum:{}] Started - ema_fast={} ema_slow={} atr={} "
           "sl={} tp={} risk={}%",
           id_, ema_fast_, ema_slow_, atr_period_, atr_sl_multiplier_,
           atr_tp_multiplier_, risk_pct_ * 100.0);
}

void MomentumStrategy::on_stop() {
  alive_ = false;

  eod_flatten_all();

  LOG_INFO("[Momentum:{}] Stopped", id_);
}

void MomentumStrategy::on_kill_signal() {
  LOG_WARN("[Momentum:{}] Kill signal received - flattening all positions",
           id_);

  alive_ = false;

  eod_flatten_all();
}

void MomentumStrategy::update_params(const ParamMap &p) {
  auto g = [&](const std::string &k, auto &dest) {
    auto it = p.find(k);

    if (it != p.end())
      dest = static_cast<std::decay_t<decltype(dest)>>(it->second);
  };

  g("ema_fast", ema_fast_);
  g("ema_slow", ema_slow_);
  g("atr_period", atr_period_);
  g("atr_sl_multiplier", atr_sl_multiplier_);
  g("atr_tp_multiplier", atr_tp_multiplier_);
  g("risk_pct", risk_pct_);
  g("max_positions", max_positions_);
  g("min_signal_strength", min_signal_strength_);

  LOG_INFO("[Momentum:{}] Params updated", id_);
}

///----- on_candle: update indicators, checks entry singnal--------
void MomentumStrategy::on_tick(const NormalizedTick &tick) {
  if (!alive_)
    return;

  auto &s = states_[tick.instrument_token];

  s.last_ltp = tick.ltp;

  if (s.in_position) {
    manage_position(tick.instrument_token, tick);
  }
}

void MomentumStrategy::evaluate_signal(uint64_t token) {
  auto &s = states_[token];

  if (!s.closes.ready(static_cast<size_t>(ema_slow_ + 5)))
    return;

  double fast = s.closes.ema(ema_fast_);

  double slow = s.closes.ema(ema_slow_);

  if (slow == 0.0)
    return;

  double separation = (fast - slow) / slow;

  // Cooldown:
  // minimum 5 minutes between signals

  uint64_t now = now_ns();

  if (now - s.last_signal_ts < 300ULL * 1'000'000'000ULL)
    return;

  if (!s.in_position) {

    if (std::abs(separation) >= min_signal_strength_) {
      NormalizedTick tick{};

      tick.ltp = s.last_ltp;
      tick.instrument_token = token;

      try_entry(token, tick);
    }
  }
}

void MomentumStrategy::try_entry(uint64_t token, const NormalizedTick &tick) {
  auto &s = states_[token];

  // Count active positions
  // across instruments

  int active = 0;

  for (const auto &[_, st] : states_)
    if (st.in_position)
      ++active;

  if (active >= max_positions_)
    return;

  double atr_val = s.closes.atr(atr_period_, s.highs, s.lows);

  if (atr_val <= 0.0)
    return;

  double fast = s.closes.ema(ema_fast_);

  double slow = s.closes.ema(ema_slow_);

  Side side = (fast > slow) ? Side::BUY : Side::SELL;

  int qty = compute_qty(token, atr_val);

  if (qty <= 0)
    return;

  double stop, target;

  if (side == Side::BUY) {
    stop = tick.ltp - atr_sl_multiplier_ * atr_val;
    target = tick.ltp + atr_tp_multiplier_ * atr_val;
  } else {
    stop = tick.ltp + atr_sl_multiplier_ * atr_val;
    target = tick.ltp - atr_tp_multiplier_ * atr_val;
  }

  s.stop_price = stop;
  s.target_price = target;
  s.entry_price = tick.ltp;
  s.last_signal_ts = now_ns();

  place_market(token, side, qty,
               side == Side::BUY ? "momentum-long" : "momentum-short");
}

void MomentumStrategy::manage_position(uint64_t token,
                                       const NormalizedTick &tick) {
  auto &s = states_[token];

  if (!s.in_position)
    return;

  double price = tick.ltp;
  bool exit = false;
  std::string exit_tag;

  if (s.position_side == Side::BUY) {

    if (price <= s.stop_price) {
      exit = true;
      exit_tag = "stop-loss";
    }

    if (price >= s.target_price) {
      exit = true;
      exit_tag = "take-profit";
    }

  } else {

    if (price >= s.stop_price) {
      exit = true;
      exit_tag = "stop-loss";
    }

    if (price <= s.target_price) {
      exit = true;
      exit_tag = "take-profit";
    }
  }

  if (exit) {
    Side close_side = (s.position_side == Side::BUY) ? Side::SELL : Side::BUY;

    place_market(token, close_side, s.position_qty, exit_tag);

    s.in_position = false;
  }
}

void MomentumStrategy::eod_flatten_all() {
  for (auto &[token, s] : states_) {

    if (s.in_position) {

      Side close_side = (s.position_side == Side::BUY) ? Side::SELL : Side::BUY;

      place_market(token, close_side, s.position_qty, "eod-flatten");

      s.in_position = false;
    }
  }
}

void MomentumStrategy::place_market(uint64_t token, Side side, int qty,
                                    const std::string &tag) {
  OrderRequest req;

  req.instrument_token = token;
  req.side = side;
  req.order_type = OrderType::MARKET;
  req.product = OrderProduct::INTRADAY;
  req.quantity = qty;
  req.signal_strength = 1.0;

  std::strncpy(req.strategy_id, id_.c_str(), MAX_STRATEGY_LEN - 1);

  std::strncpy(req.tag, tag.c_str(), MAX_TAG_LEN - 1);

  req.request_ts_ns = now_ns();

  if (!submit_order(req)) {

    LOG_WARN("[Momentum:{}] Order rejected for token={} side={} qty={}", id_,
             token, side == Side::BUY ? "BUY" : "SELL", qty);
  }
}

int MomentumStrategy::compute_qty(uint64_t token, double atr_value) const {
  double capital = available_capital();

  if (capital <= 0 || atr_value <= 0)
    return 0;

  auto pos = get_position(token);

  double price = (pos.current_price > 0) ? pos.current_price
                 : states_.count(token)  ? states_.at(token).last_ltp
                                         : 0.0;

  if (price <= 0)
    return 0;

  // Risk 1% of capital per ATR unit stop

  double risk_amount = capital * risk_pct_;

  int qty = static_cast<int>(risk_amount / (atr_sl_multiplier_ * atr_value));

  return std::max(1, qty);
}

void MomentumStrategy::on_order_fill(const Fill &fill) {
  auto &s = states_[fill.instrument_token];

  if (!s.in_position) {

    // Opening fill

    s.in_position = true;
    s.position_side = fill.side;
    s.position_qty = fill.quantity;
    s.entry_price = fill.price;

    LOG_INFO("[Momentum:{}] Entered {} {} @ {:.2f}", id_, fill.side_str(),
             fill.instrument_token, fill.price);

  } else {

    // Closing fill

    double pnl = 0;

    if (fill.side == Side::SELL)
      pnl = (fill.price - s.entry_price) * fill.quantity;
    else
      pnl = (s.entry_price - fill.price) * fill.quantity;

    s.in_position = false;
    s.position_qty = 0;

    LOG_INFO("[Momentum:{}] Exited {} {} @ {:.2f} PnL={:.2f}", id_,
             fill.side_str(), fill.instrument_token, fill.price, pnl);
  }
}

void MomentumStrategy::on_order_reject(const std::string &oid,
                                       const std::string &reason) {
  LOG_WARN("[Momentum:{}] Order {} rejected: {}", id_, oid, reason);
}

} // namespace qf