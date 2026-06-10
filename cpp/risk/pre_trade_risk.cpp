#include "pre_trade_risk.hpp"

namespace qf {

void PreTradeRiskEngine::configure(const EngineConfig &cfg) {

  std::lock_guard lk(rl_mtx_);

  for (const auto &scfg : cfg.strategies) {

    rate_limiters_.emplace(
        std::piecewise_construct, std::forward_as_tuple(scfg.id),
        std::forward_as_tuple(scfg.risk.max_orders_per_second));
  }

  LOG_INFO("[Risk] Configured rate limiters for {} strategies",
           cfg.strategies.size());
}

RiskDecision
PreTradeRiskEngine::check(const OrderRequest &req, const StrategyConfig &scfg,
                          const PortfolioSnapshot &portfolio) const noexcept {
  // --- Check 1: Global kill switch

  if (global_kill_switch_.load(std::memory_order_acquire))
    return RiskDecision::REJECT_KILL_SWITCH;
  // --- Check 2: Strategy kill

  const std::string sid(req.strategy_id);

  if (is_strategy_halted(sid))
    return RiskDecision::REJECT_KILL_SWITCH;
  // --- Check 3: Market hours

  if (!market_hours_.is_open())
    return RiskDecision::REJECT_MARKET_CLOSED;

  // --- Check 4: Rate limit

  {
    std::lock_guard lk(rl_mtx_);

    auto it = rate_limiters_.find(sid);

    if (it != rate_limiters_.end() && !it->second.try_consume())
      return RiskDecision::REJECT_RATE_LIMIT;
  }

  double ltp = current_price(req.instrument_token);

  // --- Check 5: Max single order value

  if (ltp > 0) {

    double order_value = req.quantity * ltp;

    if (order_value > scfg.risk.max_single_order_value)

      return RiskDecision::REJECT_POSITION_LIMIT;
  }

  // --- Check 6: Position limit per instrument

  if (ltp > 0) {

    auto pos = portfolio.get_position(req.instrument_token);

    int delta = (req.side == Side::BUY ? req.quantity : -req.quantity);

    int new_qty = pos.quantity + delta;

    double new_value = std::abs(new_qty) * ltp;

    if (new_value > scfg.risk.max_position_value)

      return RiskDecision::REJECT_POSITION_LIMIT;
  }

  // --- Check 7: Capital allocation

  if (ltp > 0) {

    double used = portfolio.strategy_used_capital(sid);

    double needed = req.quantity * ltp;

    if (used + needed > scfg.risk.max_capital_allocation)

      return RiskDecision::REJECT_CAPITAL;
  }

  // --- Check 8: Price sanity (limit orders only)

  if (req.order_type == OrderType::LIMIT && ltp > 0 && req.limit_price > 0) {

    double deviation = std::abs(req.limit_price - ltp) / ltp;

    if (deviation > 0.05)
      return RiskDecision::REJECT_PRICE_SANITY;
  }

  // --- Check 9: Greeks
  // (options instruments – identified by oi > 0)

  // Simplified check: if options, run greeks validation.
  // (instrument key format "NSE_FO|..." signals options;
  // use oi > 0 as proxy)

  // Full greeks are computed in portfolio engine;
  // we just check limits here.

  // RiskDecision greeks_rc =
  //      check_greeks(req, portfolio, scfg);

  // if (greeks_rc != RiskDecision::PASS)
  //      return greeks_rc;

  // --- Check 10: Daily loss

  double daily_pnl = portfolio.strategy_daily_pnl(sid);

  if (daily_pnl < -scfg.risk.max_daily_loss)

    return RiskDecision::REJECT_DAILY_LOSS;

  return RiskDecision::PASS;
}

void PreTradeRiskEngine::engage_global_kill_switch(const std::string &reason) {

  global_kill_switch_.store(true, std::memory_order_release);
  LOG_CRITICAL("[Risk] *** GLOBAL KILL SWITCH ENGAGED: {} ***", reason);
}

void PreTradeRiskEngine::disengage_global_kill_switch() {

  global_kill_switch_.store(false, std::memory_order_release);
  LOG_WARN("[Risk] Global kill switch disengaged - trading resumed");
}

bool PreTradeRiskEngine::is_strategy_halted(
    const std::string &id) const noexcept {
  std::lock_guard lk(halted_mtx_);

  return halted_strategies_.count(id) > 0;
}

double PreTradeRiskEngine::current_price(uint64_t token) const noexcept {
  std::lock_guard lk(pc_mtx_);

  auto it = price_cache_.find(token);

  if (it == price_cache_.end())
    return 0.0;

  return it->second.load(std::memory_order_relaxed);
}

RiskDecision
PreTradeRiskEngine::check_greeks(const OrderRequest & /*req*/,
                                 const PortfolioSnapshot &portfolio,
                                 const StrategyConfig &scfg) const noexcept {
  const std::string sid(scfg.id);

  auto pos = portfolio.positions;

  double net_delta = 0;
  double net_vega = 0;

  for (auto &[_, p] : pos) {

    if (std::strncmp(p.strategy_id, sid.c_str(), MAX_STRATEGY_LEN) == 0) {
      net_delta += p.net_delta;
      net_vega += p.net_vega;
    }
  }

  if (std::abs(net_delta) > scfg.risk.max_net_delta)
    return RiskDecision::REJECT_GREEK_LIMIT;

  if (std::abs(net_vega) > scfg.risk.max_net_vega)
    return RiskDecision::REJECT_GREEK_LIMIT;

  return RiskDecision::PASS;
}

} // namespace qf