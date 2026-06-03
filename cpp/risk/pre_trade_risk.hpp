// risk/pre_trade_risk.hpp - Pre-trade risk gate for the hot order path.
//
// check() must complete in < 20us
// (all in-memory checks, no I/O).
//
// Called from the strategy thread context
// immediately on submit_order().

#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "../common/logger.hpp"
#include "../common/types.hpp"
#include "token_bucket.hpp"
namespace qf {

struct MarketHours {

  // Market is open 09:15 to 15:30 IST, Mon-Fri.

  [[nodiscard]]
  bool is_open() const noexcept {

    auto now = std::chrono::system_clock::now();

    auto tt = std::chrono::system_clock::to_time_t(now);

    std::tm ist{};

#ifdef _WIN32
    gmtime_s(&ist, &tt);
#else
    gmtime_r(&tt, &ist);
#endif

    // IST = UTC + 5:30

    int h = (ist.tm_hour + 5) % 24;
    int m = (ist.tm_min + 30) % 60;

    if (m >= 60) {
      h = (h + 1) % 24;
      m -= 60;
    }

    int hhmm = h * 100 + m;

    int wday = ist.tm_wday; // 0=Sun, 6=Sat

    if (wday == 0 || wday == 6)
      return false;

    return hhmm >= 915 && hhmm < 1530;
  }
};

class PreTradeRiskEngine {
public:
  PreTradeRiskEngine() = default;

  // Configure per-strategy rate limiters.
  void configure(const EngineConfig &cfg);

  [[nodiscard]]
  RiskDecision check(const OrderRequest &req, const StrategyConfig &scfg,
                     const PortfolioSnapshot &portfolio) const noexcept;

  // Kill switches

  void engage_global_kill_switch(const std::string &reason);

  void disengage_global_kill_switch();

  void engage_strategy_kill(const std::string &strategy_id,
                            const std::string &reason);

  [[nodiscard]]
  bool is_global_killed() const noexcept {
    return global_kill_switch_.load(std::memory_order_acquire);
  }

  [[nodiscard]]
  bool is_strategy_halted(const std::string &id) const noexcept;

  // Tick price cache
  void update_price(uint64_t token, double price) noexcept {
    price_cache_[token].store(price, std::memory_order_release);
  }

private:
  [[nodiscard]]
  double current_price(uint64_t token) const noexcept;

  [[nodiscard]]
  RiskDecision check_greeks(const OrderRequest &req,
                            const PortfolioSnapshot &portfolio,
                            const StrategyConfig &scfg) const noexcept;

  mutable std::atomic<bool> global_kill_switch_{false};

  mutable std::mutex halted_mtx_;
  std::unordered_set<std::string> halted_strategies_;

  mutable std::unordered_map<std::string, TokenBucketLimiter> rate_limiters_;

  mutable std::mutex rl_mtx_;

  // token -> last traded price
  mutable std::unordered_map<uint64_t, std::atomic<double>> price_cache_;

  mutable std::mutex pc_mtx_;

  MarketHours market_hours_;
};
} // namespace qf