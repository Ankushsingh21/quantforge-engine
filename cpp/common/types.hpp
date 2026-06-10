// common/types.hpp — All shared data types for the QuantForge C++ engine.
//
// Every module includes this header. Keep it lean: only POD types,
// enums, and simple value structs. No virtual methods, no std::string
// in hot-path structs (use fixed-size arrays instead).
//

#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>
namespace qf {

// ─────────────────────────────────────────────────────────────
// Limits
// ─────────────────────────────────────────────────────────────

inline constexpr size_t MAX_SYMBOL_LEN = 32;
inline constexpr size_t MAX_ORDER_ID_LEN = 36; // UUID v7 string
inline constexpr size_t MAX_TAG_LEN = 32;
inline constexpr size_t MAX_STRATEGY_LEN = 32;
inline constexpr size_t DEPTH_LEVELS = 5;

// ─────────────────────────────────────────────────────────────
// Enumerations
// ─────────────────────────────────────────────────────────────

enum class Side : uint8_t { BUY = 1, SELL = 2 };

enum class OrderType : uint8_t {
  MARKET = 1,
  LIMIT = 2,
  SL = 3, // Stop-Loss Limit
  SLM = 4 // Stop-Loss Market
};

enum class OrderProduct : uint8_t {
  INTRADAY = 1, // "I" — must square off same day
  DELIVERY = 2, // "D" — delivery
  MTF = 3       // Margin Trading Facility
};

enum class OrderValidity : uint8_t { DAY = 1, IOC = 2 };

enum class OrderStatus : uint8_t {
  PENDING = 0,
  OPEN = 1,
  COMPLETE = 2,
  CANCELLED = 3,
  REJECTED = 4,
  PARTIAL = 5,
  TRIGGER_PENDING = 6
};

enum class ExchangeId : uint8_t {
  NSE_EQ = 1,
  NSE_FO = 2,
  BSE_EQ = 3,
  BSE_FO = 4,
  NSE_IDX = 5
};

enum class RiskDecision : uint8_t {
  PASS = 0,
  REJECT_KILL_SWITCH = 1,
  REJECT_MARKET_CLOSED = 2,
  REJECT_RATE_LIMIT = 3,
  REJECT_POSITION_LIMIT = 4,
  REJECT_CAPITAL = 5,
  REJECT_PRICE_SANITY = 6,
  REJECT_GREEK_LIMIT = 7,
  REJECT_DAILY_LOSS = 8
};

// ─────────────────────────────────────────────────────────────
// Market depth level
// ─────────────────────────────────────────────────────────────

struct DepthLevel {
  double price{0.0};
  uint32_t qty{0};
  uint16_t orders{0};
};

// ─────────────────────────────────────────────────────────────
// Normalized tick
// (cache-line aligned, fits in 2 × 64B cache lines)
// ─────────────────────────────────────────────────────────────

struct alignas(64) NormalizedTick {

  // Identity (24 bytes)
  uint64_t instrument_token; // hash of instrument_key

  char symbol[MAX_SYMBOL_LEN]{}; // "SBIN", "NIFTY25JUNFUT", ...

  ExchangeId exchange{ExchangeId::NSE_EQ};

  uint8_t _pad0[7]{};

  // Price data (40 bytes)
  double ltp{0.0};

  double prev_close{0.0};

  double open{0.0};

  double high{0.0};

  double low{0.0};

  // Volume / OI (16 bytes)
  uint64_t volume{0};

  uint64_t oi{0};

  // Depth — 5 bid + 5 ask levels (120 bytes)
  DepthLevel bid[DEPTH_LEVELS]{};

  DepthLevel ask[DEPTH_LEVELS]{};

  // OHLC candles (inline, last 1m candle)
  double candle_open_1m{0.0};

  double candle_high_1m{0.0};

  double candle_low_1m{0.0};

  double candle_close_1m{0.0};

  // Greeks (options instruments only)
  double iv{0.0};

  double delta{0.0};

  double gamma{0.0};

  double theta{0.0};

  double vega{0.0};

  // Timestamps (nanoseconds)
  uint64_t exchange_ts_ns{0}; // from exchange message

  uint64_t recv_ts_ns{0}; // local CLOCK_MONOTONIC_RAW

  int64_t latency_ns{0}; // recv - exchange (can be negative on drift)

  // flags
  bool is_stale{false};
  bool is_index{false};
  bool is_option{false};
  uint8_t _pad1[5]{};
};

// ─────────────────────────────────────────────────────────────
// Candle
// ─────────────────────────────────────────────────────────────

struct Candle {

  uint64_t instrument_token;

  uint64_t open_ts_ns;

  uint64_t close_ts_ns;

  uint32_t interval_sec; // 60 = 1m, 300 = 5m, 900 = 15m, 86400 = 1d

  double open;

  double high;

  double low;

  double close;

  uint64_t volume;

  uint64_t oi;
};

// ─────────────────────────────────────────────────────────────
// Order Request (strategy → risk → OMS)
// ─────────────────────────────────────────────────────────────

struct OrderRequest {

  uint64_t instrument_token;

  Side side{Side::BUY};

  OrderType order_type{OrderType::MARKET};

  OrderProduct product{OrderProduct::INTRADAY};

  OrderValidity validity{OrderValidity::DAY};

  int32_t quantity{0};

  double limit_price{0.0};

  double trigger_price{0.0};

  double stop_loss_price{0.0}; // for strategy tracking

  double take_profit_price{0.0};

  double signal_strength{0.0}; // for audit/analytics

  bool is_amo{false}; // After-Market Order

  bool slice{true}; // Auto-slice for freeze qty

  int32_t market_protection{-1}; // -1 = auto

  char strategy_id[MAX_STRATEGY_LEN]{};

  char tag[MAX_TAG_LEN]{};

  uint64_t request_ts_ns{0}; // time strategy generated this
};

// ─────────────────────────────────────────────────────────────
// Live Order (inside OMS order book)
// ─────────────────────────────────────────────────────────────

struct Order {

  char order_id[MAX_ORDER_ID_LEN]{}; // from Upstox

  char strategy_id[MAX_STRATEGY_LEN]{};

  char tag[MAX_TAG_LEN]{};

  uint64_t instrument_token{0};

  Side side{Side::BUY};

  OrderType order_type{OrderType::MARKET};

  OrderProduct product{OrderProduct::INTRADAY};

  OrderStatus status{OrderStatus::PENDING};

  int32_t quantity{0};

  int32_t filled_qty{0};

  double limit_price{0.0};

  double trigger_price{0.0};

  double avg_fill_price{0.0};

  uint64_t placed_ts_ns{0};

  uint64_t last_update_ns{0};
};

// ─────────────────────────────────────────────────────────────
// Fill
// ─────────────────────────────────────────────────────────────

struct Fill {

  char order_id[MAX_ORDER_ID_LEN]{};

  char trade_id[MAX_ORDER_ID_LEN]{};

  char strategy_id[MAX_STRATEGY_LEN]{};

  uint64_t instrument_token{0};

  Side side{Side::BUY};

  int32_t quantity{0};

  double price{0.0};

  uint64_t fill_ts_ns{0};

  [[nodiscard]]
  const char *side_str() const noexcept {
    return side == Side::BUY ? "BUY" : "SELL";
  }
};

// ─────────────────────────────────────────────────────────────
// Position
// ─────────────────────────────────────────────────────────────

struct Position {

  uint64_t instrument_token{0};

  char symbol[MAX_SYMBOL_LEN]{};

  char strategy_id[MAX_STRATEGY_LEN]{};

  int32_t quantity{0}; // negative = short

  double avg_price{0.0};

  double current_price{0.0};

  double realized_pnl{0.0};

  double unrealized_pnl{0.0};

  // Options greeks
  double net_delta{0.0};

  double net_gamma{0.0};

  double net_vega{0.0};

  double net_theta{0.0};

  [[nodiscard]]
  double value() const noexcept {
    return std::abs(quantity) * current_price;
  }

  [[nodiscard]]
  bool is_long() const noexcept {
    return quantity > 0;
  }

  [[nodiscard]]
  bool is_short() const noexcept {
    return quantity < 0;
  }

  [[nodiscard]]
  bool is_flat() const noexcept {
    return quantity == 0;
  }
};

// ─────────────────────────────────────────────────────────────
// Portfolio Snapshot (thread-safe read)
// ─────────────────────────────────────────────────────────────

struct PortfolioSnapshot {

  std::unordered_map<uint64_t, Position> positions;

  double total_realized_pnl{0.0};

  double total_unrealized_pnl{0.0};

  double available_capital{0.0};

  double used_margin{0.0};

  uint64_t snapshot_ts_ns{0};

  [[nodiscard]]
  Position get_position(uint64_t token) const {

    auto it = positions.find(token);

    return it != positions.end() ? it->second : Position{};
  }

  [[nodiscard]]
  double strategy_daily_pnl(const std::string &sid) const {
    double total = 0.0;

    for (const auto &[_, pos] : positions) {

      if (std::strncmp(pos.strategy_id, sid.c_str(), MAX_STRATEGY_LEN) == 0) {
        total += pos.realized_pnl + pos.unrealized_pnl;
      }
    }

    return total;
  }

  [[nodiscard]]
  double strategy_used_capital(const std::string &sid) const {
    double total = 0.0;

    for (const auto &[_, pos] : positions) {

      if (std::strncmp(pos.strategy_id, sid.c_str(), MAX_STRATEGY_LEN) == 0) {
        total += pos.value();
      }
    }

    return total;
  }
};

// ─────────────────────────────────────────────────────────────
// Strategy Config
// ─────────────────────────────────────────────────────────────

using ParamMap = std::unordered_map<std::string, double>;

struct RiskLimits {

  double max_single_order_value{10000.0};

  double max_position_value{50000.0};

  double max_capital_allocation{50000.0};

  double max_daily_loss{5000.0};

  double max_net_delta{500.0};

  double max_net_vega{10000.0};

  int max_orders_per_second{10};
};

struct StrategyConfig {

  std::string id;

  std::string name;

  std::string plugin_path; // .so path for dynamic loading

  std::vector<uint64_t> instruments; // instrument tokens

  int allocated_core{2}; // CPU core for this strategy thread

  RiskLimits risk;

  ParamMap params;
};

struct StrategyInfo {

  std::string id;

  std::string name;

  std::string version;

  std::string description;
};

// ─────────────────────────────────────────────────────────────
// Engine Config
// (parsed from config/engine.yaml)
// ─────────────────────────────────────────────────────────────

struct ThreadConfig {

  int market_data_io{0};

  int tick_normalizer{1};

  int risk_gate{6};

  int oms{7};

  int fill_processor{8};

  int portfolio{3};

  int kafka_publisher{4};

  int heartbeat{5};
};

struct EngineConfig {

  std::string upstox_access_token;

  std::string upstox_api_key;

  std::string upstox_redirect_uri;

  std::string redis_url{"redis://127.0.0.1:6379"};

  std::string kafka_brokers{"127.0.0.1:9092"};

  ThreadConfig threads;

  std::vector<StrategyConfig> strategies;

  bool paper_trading{true};

  bool dry_run{false};
};

// ─────────────────────────────────────────────────────────────
// Market OrderBook (raw depth)
// ─────────────────────────────────────────────────────────────

struct OrderBook {

  uint64_t instrument_token;

  uint64_t ts_ns;

  DepthLevel bid[DEPTH_LEVELS];

  DepthLevel ask[DEPTH_LEVELS];
};

// ─────────────────────────────────────────────────────────────
// Alert / kill signal
// ─────────────────────────────────────────────────────────────

enum class AlertLevel : uint8_t {

  DEBUG = 0,

  INFO = 1,

  WARN = 2,

  HIGH = 3,

  CRITICAL = 4
};

struct RiskAlert {

  AlertLevel level;

  char strategy_id[MAX_STRATEGY_LEN]{};

  char message[256]{};

  uint64_t ts_ns{0};
};

} // namespace qf