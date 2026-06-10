// transport/redis_client.hpp
//
// Writes live state to Redis (hiredis).
// Used by PortfolioEngine to publish positions after every fill.
// Used by PreTradeRiskEngine to publish risk counters.
//
// All writes are FIRE-AND-FORGET (async-ish via pipelining).
// No read path — reads happen on the Python analytics side.
//
// Keys written:
//
//   HASH positions:{strategy_id}
//        -> {token:qty, token:avg_price, token:upnl, ...}
//
//   HASH ticks:{token}
//        -> {ltp, volume, bid1, ask1, ts}
//
//   HASH risk:{strategy_id}
//        -> {daily_pnl, order_count, exposure}
//
//   STRING kill_switch
//        -> "0" or "1"
//

#pragma once

#ifdef QUANTFORGE_REDIS_ENABLED

#include <hiredis/async.h>
#include <hiredis/hiredis.h>
#include <memory>
#include <mutex>
#include <string>

#include "../common/logger.hpp"
#include "../common/types.hpp"

namespace qf {

class RedisClient {
public:
  explicit RedisClient(const std::string &url = "redis://127.0.0.1:6379");

  ~RedisClient();

  // Non-copyable
  RedisClient(const RedisClient &) = delete;
  RedisClient &operator=(const RedisClient &) = delete;

  [[nodiscard]]
  bool is_connected() const noexcept {
    return ctx_ != nullptr;
  }

  bool reconnect();

  // ── Writes ───────────────────────────────────────────────────────

  void publish_position(const std::string &strategy_id, const Position &pos);

  void publish_tick(const NormalizedTick &tick);

  void publish_risk_counters(const std::string &strategy_id, double daily_pnl,
                             int order_count, double exposure);

  void set_kill_switch(bool engaged);

  // Flush pipelined commands
  void flush();

private:
  void connect(const std::string &host, int port,
               const std::string &password = {});

  void cmd(const char *fmt, ...);

  redisContext *ctx_{nullptr};

  std::string host_{"127.0.0.1"};
  int port_{6379};
  std::string password_;

  mutable std::mutex mtx_;
};

} // namespace qf

#else

// Stub when hiredis is not available

namespace qf {

struct Position;
struct NormalizedTick;

class RedisClient {
public:
  explicit RedisClient(const std::string & = {}) {}

  [[nodiscard]]
  bool is_connected() const noexcept {
    return false;
  }

  bool reconnect() { return false; }

  void publish_position(const std::string &, const Position &) {}

  void publish_tick(const NormalizedTick &) {}

  void publish_risk_counters(const std::string &, double, int, double) {}

  void set_kill_switch(bool) {}

  void flush() {}
};

} // namespace qf

#endif // QUANTFORGE_REDIS_ENABLED