// transport/redis_client.cpp

#ifdef QUANTFORGE_REDIS_ENABLED

#include "redis_client.hpp"

#include <cstdarg>
#include <cstdio>
#include <regex>
#include <stdexcept>

namespace qf {

RedisClient::RedisClient(const std::string &url) {

  // Parse redis://[:password@]host:port

  std::string host = "127.0.0.1";
  int port = 6379;
  std::string password;

  std::regex re(R"(redis://(?:([^@]*)@)?([^:]+):(\d+))");

  std::smatch m;

  if (std::regex_match(url, m, re)) {
    password = m[1].str();
    host = m[2].str();
    port = std::stoi(m[3].str());
  }

  host_ = host;
  port_ = port;
  password_ = password;

  connect(host, port, password);
}

RedisClient::~RedisClient() {
  if (ctx_) {
    redisFree(ctx_);
    ctx_ = nullptr;
  }
}

bool RedisClient::reconnect() {
  if (ctx_) {
    redisFree(ctx_);
    ctx_ = nullptr;
  }

  connect(host_, port_, password_);
  return ctx_ != nullptr;
}

void RedisClient::connect(const std::string &host, int port,
                          const std::string &password) {
  struct timeval tv{2, 0}; // 2s connect timeout

  ctx_ = redisConnectWithTimeout(host.c_str(), port, tv);

  if (!ctx_ || ctx_->err) {

    LOG_ERROR("[Redis] Connect failed: {}",
              ctx_ ? ctx_->errstr : "null context");

    if (ctx_) {
      redisFree(ctx_);
      ctx_ = nullptr;
    }

    return;
  }

  // Set 1s read/write timeout
  struct timeval rw_tv{1, 0};

  redisSetTimeout(ctx_, rw_tv);

  if (!password.empty()) {

    redisReply *r = static_cast<redisReply *>(
        redisCommand(ctx_, "AUTH %s", password.c_str()));

    if (r)
      freeReplyObject(r);
  }

  LOG_INFO("[Redis] Connected to {}:{}", host, port);
}

// ── Public writes ────────────────────────────────────────────────

void RedisClient::publish_position(const std::string &sid,
                                   const Position &pos) {
  if (!ctx_)
    return;

  std::lock_guard lk(mtx_);

  std::string key = "positions:" + sid;
  std::string token_s = std::to_string(pos.instrument_token);

  // Pipeline: 4 HSET fields for this position

  redisPipelineAppend(ctx_, "HSET %s %s:qty %d", key.c_str(), token_s.c_str(),
                      pos.quantity);

  redisPipelineAppend(ctx_, "HSET %s %s:avg_price %.4f", key.c_str(),
                      token_s.c_str(), pos.avg_price);

  redisPipelineAppend(ctx_, "HSET %s %s:upnl %.4f", key.c_str(),
                      token_s.c_str(), pos.unrealized_pnl);

  redisPipelineAppend(ctx_, "HSET %s %s:rpnl %.4f", key.c_str(),
                      token_s.c_str(), pos.realized_pnl);
}

void RedisClient::publish_tick(const NormalizedTick &tick) {
  if (!ctx_)
    return;

  std::lock_guard lk(mtx_);

  std::string key = "ticks:" + std::to_string(tick.instrument_token);

  redisPipelineAppend(ctx_,
                      "HSET %s "
                      "ltp %.4f "
                      "volume %llu "
                      "bid1_price %.4f "
                      "bid1_qty %u "
                      "ask1_price %.4f "
                      "ask1_qty %u "
                      "ts %llu",

                      key.c_str(), tick.ltp, (unsigned long long)tick.volume,

                      tick.bid[0].price, tick.bid[0].qty,

                      tick.ask[0].price, tick.ask[0].qty,

                      (unsigned long long)tick.recv_ts_ns);
}

void RedisClient::publish_risk_counters(const std::string &sid,
                                        double daily_pnl, int order_count,
                                        double exposure) {
  if (!ctx_)
    return;

  std::lock_guard lk(mtx_);

  std::string key = "risk:" + sid;

  redisPipelineAppend(ctx_,
                      "HSET %s "
                      "daily_pnl %.4f "
                      "order_count %d "
                      "exposure %.4f",

                      key.c_str(), daily_pnl, order_count, exposure);
}

void RedisClient::set_kill_switch(bool engaged) {

  if (!ctx_)
    return;

  std::lock_guard lk(mtx_);

  redisPipelineAppend(ctx_, "SET kill_switch %d", engaged ? 1 : 0);
}

void RedisClient::flush() {

  if (!ctx_)
    return;

  std::lock_guard lk(mtx_);

  // Drain all pipeline replies

  redisReply *r = nullptr;

  while (redisGetReply(ctx_, (void **)&r) == REDIS_OK) {
    if (r)
      freeReplyObject(r);
  }

  // Reconnect on error

  if (ctx_->err) {

    LOG_WARN("[Redis] Pipeline flush error {} - reconnecting", ctx_->errstr);

    redisFree(ctx_);
    ctx_ = nullptr;

    connect(host_, port_, password_);
  }
}

} // namespace qf

#endif // QUANTFORGE_REDIS_ENABLED