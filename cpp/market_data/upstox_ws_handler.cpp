// market_data/upstox_ws_handler.cpp
//
// Implementation of UpstoxWSHandler.
// Uses Boost.Beast async WebSocket over TLS.
// Protobuf decoding uses the Upstox V3 MarketDataFeed proto generated code.
//

#include "upstox_ws_handler.hpp"

#include <chrono>
#include <cstring>
#include <functional>
#include <regex>
#include <sstream>

#include <boost/asio/strand.hpp>
#include <boost/beast/http.hpp>
#include <nlohmann/json.hpp>

// Include protobuf generated header (compiled from Upstox V3 proto).
// The .proto file is downloaded by proto/download_proto.py and compiled
// by CMakeLists.txt via protoc.
#include "MarketDataFeed.pb.h"

namespace qf {

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace ssl = net::ssl;

using tcp = net::ip::tcp;
using json = nlohmann::json;

static constexpr char API_HOST[] = "api.upstox.com";

static constexpr char AUTHORIZE_PATH[] = "/v3/feed/market-data-feed/authorize";

// --- Constructor ------------------------------------------------------------

UpstoxWSHandler::UpstoxWSHandler(net::io_context &ioc, TickQueue &tick_queue,
                                 const EngineConfig &cfg)

    : ioc_(ioc), tick_queue_(tick_queue), cfg_(cfg),
      ssl_ctx_(std::make_unique<ssl::context>(ssl::context::tlsv12_client)),
      resolver_(ioc), reconnect_timer_(ioc) {
  ssl_ctx_->set_default_verify_paths();
  ssl_ctx_->set_verify_mode(ssl::verify_peer);
}

// --- Public ----------------------------------------------------------------

void UpstoxWSHandler::start() {

  LOG_INFO("[WS] Starting market-data feed handler");

  do_authorize();
}

void UpstoxWSHandler::subscribe(const std::vector<std::string> &keys,
                                const std::string &mode) {
  subscribed_keys_ = keys;
  subscribe_mode_ = mode;

  // Build token map
  for (size_t i = 0; i < keys.size(); ++i) {

    uint64_t token = std::hash<std::string>{}(keys[i]);

    token_to_key_[token] = keys[i];
  }

  if (is_connected())
    do_send_subscription();
}

void UpstoxWSHandler::unsubscribe(const std::vector<std::string> &keys) {
  for (const auto &k : keys) {

    subscribed_keys_.erase(
        std::remove(subscribed_keys_.begin(), subscribed_keys_.end(), k),
        subscribed_keys_.end());

    uint64_t token = std::hash<std::string>{}(k);

    token_to_key_.erase(token);
  }
}

void UpstoxWSHandler::stop() {

  stop_.store(true, std::memory_order_release);

  reconnect_timer_.cancel();

  if (ws_ && is_connected()) {

    beast::error_code ec;

    ws_->close(beast::websocket::close_code::normal, ec);
  }
}

// --- Step 1: HTTP GET /v3/feed/market-data-feed/authorize ------------------
// Returns the WSS URL we must connect to.

std::string UpstoxWSHandler::fetch_ws_url() {

  try {

    tcp::resolver resolver(ioc_);

    beast::ssl_stream<beast::tcp_stream> stream(ioc_, *ssl_ctx_);

    // Set SNI
    if (!SSL_set_tlsext_host_name(stream.native_handle(), API_HOST)) {
      throw std::runtime_error("SSL SNI failed");
    }

    auto const results = resolver.resolve(API_HOST, "443");

    beast::get_lowest_layer(stream).connect(results);

    stream.handshake(ssl::stream_base::client);

    http::request<http::empty_body> req(http::verb::get, AUTHORIZE_PATH, 11);

    req.set(http::field::host, API_HOST);
    req.set(http::field::user_agent, "quantforge-engine/1.0");

    req.set(http::field::accept, "application/json");

    req.set("Authorization", "Bearer " + cfg_.upstox_access_token);

    req.set("Api-Version", "2.0");

    http::write(stream, req);

    beast::flat_buffer buf;

    http::response<http::string_body> res;

    http::read(stream, buf, res);

    if (res.result() != http::status::ok) {
      throw std::runtime_error("Authorize returned HTTP " +
                               std::to_string(res.result_int()));
    }

    auto j = json::parse(res.body());

    std::string authorized_redirect_uri =
        j.at("data").at("authorizedRedirectUri").get<std::string>();

    // Tear down HTTP connection
    beast::error_code ec;
    stream.shutdown(ec);

    return authorized_redirect_uri;

  } catch (const std::exception &e) {

    LOG_ERROR("[WS] Authorize failed: {}", e.what());

    return {};
  }
}
void UpstoxWSHandler::do_authorize() {

  LOG_INFO("[WS] Fetching WebSocket URL from /authorize");

  wss_url_ = fetch_ws_url();

  if (wss_url_.empty()) {

    LOG_ERROR("[WS] Empty WSS URL - scheduling reconnect");

    schedule_reconnect();

    return;
  }

  // Parse wss://hostname/path
  std::regex re(R"(wss://([^/]+)(/.*)$)");

  std::smatch m;

  if (!std::regex_search(wss_url_, m, re)) {

    LOG_ERROR("[WS] Cannot parse WSS URL: {}", wss_url_);

    schedule_reconnect();

    return;
  }

  wss_host_ = m[1].str();
  wss_path_ = m[2].str();
  wss_port_ = "443";

  LOG_INFO("[WS] Connecting to wss://{}{}", wss_host_, wss_path_);

  do_resolve(wss_host_, wss_port_);
}
// --- Step 2: Resolve host -----------------------------------------------

void UpstoxWSHandler::do_resolve(const std::string &host,
                                 const std::string &port) {
  resolver_.async_resolve(
      host, port,

      [this](beast::error_code ec, tcp::resolver::results_type results) {
        if (ec) {

          LOG_ERROR("[WS] Resolve failed: {}", ec.message());

          schedule_reconnect();

          return;
        }

        do_connect(results, wss_path_);
      });
}
// --- Step 3: TCP connect -----------------------------------------------

void UpstoxWSHandler::do_connect(const tcp::resolver::results_type &eps,
                                 std::string path) {
  ws_ = std::make_unique<WsStream>(ioc_, *ssl_ctx_);

  beast::get_lowest_layer(*ws_).async_connect(
      eps,

      [this, path = std::move(path)](
          beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
        if (ec) {

          LOG_ERROR("[WS] TCP connect failed: {}", ec.message());

          schedule_reconnect();

          return;
        }

        do_ssl_handshake(path);
      });
}
// --- Step 4: TLS handshake ---------------------------------------------

void UpstoxWSHandler::do_ssl_handshake(std::string path) {
  if (!SSL_set_tlsext_host_name(ws_->next_layer().native_handle(),
                                wss_host_.c_str())) {
    LOG_ERROR("[WS] SSL SNI error");

    schedule_reconnect();

    return;
  }

  ws_->next_layer().async_handshake(
      ssl::stream_base::client,

      [this, path = std::move(path)](beast::error_code ec) {
        if (ec) {

          LOG_ERROR("[WS] TLS handshake failed: {}", ec.message());

          schedule_reconnect();

          return;
        }

        do_ws_handshake(wss_host_, path);
      });
}

// --- Step 5: WebSocket upgrade -----------------------------------------

void UpstoxWSHandler::do_ws_handshake(std::string host, std::string path) {
  ws_->set_option(beast::websocket::stream_base::decorator(

      [&](beast::websocket::request_type &req) {
        req.set(http::field::user_agent, "quantforge-engine/1.0");

        req.set("Authorization", "Bearer " + cfg_.upstox_access_token);

        req.set("Api-Version", "2.0");
      }));

  ws_->binary(true); // all Upstox frames are binary

  ws_->read_message_max(1 * 1024 * 1024); // 1MB max frame

  ws_->async_handshake(

      host, path,

      [this](beast::error_code ec) {
        if (ec) {

          LOG_ERROR("[WS] WS upgrade failed: {}", ec.message());

          schedule_reconnect();

          return;
        }

        connected_.store(true, std::memory_order_release);

        reconnect_delay_s_ = 1; // reset backoff

        LOG_INFO("[WS] Connected to Upstox market-data feed");

        if (!subscribed_keys_.empty())
          do_send_subscription();

        do_read();
      });
}
// --- Step 6: Send subscription -----------------------------------------

void UpstoxWSHandler::do_send_subscription() {

  // Upstox V3 subscription payload (binary JSON)
  json payload = {

      {"guid", "quantforge-sub"},

      {"method", "sub"},

      {"data",
       {

           {"mode", subscribe_mode_}, // "full", "quote", "ltpc"

           {"instrumentKeys", subscribed_keys_}}}};

  std::string body = payload.dump();

  ws_->async_write(

      net::buffer(body),

      [this](beast::error_code ec, size_t) {
        if (ec) {

          LOG_ERROR("[WS] Subscribe write failed: {}", ec.message());

        } else {

          LOG_INFO("[WS] Subscribed to {} instruments (mode={})",
                   subscribed_keys_.size(), subscribe_mode_);
        }
      });
}
// --- Step 7: Read loop -------------------------------------------------

void UpstoxWSHandler::do_read() {

  auto buf = std::make_shared<beast::flat_buffer>();

  ws_->async_read(

      *buf,

      [this, buf](beast::error_code ec, size_t bytes_transferred) {
        if (stop_.load(std::memory_order_acquire))
          return;

        if (ec == beast::websocket::error::closed) {
          LOG_WARN("[WS] Connection closed by server");

          connected_.store(false, std::memory_order_release);

          schedule_reconnect();

          return;
        }

        if (ec) {

          LOG_ERROR("[WS] Read error: {}", ec.message());

          connected_.store(false, std::memory_order_release);

          schedule_reconnect();

          return;
        }

        // Process the binary protobuf frame
        const auto *data = static_cast<const uint8_t *>(buf->data().data());

        on_frame(*buf);

        buf->consume(bytes_transferred);

        // Continue reading
        do_read();
      });
}
// --- Protobuf decode ---------------------------------------------------

void UpstoxWSHandler::on_frame(beast::flat_buffer &buf) {
  const auto *data = static_cast<const uint8_t *>(buf.data().data());

  size_t len = buf.data().size();

  decode_protobuf_frame(data, len);
}
void UpstoxWSHandler::decode_protobuf_frame(const uint8_t *data, size_t len) {
  using namespace com::upstox::marketdatafeeder::rpc::proto;

  // Record receive timestamp IMMEDIATELY (first thing).
  uint64_t recv_ts = static_cast<uint64_t>(

      std::chrono::duration_cast<std::chrono::nanoseconds>(

          std::chrono::steady_clock::now().time_since_epoch())
          .count());

  MarketDataFeed feed;

  if (!feed.ParseFromArray(data, static_cast<int>(len))) {
    decode_errors_.fetch_add(1, std::memory_order_relaxed);

    LOG_WARN("[WS] Protobuf parse failed ({} bytes)", len);

    return;
  }

  for (const auto &[key, feed_msg] : feed.feeds()) {
    NormalizedTick tick{};

    tick.instrument_token = std::hash<std::string>{}(key);

    tick.recv_ts_ns = recv_ts;

    // Copy symbol (truncated to MAX_SYMBOL_LEN - 1)
    std::strncpy(tick.symbol, key.c_str(), sizeof(tick.symbol) - 1);

    bool have_data = false;

    if (feed_msg.has_ff()) {

      const auto &ff = feed_msg.ff();

      if (ff.has_market_ff()) {

        const auto &mff = ff.market_ff();

        if (mff.has_ltpc()) {

          tick.ltp = mff.ltpc().ltp();

          tick.prev_close = mff.ltpc().cp();

          tick.exchange_ts_ns =
              static_cast<uint64_t>(mff.ltpc().ltt() * 1'000'000ULL);

          have_data = true;
        }

        if (ohlc.ohlc_size() > 0) {

          auto &d = ohlc.ohlc(0);

          tick.open = d.open();
          tick.high = d.high();
          tick.low = d.low();
        }

        tick.volume = static_cast<uint64_t>(mff.vtt());

        tick.oi = static_cast<uint64_t>(mff.oi());
        // Depth
        for (int i = 0; i < std::min(mff.depth_size(), (int)DEPTH_LEVELS);
             ++i) {
          const auto &d = mff.depth(i);

          if (i < (int)DEPTH_LEVELS) {

            tick.bid[i].price = d.bid_price();
            tick.bid[i].qty = d.bid_quantity();
            tick.bid[i].orders = d.bid_orders();

            tick.ask[i].price = d.ask_price();
            tick.ask[i].qty = d.ask_quantity();
            tick.ask[i].orders = d.ask_orders();
          }
        }
      }
    } else if (feed_msg.has_ltl()) {

      // LTP-only feed
      const auto &ltl = feed_msg.ltl();

      if (ltl.has_ltpc()) {

        tick.ltp = ltl.ltpc().ltp();

        tick.prev_close = ltl.ltpc().cp();

        tick.exchange_ts_ns =
            static_cast<uint64_t>(ltl.ltpc().ltt()) * 1'000'000ULL;

        have_data = true;
      }
    }

    if (!have_data)
      continue;

    // Staleness check: if exchange_ts is more than 5s
    // behind recv_ts, mark stale
    if (tick.exchange_ts_ns > 0) {

      tick.latency_ns = static_cast<int64_t>(recv_ts) -
                        static_cast<int64_t>(tick.exchange_ts_ns);

      tick.is_stale = (tick.latency_ns > 5'000'000'000LL);
    }

    if (!tick_queue_.push(tick)) {

      LOG_WARN("[WS] Tick queue full, dropping tick for {}", key);
    }

    msg_count_.fetch_add(1, std::memory_order_relaxed);
  }
}
// --- Reconnect ---------------------------------------------------------

void UpstoxWSHandler::schedule_reconnect() {

  if (stop_.load(std::memory_order_acquire))
    return;

  connected_.store(false, std::memory_order_release);

  LOG_INFO("[WS] Reconnecting in {}s", reconnect_delay_s_);

  reconnect_timer_.expires_after(std::chrono::seconds(reconnect_delay_s_));

  reconnect_timer_.async_wait(

      [this](beast::error_code ec) {
        if (ec == net::error::operation_aborted)
          return;

        do_reconnect();
      });

  reconnect_delay_s_ = std::min(reconnect_delay_s_ * 2, MAX_RECONNECT_DELAY_S);
}

void UpstoxWSHandler::do_reconnect() {

  ws_.reset();

  do_authorize();
}
} // namespace qf