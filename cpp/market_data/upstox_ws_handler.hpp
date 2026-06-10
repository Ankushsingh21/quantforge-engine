// market_data/upstox_ws_handler.hpp
//
// Manages the Upstox V3 market-data WebSocket connection:
//  1. HTTP GET /v3/feed/market-data-feed/authorize -> WSS URL
//  2. Boost.Beast async WebSocket connect + TLS handshake
//  3. Send subscription request (JSON over binary frame)
//  4. Receive binary protobuf frames -> decode -> push to SPSC queue
//  5. Auto-reconnect with exponential backoff
//
// Runs entirely on one Boost.Asio io_context (pinned to Core 0).
//

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

#include "../common/logger.hpp"
#include "../common/spsc_queue.hpp"
#include "../common/types.hpp"
#include "MarketDataFeed.pb.h"
// Forward-declare generated proto class
// namespace com {
// namespace upstox {
// namespace marketdatafeeder {
// namespace rest {
// namespace proto {

// class MarketDataFeed;

// }
// } // namespace rest
// } // namespace marketdatafeeder
// } // namespace upstox
// } // namespace com

namespace qf {

class UpstoxWSHandler {
public:
  using WsResolver = boost::asio::ip::tcp::resolver;
  using SslCtx = boost::asio::ssl::context;
  using SslStream = boost::beast::ssl_stream<boost::beast::tcp_stream>;
  using WsStream = boost::beast::websocket::stream<SslStream>;

  explicit UpstoxWSHandler(boost::asio::io_context &ioc, TickQueue &tick_queue,
                           const EngineConfig &cfg);

  // Start connecting; non-blocking (uses ioc event loop).
  void start();

  // Subscribe to instruments (call after start, or any time).
  void subscribe(const std::vector<std::string> &instrument_keys,
                 const std::string &mode = "full");

  // Unsubscribe instruments.
  void unsubscribe(const std::vector<std::string> &instrument_keys);

  // Graceful shutdown.
  void stop();

  [[nodiscard]]
  bool is_connected() const noexcept {
    return connected_.load(std::memory_order_acquire);
  }

  [[nodiscard]]
  uint64_t total_messages() const noexcept {
    return msg_count_.load(std::memory_order_relaxed);
  }

  [[nodiscard]]
  uint64_t decode_errors() const noexcept {
    return decode_errors_.load(std::memory_order_relaxed);
  }

private:
  // ---------------- Connection sequence ----------------

  void do_authorize();

  void do_resolve(const std::string &host, const std::string &port);

  void do_connect(const WsResolver::results_type &endpoints, std::string path);

  void do_ssl_handshake(std::string path);

  void do_ws_handshake(std::string host, std::string path);

  void do_send_subscription();

  void do_read();

  // ---------------- Frame processing ----------------

  void on_frame(boost::beast::flat_buffer &buf);

  void decode_protobuf_frame(const uint8_t *data, size_t len);

  NormalizedTick translate_feed(uint64_t token,
                                const void *feed_msg, // typed inside .cpp
                                uint64_t recv_ts);

  // ---------------- Reconnect ----------------

  void schedule_reconnect();

  void do_reconnect();

  // ---------------- Auth helper ----------------

  std::string fetch_ws_url();

  boost::asio::io_context &ioc_;
  TickQueue &tick_queue_;
  const EngineConfig &cfg_;

  std::unique_ptr<SslCtx> ssl_ctx_;
  std::unique_ptr<WsStream> ws_;

  boost::asio::ip::tcp::resolver resolver_;
  boost::asio::steady_timer reconnect_timer_;

  std::string wss_url_; // obtained from /authorize
  std::string wss_host_;
  std::string wss_port_;
  std::string wss_path_;

  // Pending subscription state (sent on connection / re-connection)
  std::vector<std::string> subscribed_keys_;
  std::string subscribe_mode_{"full"};

  std::atomic<bool> connected_{false};
  std::atomic<bool> stop_{false};
  std::atomic<uint64_t> msg_count_{0};
  std::atomic<uint64_t> decode_errors_{0};

  int reconnect_delay_s_{1};

  static constexpr int MAX_RECONNECT_DELAY_S = 60;

  // Map instrument token -> instrument_key
  // (for reverse lookup in tick)
  std::unordered_map<uint64_t, std::string> token_to_key_;
};

} // namespace qf