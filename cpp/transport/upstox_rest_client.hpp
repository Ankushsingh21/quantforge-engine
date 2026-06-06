// transport/upstox_rest_client.hpp
//
// Async Upstox REST API client (Boost.Beast + Boost.Asio).
// Handles:
//   - POST   /v3/order/place      (HFT endpoint: api-hft.upstox.com)
//   - DELETE /v3/order/cancel
//   - GET    /v3/portfolio/positions
//   - GET    /v3/user/funds-and-margin
//
// All requests are non-blocking (async). Callbacks are invoked on the
// io_context thread, so they must be fast.
//

#pragma once

#include <functional>
#include <memory>
#include <string>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

#include "../common/logger.hpp"
#include "../common/types.hpp"

namespace qf {

class UpstoxRestClient {
public:
  using SuccessCb = std::function<void(const std::string &response_body)>;
  using ErrorCb = std::function<void(const std::string &error_msg)>;

  explicit UpstoxRestClient(boost::asio::io_context &ioc,
                            const std::string &access_token,
                            bool paper_mode = true);

  // ── Order API ─────────────────────────────────────────────────────

  void place_order(const OrderRequest &req,
                   const std::string &internal_order_id, SuccessCb on_success,
                   ErrorCb on_error);

  void cancel_order(const std::string &exchange_order_id,
                    SuccessCb on_success = {}, ErrorCb on_error = {});

  void modify_order(const std::string &exchange_order_id, double new_price,
                    int new_qty, SuccessCb on_success = {},
                    ErrorCb on_error = {});

  // ── Portfolio API ────────────────────────────────────────────────

  void get_positions(SuccessCb on_success, ErrorCb on_error = {});

  void get_funds(SuccessCb on_success, ErrorCb on_error = {});

  // ── Auth ─────────────────────────────────────────────────────────

  void set_access_token(const std::string &token) { access_token_ = token; }

private:
  using SslCtx = boost::asio::ssl::context;
  using SslStream = boost::beast::ssl_stream<boost::beast::tcp_stream>;

  void
  do_request(const std::string &host, const std::string &port,
             boost::beast::http::request<boost::beast::http::string_body> req,
             SuccessCb on_success, ErrorCb on_error);

  std::string build_place_order_body(const OrderRequest &req,
                                     const std::string &internal_id) const;

  boost::asio::io_context &ioc_;
  std::string access_token_;
  bool paper_mode_;

  static constexpr char HFT_HOST[] = "api-hft.upstox.com";
  static constexpr char API_HOST[] = "api.upstox.com";
  static constexpr char PORT[] = "443";
  static constexpr char API_VER[] = "2.0";
};

} // namespace qf