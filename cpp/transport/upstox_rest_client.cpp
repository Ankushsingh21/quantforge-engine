// transport/upstox_rest_client.cpp

#include "upstox_rest_client.hpp"

#include <nlohmann/json.hpp>

namespace qf {

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;
using json = nlohmann::json;

UpstoxRestClient::UpstoxRestClient(net::io_context &ioc,
                                   const std::string &token, bool paper_mode)
    : ioc_(ioc), access_token_(token), paper_mode_(paper_mode) {}

// ── Public API ─────────────────────────────────────────────

void UpstoxRestClient::place_order(const OrderRequest &req,
                                   const std::string &internal_oid,
                                   SuccessCb on_success, ErrorCb on_error) {
  if (paper_mode_) {

    // Paper mode: echo back a fake response with the internal_oid as
    // exchange_id

    if (on_success)
      on_success("{\"data\":{\"order_ids\":[\"PAPER-" + internal_oid +
                 "\"]},\"status\":\"success\"}");

    return;
  }

  std::string body = build_place_order_body(req, internal_oid);

  http::request<http::string_body> hreq{http::verb::post, "/v3/order/place",
                                        11};

  hreq.set(http::field::host, HFT_HOST);
  hreq.set(http::field::content_type, "application/json");
  hreq.set(http::field::user_agent, "quantforge-engine/1.0");
  hreq.set("Authorization", "Bearer " + access_token_);

  hreq.body() = body;
  hreq.prepare_payload();

  do_request(HFT_HOST, PORT, std::move(hreq), std::move(on_success),
             std::move(on_error));
}

void UpstoxRestClient::cancel_order(const std::string &exchange_oid,
                                    SuccessCb on_success, ErrorCb on_error) {
  if (paper_mode_) {
    if (on_success)
      on_success("{}");
    return;
  }

  std::string target = "/v3/order/cancel?order_id=" + exchange_oid;

  http::request<http::string_body> hreq{http::verb::delete_, target, 11};

  hreq.set(http::field::host, HFT_HOST);
  hreq.set(http::field::user_agent, "quantforge-engine/1.0");
  hreq.set("Authorization", "Bearer " + access_token_);

  hreq.prepare_payload();

  do_request(HFT_HOST, PORT, std::move(hreq), std::move(on_success),
             std::move(on_error));
}

void UpstoxRestClient::modify_order(const std::string &exchange_oid,
                                    double new_price, int new_qty,
                                    SuccessCb on_success, ErrorCb on_error) {
  if (paper_mode_) {
    if (on_success)
      on_success("{}");
    return;
  }

  json body_j = {{"order_id", exchange_oid},
                 {"price", new_price},
                 {"quantity", new_qty},
                 {"validity", "DAY"}};

  http::request<http::string_body> hreq{http::verb::put, "/v3/order/modify",
                                        11};

  hreq.set(http::field::host, HFT_HOST);
  hreq.set(http::field::content_type, "application/json");
  hreq.set(http::field::user_agent, "quantforge-engine/1.0");
  hreq.set("Authorization", "Bearer " + access_token_);

  hreq.body() = body_j.dump();
  hreq.prepare_payload();

  do_request(HFT_HOST, PORT, std::move(hreq), std::move(on_success),
             std::move(on_error));
}

void UpstoxRestClient::get_positions(SuccessCb on_success, ErrorCb on_error) {
  http::request<http::string_body> hreq{
      http::verb::get, "/v3/portfolio/short-term-positions", 11};

  hreq.set(http::field::host, API_HOST);
  hreq.set(http::field::user_agent, "quantforge-engine/1.0");
  hreq.set("Authorization", "Bearer " + access_token_);

  hreq.prepare_payload();

  do_request(API_HOST, PORT, std::move(hreq), std::move(on_success),
             std::move(on_error));
}

void UpstoxRestClient::get_funds(SuccessCb on_success, ErrorCb on_error) {
  http::request<http::string_body> hreq{http::verb::get,
                                        "/v3/user/get-funds-and-margin", 11};

  hreq.set(http::field::host, API_HOST);
  hreq.set(http::field::user_agent, "quantforge-engine/1.0");
  hreq.set("Authorization", "Bearer " + access_token_);

  hreq.prepare_payload();

  do_request(API_HOST, PORT, std::move(hreq), std::move(on_success),
             std::move(on_error));
}

// ── Private ────────────────────────────────────────────────

std::string
UpstoxRestClient::build_place_order_body(const OrderRequest &req,
                                         const std::string &tag) const {
  auto side_str = [](Side s) { return s == Side::BUY ? "BUY" : "SELL"; };

  auto type_str = [](OrderType t) {
    switch (t) {
    case OrderType::MARKET:
      return "MARKET";

    case OrderType::LIMIT:
      return "LIMIT";

    case OrderType::SL:
      return "SL";

    case OrderType::SLM:
      return "SL-M";

    default:
      return "MARKET";
    }
  };

  auto prod_str = [](OrderProduct p) {
    switch (p) {
    case OrderProduct::INTRADAY:
      return "I";

    case OrderProduct::DELIVERY:
      return "D";

    case OrderProduct::MTF:
      return "MTF";

    default:
      return "I";
    }
  };

  // Upstox V3 requires instrument_key (string), not just token.
  // The engine.yaml stores instrument keys; here we use token as a proxy.
  // In production, maintain a token -> key map and look up here.

  json body = {{"quantity", req.quantity},
               {"product", prod_str(req.product)},
               {"validity", req.validity == OrderValidity::IOC ? "IOC" : "DAY"},
               {"price", req.limit_price},
               {"tag", std::string(req.tag) + "-" + tag},
               {"instrument_token", std::to_string(req.instrument_token)},
               {"order_type", type_str(req.order_type)},
               {"transaction_type", side_str(req.side)},
               {"disclosed_quantity", 0},
               {"trigger_price", req.trigger_price},
               {"is_amo", req.is_amo},
               {"slice", req.slice}};

  return body.dump();
}

void UpstoxRestClient::do_request(const std::string &host,
                                  const std::string &port,
                                  http::request<http::string_body> req,
                                  SuccessCb on_success, ErrorCb on_error) {
  // Spawn a coroutine-style async chain using Asio lambdas

  auto ssl_ctx = std::make_shared<ssl::context>(ssl::context::tlsv12_client);

  ssl_ctx->set_default_verify_paths();
  ssl_ctx->set_verify_mode(ssl::verify_peer);

  auto stream = std::make_shared<SslStream>(ioc_, *ssl_ctx);

  auto resolver = std::make_shared<tcp::resolver>(ioc_);

  auto hreq =
      std::make_shared<http::request<http::string_body>>(std::move(req));

  resolver->async_resolve(
      host, port,
      [this, host, stream, ssl_ctx, resolver, hreq,
       on_success = std::move(on_success), on_error = std::move(on_error)](
          beast::error_code ec, tcp::resolver::results_type results) mutable {
        if (ec) {
          if (on_error)
            on_error("Resolve: " + ec.message());
          return;
        }

        if (!SSL_set_tlsext_host_name(stream->native_handle(), host.c_str())) {
          if (on_error)
            on_error("SSL SNI failed");
          return;
        }

        beast::get_lowest_layer(*stream).async_connect(
            results, [stream, ssl_ctx, hreq, on_success = std::move(on_success),
                      on_error = std::move(on_error)](beast::error_code ec2,
                                                      auto) mutable {
              if (ec2) {
                if (on_error)
                  on_error("Connect: " + ec2.message());
                return;
              }

              stream->async_handshake(
                  ssl::stream_base::client,
                  [stream, ssl_ctx, hreq, on_success = std::move(on_success),
                   on_error =
                       std::move(on_error)](beast::error_code ec3) mutable {
                    if (ec3) {
                      if (on_error)
                        on_error("TLS: " + ec3.message());
                      return;
                    }

                    http::async_write(
                        *stream, *hreq,
                        [stream, ssl_ctx, on_success = std::move(on_success),
                         on_error = std::move(on_error)](beast::error_code ec4,
                                                         size_t) mutable {
                          if (ec4) {
                            if (on_error)
                              on_error("Write: " + ec4.message());
                            return;
                          }

                          auto buf = std::make_shared<beast::flat_buffer>();

                          auto res = std::make_shared<
                              http::response<http::string_body>>();

                          http::async_read(
                              *stream, *buf, *res,
                              [stream, ssl_ctx, buf, res,
                               on_success = std::move(on_success),
                               on_error = std::move(on_error)](
                                  beast::error_code ec5, size_t) mutable {
                                if (ec5) {
                                  if (on_error)
                                    on_error("Read: " + ec5.message());
                                  return;
                                }

                                int status = res->result_int();

                                if (status >= 200 && status < 300) {
                                  if (on_success)
                                    on_success(res->body());
                                } else {
                                  if (on_error)
                                    on_error("HTTP " + std::to_string(status) +
                                             ": " + res->body());
                                }

                                // Graceful shutdown
                                beast::error_code ec6;
                                stream->shutdown(ec6);
                              });
                        });
                  });
            });
      });
}
} // namespace qf