// ems/ems.cpp

#include "ems.hpp"

#include <nlohmann/json.hpp>

namespace qf {

using json = nlohmann::json;

ExecutionManagementSystem::ExecutionManagementSystem(
    boost::asio::io_context &ioc, const EngineConfig &cfg)
    : ioc_(ioc), cfg_(cfg),
      rest_client_(ioc, cfg.upstox_access_token, cfg.paper_trading) {}

void ExecutionManagementSystem::register_instruments(
    const std::unordered_map<uint64_t, std::string> &token_to_key) {
  std::lock_guard lk(key_mtx_);

  token_to_key_ = token_to_key;

  LOG_INFO("[EMS] Registered {} instruments", token_to_key_.size());
}

void ExecutionManagementSystem::place_order(const OrderRequest &req,
                                            const std::string &internal_oid,
                                            AckCb on_ack, RejectCb on_reject) {
  // In paper mode, rest_client_ echoes immediately — no IO occurs.

  rest_client_.place_order(
      req, internal_oid,

      [on_ack = std::move(on_ack)](const std::string &body) {
        try {

          auto j = json::parse(body);

          // V3 response: data.order_ids is an array (support sliced array)
          //  use the first order_id as the exchange reference for this request.
          const auto $ids = j.at("data").at("order_ids");
          std::string exchange_oid = ids.at(0).get<std::string>();
          if (on_ack)
            on_ack(exchange_oid);

        } catch (const std::exception &e) {

          LOG_ERROR("[EMS] Failed to parse place_order response: {}", e.what());
        }
      },

      [on_reject = std::move(on_reject)](const std::string &err) {
        LOG_ERROR("[EMS] place_order error: {}", err);

        if (on_reject)
          on_reject(err);
      });
}

void ExecutionManagementSystem::cancel_order(
    const std::string &exchange_order_id) {
  rest_client_.cancel_order(

      exchange_order_id,

      [exchange_order_id](const std::string &) {
        LOG_INFO("[EMS] Cancel ACK for {}", exchange_order_id);
      },

      [exchange_order_id](const std::string &err) {
        LOG_WARN("[EMS] Cancel failed for {}: {}", exchange_order_id, err);
      });
}

void ExecutionManagementSystem::on_fill_event(const std::string &exchange_oid,
                                              double price, int qty,
                                              const std::string &trade_id) {
  if (fill_cb_) {

    fill_cb_(exchange_oid, price, qty, trade_id);
  }
}

std::string
ExecutionManagementSystem::resolve_instrument_key(uint64_t token) const {
  std::lock_guard lk(key_mtx_);

  auto it = token_to_key_.find(token);

  if (it == token_to_key_.end()) {
    return std::to_string(token);
  }

  return it->second;
}

} // namespace qf