// ems/ems.hpp

#pragma once

// ems/ems.hpp — Execution Management System
//
// Thin layer between OMS and the exchange REST API.
// Receives OrderRequest + internal_oid from OMS,
// maps instrument tokens to Upstox instrument keys,
// and submits via UpstoxRestClient.
//
// Also owns the order-status WebSocket (Upstox portfolio feed)
// which delivers ACK/FILL/REJECT callbacks.

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <boost/asio.hpp>

#include "../common/logger.hpp"
#include "../common/types.hpp"
#include "../transport/upstox_rest_client.hpp"

namespace qf {

class ExecutionManagementSystem {

public:
  using AckCb = std::function<void(const std::string &exchange_order_id)>;

  using RejectCb = std::function<void(const std::string &reason)>;

  using FillCb =
      std::function<void(const std::string &exchange_order_id, double price,
                         int qty, const std::string &trade_id)>;

  ExecutionManagementSystem(boost::asio::io_context &ioc,
                            const EngineConfig &cfg);

  // Register instrument_token -> Upstox instrument_key mapping

  void register_instruments(
      const std::unordered_map<uint64_t, std::string> &token_to_key);

  // Submit a new order (called by OMS)

  void place_order(const OrderRequest &req, const std::string &internal_oid,
                   AckCb on_ack, RejectCb on_reject);

  // Cancel order (called by OMS)

  void cancel_order(const std::string &exchange_order_id);

  // Register fill callback (installed by OMS)

  void set_fill_callback(FillCb cb) { fill_cb_ = std::move(cb); }

  // Called on fill events from order-feed WebSocket

  void on_fill_event(const std::string &exchange_oid, double price, int qty,
                     const std::string &trade_id);

private:
  std::string resolve_instrument_key(uint64_t token) const;

  boost::asio::io_context &ioc_;

  const EngineConfig &cfg_;

  UpstoxRestClient rest_client_;

  std::unordered_map<uint64_t, std::string> token_to_key_;

  mutable std::mutex key_mtx_;

  FillCb fill_cb_;
};

} // namespace qf