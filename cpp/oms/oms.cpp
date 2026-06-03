// oms/oms.cpp

#include "oms.hpp"

#include <chrono>
#include <iomanip>
#include <sstream>

#include "../ems/ems.hpp"

namespace qf {

OrderManagementSystem::OrderManagementSystem(ExecutionManagementSystem &ems,
                                             FillQueue &fill_queue,
                                             const EngineConfig &cfg)
    : ems_(ems), fill_queue_(fill_queue), cfg_(cfg),
      paper_mode_(cfg.paper_trading) {}

bool OrderManagementSystem::submit(const OrderRequest &req) {

  PooledOrder *po = pool_.acquire();

  if (!po) {
    LOG_ERROR("[OMS] Order pool exhausted!");
    return false;
  }

  std::string oid = generate_order_id();

  // Populate the order record
  Order &o = po->order;

  std::strncpy(o.order_id, oid.c_str(), MAX_ORDER_ID_LEN - 1);

  std::strncpy(o.strategy_id, req.strategy_id, MAX_STRATEGY_LEN - 1);

  std::strncpy(o.tag, req.tag, MAX_TAG_LEN - 1);

  o.instrument_token = req.instrument_token;
  o.side = req.side;
  o.order_type = req.order_type;
  o.product = req.product;
  o.quantity = req.quantity;
  o.limit_price = req.limit_price;
  o.trigger_price = req.trigger_price;

  o.status = OrderStatus::PENDING;

  o.placed_ts_ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());

  {
    std::lock_guard lk(orders_mtx_);
    orders_[oid] = po;
  }

  if (paper_mode_) {

    // Paper mode: immediately simulate a fill
    LOG_INFO("[OMS][PAPER] {} {} {} qty={} price={:.2f}", oid, req.strategy_id,
             req.side == Side::BUY ? "BUY" : "SELL", req.quantity,
             req.limit_price);

    // Simulate ACK + fill
    on_order_ack("PAPER-" + oid, oid);

    on_fill("PAPER-" + oid, req.limit_price > 0 ? req.limit_price : 0.0,
            req.quantity, "PAPER-TRD-" + oid);

    return true;
  }

  // Live mode: send to EMS for async REST submission
  ems_.place_order(
      req, oid,

      [this, oid](const std::string &exchange_oid) {
        on_order_ack(exchange_oid, oid);
      },

      [this, oid](const std::string &reason) {
        on_reject("", reason);

        // Return order to pool
        std::lock_guard lk(orders_mtx_);

        auto it = orders_.find(oid);

        if (it != orders_.end()) {
          pool_.release(it->second);
          orders_.erase(it);
        }
      });

  LOG_DEBUG("[OMS] Submitted {} - {} {} qty={}", oid, req.strategy_id,
            req.side == Side::BUY ? "BUY" : "SELL", req.quantity);

  return true;
}

bool OrderManagementSystem::cancel(const std::string &order_id) {

  std::string exchange_oid;

  {
    std::lock_guard lk(orders_mtx_);

    auto it = orders_.find(order_id);

    if (it == orders_.end())
      return false;

    auto *po = it->second;

    if (po->sm.current() != OrderStatus::OPEN &&
        po->sm.current() != OrderStatus::PARTIAL) {
      return false;
    }

    // Find exchange OID
    for (auto &[exoid, iid] : exchange_to_internal_) {
      if (iid == order_id) {
        exchange_oid = exoid;
        break;
      }
    }
  }

  if (exchange_oid.empty())
    return false;

  ems_.cancel_order(exchange_oid);

  return true;
}

void OrderManagementSystem::on_order_ack(const std::string &exchange_oid,
                                         const std::string &internal_oid) {
  std::lock_guard lk(orders_mtx_);

  auto it = orders_.find(internal_oid);
  if (it == orders_.end())
    return;

  exchange_to_internal_[exchange_oid] = internal_oid;

  it->second->sm.transition(OrderStatus::OPEN);
  it->second->order.status = OrderStatus::OPEN;

  LOG_DEBUG("[OMS] ACK {} -> exchange_id={}", internal_oid, exchange_oid);
}
void OrderManagementSystem::on_fill(const std::string &exchange_oid,
                                    double fill_price, int fill_qty,
                                    const std::string &trade_id) {
  // Deduplication guard
  if (!trade_id.empty() && !dedup_.try_mark(trade_id)) {
    return; // duplicate fill event
  }

  std::string internal_oid;
  PooledOrder *po = nullptr;

  {
    std::lock_guard lk(orders_mtx_);

    auto eit = exchange_to_internal_.find(exchange_oid);

    if (eit == exchange_to_internal_.end()) {

      // Paper mode uses "PAPER-{oid}" directly
      internal_oid =
          exchange_oid.size() > 6 ? exchange_oid.substr(6) : exchange_oid;
    } else {
      internal_oid = eit->second;
    }

    auto it = orders_.find(internal_oid);
    if (it == orders_.end())
      return;

    po = it->second;
  }

  // Build fill
  Fill fill;

  std::strncpy(fill.order_id, internal_oid.c_str(), MAX_ORDER_ID_LEN - 1);

  std::strncpy(fill.trade_id, trade_id.c_str(), MAX_ORDER_ID_LEN - 1);

  std::strncpy(fill.strategy_id, po->order.strategy_id, MAX_STRATEGY_LEN - 1);

  fill.instrument_token = po->order.instrument_token;
  fill.side = po->order.side;
  fill.quantity = fill_qty;

  fill.price = fill_price > 0 ? fill_price : po->order.limit_price;

  fill.fill_ts_ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());

  // Update order
  {
    std::lock_guard lk(orders_mtx_);

    po->order.filled_qty += fill_qty;
    po->order.avg_fill_price = fill.price;
    po->order.last_update_ns = fill.fill_ts_ns;

    bool fully_filled = (po->order.filled_qty >= po->order.quantity);

    po->sm.transition(fully_filled ? OrderStatus::COMPLETE
                                   : OrderStatus::PARTIAL);

    po->order.status =
        fully_filled ? OrderStatus::COMPLETE : OrderStatus::PARTIAL;

    if (po->sm.is_terminal()) {

      pool_.release(po);

      orders_.erase(internal_oid);

      exchange_to_internal_.erase(exchange_oid);
    }
  }

  // Push to fill queue
  if (!fill_queue_.push(fill)) {
    LOG_WARN("[OMS] Fill queue full, processing fill synchronously");
  }

  if (fill_cb_)
    fill_cb_(fill);

  LOG_INFO("[OMS] FILL {} {} {} qty={} @ {:.2f}", internal_oid,
           fill.strategy_id, fill.side == Side::BUY ? "BUY" : "SELL", fill_qty,
           fill.price);
}

void OrderManagementSystem::on_reject(const std::string &exchange_oid,
                                      const std::string &reason) {
  std::string internal_oid = exchange_oid;

  {
    std::lock_guard lk(orders_mtx_);

    auto eit = exchange_to_internal_.find(exchange_oid);

    if (eit != exchange_to_internal_.end())
      internal_oid = eit->second;

    auto it = orders_.find(internal_oid);

    if (it != orders_.end()) {

      it->second->sm.transition(OrderStatus::REJECTED);

      pool_.release(it->second);

      orders_.erase(it);

      exchange_to_internal_.erase(exchange_oid);
    }
  }

  if (reject_cb_)
    reject_cb_(internal_oid, reason);

  LOG_WARN("[OMS] REJECT {}: {}", internal_oid, reason);
}
void OrderManagementSystem::cancel_all_open() {
  std::vector<std::string> oids;

  {
    std::lock_guard lk(orders_mtx_);

    for (auto &[oid, po] : orders_) {

      if (po->sm.current() == OrderStatus::OPEN ||
          po->sm.current() == OrderStatus::PARTIAL) {
        oids.push_back(oid);
      }
    }
  }

  for (const auto &oid : oids)
    cancel(oid);

  LOG_INFO("[OMS] Cancelled {} open orders (EOD)", oids.size());
}
size_t OrderManagementSystem::open_order_count() const {
  std::lock_guard lk(orders_mtx_);

  size_t count = 0;

  for (const auto &[_, po] : orders_) {
    if (!po->sm.is_terminal())
      ++count;
  }

  return count;
}
void OrderManagementSystem::log_open_orders() const {
  std::lock_guard lk(orders_mtx_);

  for (const auto &[oid, po] : orders_) {

    const auto &o = po->order;

    LOG_INFO("[OMS] Open order: {} | {} | {} {} qty={} filled={}", oid,
             o.strategy_id, o.side == Side::BUY ? "BUY" : "SELL",
             o.instrument_token, o.quantity, o.filled_qty);
  }
}
std::string OrderManagementSystem::generate_order_id() {
  // Format: QF-{timestamp_ms}-{sequence}

  uint64_t ts_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());

  uint64_t seq = seq_.fetch_add(1, std::memory_order_relaxed);

  std::ostringstream oss;

  oss << "QF-" << ts_ms << "-" << seq;

  return oss.str();
}
} // namespace qf