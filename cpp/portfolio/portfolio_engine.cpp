// portfolio/portfolio_engine.cpp

#include "portfolio_engine.hpp"
#include "../common/thread_utils.hpp"

namespace qf {

PortfolioEngine::PortfolioEngine(FillQueue &fill_queue, const EngineConfig &cfg)
    : fill_queue_(fill_queue), cfg_(cfg),
      starting_capital_(100000.0) // default; overridden by load_positions
{
  snapshot_.available_capital = starting_capital_;
}

PortfolioEngine::~PortfolioEngine() { stop(); }

void PortfolioEngine::start() {
  stop_.store(false, std::memory_order_release);
  thread_ = std::thread([this] { fill_loop(); });
}

void PortfolioEngine::stop() {
  stop_.store(true, std::memory_order_release);
  if (thread_.joinable())
    thread_.join();
}

//-- Fill processing loop and logic------------------------------

void PortfolioEngine::fill_loop() {

  setup_hot_thread(cfg_.threads.fill_processor, 55, "qf-portfolio");
  LOG_INFO("[Portfolio] Fill-processor thread started");

  Fill fill;

  while (!stop_.load(std::memory_order_acquire)) {

    if (fill_queue_.try_pop(fill)) {
      process_fill(fill);
    } else {
      std::this_thread::yield();
    }
  }

  LOG_INFO("[Portfolio] Fill-processor thread stopped");
}

//-- Fill Application ------------------------------------

void PortfolioEngine::process_fill(const Fill &fill) {
  std::unique_lock lk(snapshot_mtx_);

  auto &pos = snapshot_.positions[fill.instrument_token];
  if (pos.instrument_token == 0) {
    pos.instrument_token = fill.instrument_token;

    std::strncpy(pos.strategy_id, fill.strategy_id, MAX_STRATEGY_LEN - 1);
  }

  int qty_delta = (fill.side == Side::BUY) ? fill.quantity : -fill.quantity;

  if (pos.quantity == 0) {

    // Opening a new position
    pos.quantity = qty_delta;
    pos.avg_price = fill.price;
  } else if ((qty_delta > 0 && pos.quantity > 0) ||
             (qty_delta < 0 && pos.quantity < 0)) {

    // Adding to an existing position (VWAP update)

    double total_value = std::abs(pos.quantity) * pos.avg_price +
                         std::abs(qty_delta) * fill.price;

    pos.quantity += qty_delta;

    pos.avg_price =
        (pos.quantity != 0) ? total_value / std::abs(pos.quantity) : 0.0;
  } else {

    // Reducing / closing a position

    int close_qty = std::min(std::abs(qty_delta), std::abs(pos.quantity));

    double realized =
        close_qty * (fill.side == Side::SELL ? (fill.price - pos.avg_price)
                                             : (pos.avg_price - fill.price));

    pos.realized_pnl += realized;

    snapshot_.total_realized_pnl += realized;

    pos.quantity += qty_delta;

    if (pos.quantity == 0) {
      pos.avg_price = 0.0;
    }
  }
  // Update available capital
  // (crude estimate: subtract/add fill value)

  double fill_value = fill.quantity * fill.price;

  if (fill.side == Side::BUY) {
    snapshot_.available_capital -= fill_value;
  } else {
    snapshot_.available_capital += fill_value;
  }

  snapshot_.snapshot_ts_ns = fill.fill_ts_ns;

  lk.unlock();
  LOG_DEBUG("[Portfolio] Fill applied: {} {} {} qty={} @ {:.2f} | pos={} "
            "avg={:.2f} rpnl={:.2f}",
            fill.strategy_id, fill.side == Side::BUY ? "BUY" : "SELL",
            fill.instrument_token, fill.quantity, fill.price,
            snapshot_.positions[fill.instrument_token].quantity,
            snapshot_.positions[fill.instrument_token].avg_price,
            snapshot_.positions[fill.instrument_token].realized_pnl);

  emit_update();
}

//-- Price Update ------------------------------------
void PortfolioEngine::update_price(uint64_t token, double price) {

  std::unique_lock lk(snapshot_mtx_);

  auto it = snapshot_.positions.find(token);

  if (it == snapshot_.positions.end())
    return;

  auto &pos = it->second;

  pos.current_price = price;

  if (pos.quantity != 0 && pos.avg_price > 0) {

    double sign = pos.quantity > 0 ? 1.0 : -1.0;

    pos.unrealized_pnl =
        sign * std::abs(pos.quantity) * (price - pos.avg_price);
  }

  // Recalculate total unrealized PnL

  double total_upnl = 0;

  for (const auto &[_, p] : snapshot_.positions)
    total_upnl += p.unrealized_pnl;

  snapshot_.total_unrealized_pnl = total_upnl;
}

//-- Sanpshot accessors ------------------------

PortfolioSnapshot PortfolioEngine::get_snapshot() const {
  std::shared_lock lk(snapshot_mtx_);
  return snapshot_;
}

Position PortfolioEngine::get_position(uint64_t token) const {
  std::shared_lock lk(snapshot_mtx_);
  auto it = snapshot_.positions.find(token);
  if (it == snapshot_.positions.end())
    return {};
  return it->second;
}

double PortfolioEngine::available_capital() const {
  std::shared_lock lk(snapshot_mtx_);
  return snapshot_.available_capital;
}

// Lisfecycle and utility methods------------------------------

void PortfolioEngine::load_positions(const std::vector<Position> &positions) {
  std::unique_lock lk(snapshot_mtx_);
  for (const auto &p : positions) {
    snapshot_.positions[p.instrument_token] = p;
  }
  LOG_INFO("[Portfolio] Loaded {} positions from broker", positions.size());
}

void PortfolioEngine::emit_update() {
  if (!update_cb_)
    return;

  PortfolioSnapshot snap;

  {
    std::shared_lock lk(snapshot_mtx_);
    snap = snapshot_;
  }

  update_cb_(snap);
}
} // namespace qf