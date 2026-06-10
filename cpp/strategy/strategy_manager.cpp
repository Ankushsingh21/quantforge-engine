// strategy/strategy_manager.cpp

#include "strategy_manager.hpp"

#include <dlfcn.h>
#include <stdexcept>

#include "../common/thread_utils.hpp"
#include "../oms/oms.hpp"
#include "../portfolio/portfolio_engine.hpp"
#include "../risk/pre_trade_risk.hpp"

namespace qf {

StrategyManager::StrategyManager(TickDisruptor &disruptor,
                                 PortfolioEngine &portfolio,
                                 PreTradeRiskEngine &risk,
                                 OrderManagementSystem &oms,
                                 const EngineConfig &cfg)
    : disruptor_(disruptor), portfolio_(portfolio), risk_(risk), oms_(oms),
      cfg_(cfg) {}

StrategyManager::~StrategyManager() { stop_all(); }

void StrategyManager::start_all() {
  for (const auto &scfg : cfg_.strategies) {
    try {
      load_strategy(scfg);
    } catch (const std::exception &e) {
      LOG_ERROR("[StrategyManager] Failed to load {}: {}", scfg.id, e.what());
    }
  }

  LOG_INFO("[StrategyManager] {} strategies loaded", strategies_.size());
}

void StrategyManager::stop_all() {
  for (auto &s : strategies_) {

    if (s->instance) {
      try {
        s->instance->on_stop();
      } catch (...) {
      }
    }

    s->running.store(false, std::memory_order_release);

    if (s->thread.joinable())
      s->thread.join();

    if (s->dl_handle) {
      dlclose(s->dl_handle);
      s->dl_handle = nullptr;
    }
  }

  strategies_.clear();
}

void StrategyManager::load_strategy(const StrategyConfig &cfg) {

  auto ls = std::make_unique<LoadedStrategy>();
  ls->config = cfg;

  // ── Dynamic load .so ─────────────────────────────

  dlerror(); // clear

  ls->dl_handle = dlopen(cfg.plugin_path.c_str(), RTLD_NOW | RTLD_LOCAL);

  if (!ls->dl_handle) {
    throw std::runtime_error(std::string("dlopen failed: ") + dlerror());
  }

  dlerror();

  auto *create_fn = reinterpret_cast<CreateStrategyFn>(
      dlsym(ls->dl_handle, "create_strategy"));

  const char *err = dlerror();

  if (err) {
    dlclose(ls->dl_handle);

    throw std::runtime_error(std::string("dlsym create_strategy: ") + err);
  }

  ls->instance.reset(create_fn());

  if (!ls->instance) {
    dlclose(ls->dl_handle);

    throw std::runtime_error("create_strategy() returned null");
  }

  ls->instance->set_strategy_id(cfg.id);

  // ── Build context ───────────────────────────────

  ls->context = build_context(*ls);

  ls->instance->inject_context(ls->context);

  // ── Spawn thread ────────────────────────────────

  LoadedStrategy *raw = ls.get();

  ls->running.store(true, std::memory_order_release);

  ls->thread = std::thread([this, raw] { run_strategy_thread(*raw); });

  std::lock_guard lk(mtx_);

  strategies_.push_back(std::move(ls));
}

StrategyContext StrategyManager::build_context(LoadedStrategy &s) {

  StrategyContext ctx;

  ctx.config = &s.config;

  ctx.submit_order = [this, &s](const OrderRequest &req) -> bool {
    // Route through risk gate first

    auto snapshot = portfolio_.get_snapshot();

    auto decision = risk_.check(req, s.config, snapshot);

    if (decision != RiskDecision::PASS) {

      LOG_WARN("[Strategy:{}] Order rejected by risk: {}", s.config.id,
               static_cast<int>(decision));

      return false;
    }

    return oms_.submit(req);
  };

  ctx.cancel_order = [this](const std::string &order_id) -> bool {
    return oms_.cancel(order_id);
  };

  ctx.get_position = [this](uint64_t token) -> Position {
    return portfolio_.get_position(token);
  };

  ctx.get_available_capital = [this]() -> double {
    return portfolio_.get_snapshot().available_capital;
  };

  ctx.get_last_tick = [this](uint64_t token) -> NormalizedTick {
    std::shared_lock lk(last_tick_mtx_);

    auto it = last_tick_cache_.find(token);

    return (it != last_tick_cache_.end()) ? it->second : NormalizedTick{};
  };

  ctx.now_ns = []() -> uint64_t {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
  };

  return ctx;
}

void StrategyManager::run_strategy_thread(LoadedStrategy &s) {

  setup_hot_thread(s.config.allocated_core, 60,
                   "qf-strat-" + s.config.id.substr(0, 10));

  LOG_INFO("[Strategy:{}] Thread started on core {}", s.config.id,
           s.config.allocated_core);

  // on_start

  try {
    s.instance->on_start(s.context);
  } catch (const std::exception &e) {

    LOG_ERROR("[Strategy:{}] on_start threw: {}", s.config.id, e.what());

    s.running.store(false, std::memory_order_release);

    return;
  }

  NormalizedTick tick;

  while (s.running.load(std::memory_order_acquire)) {

    if (disruptor_.try_consume(s.disruptor_seq, tick)) {

      // Keep last-tick cache current

      {
        std::unique_lock lk(last_tick_mtx_);

        last_tick_cache_[tick.instrument_token] = tick;
      }

      try {
        s.instance->on_tick(tick);
      } catch (const std::exception &e) {

        LOG_ERROR("[Strategy:{}] on_tick threw: {}", s.config.id, e.what());
      }

    } else {

      std::this_thread::yield();
    }
  }

  try {
    s.instance->on_stop();
  } catch (...) {
  }

  LOG_INFO("[Strategy:{}] Thread stopped", s.config.id);
}

void StrategyManager::dispatch_candle(const Candle &candle) {
  std::lock_guard lk(mtx_);

  for (auto &s : strategies_) {

    if (!s->running.load(std::memory_order_acquire))
      continue;

    // Check if strategy is subscribed to this instrument
    bool subscribed = s->config.instruments.empty();

    if (!subscribed) {
      for (auto t : s->config.instruments) {
        if (t == candle.instrument_token) {
          subscribed = true;
          break;
        }
      }
    }

    if (subscribed) {
      try {
        s->instance->on_candle(candle);
      } catch (...) {
      }
    }
  }
}

void StrategyManager::dispatch_fill(const Fill &fill) {
  std::lock_guard lk(mtx_);

  for (auto &s : strategies_) {

    if (std::strncmp(s->config.id.c_str(), fill.strategy_id,
                     MAX_STRATEGY_LEN) == 0) {

      try {
        s->instance->on_order_fill(fill);
      } catch (...) {
      }

      return;
    }
  }
}

void StrategyManager::halt_strategy(const std::string &id,
                                    const std::string &reason) {
  std::lock_guard lk(mtx_);

  for (auto &s : strategies_) {

    if (s->config.id == id) {

      LOG_WARN("[StrategyManager] Halting {} - {}", id, reason);

      try {
        s->instance->on_kill_signal();
      } catch (...) {
      }

      s->running.store(false, std::memory_order_release);

      return;
    }
  }
}

void StrategyManager::update_params(const std::string &id,
                                    const ParamMap &params) {
  std::lock_guard lk(mtx_);

  for (auto &s : strategies_) {

    if (s->config.id == id) {

      try {
        s->instance->update_params(params);
      } catch (...) {
      }

      return;
    }
  }
}

} // namespace qf