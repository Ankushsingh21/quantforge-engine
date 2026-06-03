// market_data/tick_normalizer.cpp

#include "tick_normalizer.hpp"
#include "../common/thread_utils.hpp"

namespace qf {

TickNormalizer::TickNormalizer(TickQueue &raw_queue, TickDisruptor &disruptor,
                               CandleBuilder &candle_builder,
                               const EngineConfig &cfg)

    : raw_queue_(raw_queue), disruptor_(disruptor),
      candle_builder_(candle_builder), cfg_(cfg) {}

TickNormalizer::~TickNormalizer() { stop(); }

void TickNormalizer::start() {

  stop_.store(false, std::memory_order_release);

  thread_ = std::thread([this] { run(); });
}

void TickNormalizer::stop() {

  stop_.store(true, std::memory_order_release);

  if (thread_.joinable())
    thread_.join();
}

void TickNormalizer::run() {

  setup_hot_thread(cfg_.threads.tick_normalizer, 70, "qf-normalizer");

  LOG_INFO("[Normalizer] Thread started on core {}",
           cfg_.threads.tick_normalizer);

  NormalizedTick tick;

  while (!stop_.load(std::memory_order_acquire)) {

    if (!raw_queue_.try_pop(tick)) {

      // Busy-spin in production; yield for laptop-friendly mode.

      std::this_thread::yield();

      continue;
    }

    if (!validate(tick)) {

      continue;
    }

    // Fan-out 1: Disruptor -> strategy threads

    disruptor_.push(tick);

    // Fan-out 2: CandleBuilder (1m, 5m, 15m)

    candle_builder_.on_tick(tick);

    // Fan-out 3: Kafka (optional, non-blocking)

    if (kafka_fn_) {

      kafka_fn_(tick);
    }

    ticks_processed_.fetch_add(1, std::memory_order_relaxed);
  }

  LOG_INFO("[Normalizer] Thread stopped. Processed {} ticks, dropped stale={}, "
           "sanity={}",
           ticks_processed_.load(), stale_dropped_.load(),
           sanity_rejected_.load());
}

bool TickNormalizer::validate(const NormalizedTick &t) const noexcept {

  // 1. Stale check

  if (t.is_stale) {

    stale_dropped_.fetch_add(1, std::memory_order_relaxed);

    return false;
  }

  // 2. LTP must be positive

  if (t.ltp <= 0.0) {

    sanity_rejected_.fetch_add(1, std::memory_order_relaxed);

    return false;
  }

  // 3. Price sanity vs open (if we have open price)

  if (t.open > 0.0) {

    double deviation = std::abs(t.ltp - t.open) / t.open;

    if (deviation > MAX_PRICE_MOVE_PCT) {

      // For circuit-limit stocks/indices this can be legitimate;
      // we flag but still forward (strategy decides).
      // For obviously corrupt data (e.g. 10x open), drop.

      if (deviation > 0.5) {

        sanity_rejected_.fetch_add(1, std::memory_order_relaxed);

        return false;
      }
    }
  }

  return true;
}

} // namespace qf