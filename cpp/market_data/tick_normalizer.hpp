// market_data/tick_normalizer.hpp
//
// Runs on Core 1 (SCHED_FIFO).
// Consumes raw NormalizedTick from the SPSC TickQueue written by the WS
// handler, applies price validation and stale checks, then fans out to:
//
// 1. TickDisruptor -> all strategy threads
// 2. CandleBuilder -> OHLCV aggregation
// 3. Kafka publish -> market.ticks (if enabled)
//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <thread>

#include "../common/disruptor.hpp"
#include "../common/logger.hpp"
#include "../common/spsc_queue.hpp"
#include "../common/types.hpp"
#include "candle_builder.hpp"

namespace qf {

class TickNormalizer {
public:
  using KafkaPublishFn = std::function<void(const NormalizedTick &)>;

  explicit TickNormalizer(TickQueue &raw_queue, TickDisruptor &disruptor,
                          CandleBuilder &candle_builder,
                          const EngineConfig &cfg);

  ~TickNormalizer();

  // Optionally set a Kafka publish callback.
  void set_kafka_fn(KafkaPublishFn fn) { kafka_fn_ = std::move(fn); }

  // Start the normalizer thread.
  void start();

  // Signal stop and join.
  void stop();

  // --- Stats ---

  [[nodiscard]]
  uint64_t ticks_processed() const noexcept {
    return ticks_processed_.load(std::memory_order_relaxed);
  }

  [[nodiscard]]
  uint64_t stale_dropped() const noexcept {
    return stale_dropped_.load(std::memory_order_relaxed);
  }

  [[nodiscard]]
  uint64_t sanity_rejected() const noexcept {
    return sanity_rejected_.load(std::memory_order_relaxed);
  }

private:
  void run();

  // Returns false if tick should be discarded.
  [[nodiscard]]
  bool validate(const NormalizedTick &t) const noexcept;

  TickQueue &raw_queue_;
  TickDisruptor &disruptor_;
  CandleBuilder &candle_builder_;
  const EngineConfig &cfg_;

  KafkaPublishFn kafka_fn_;

  std::thread thread_;
  std::atomic<bool> stop_{false};

  std::atomic<uint64_t> ticks_processed_{0};
  mutable std::atomic<uint64_t> stale_dropped_{0};
  mutable std::atomic<uint64_t> sanity_rejected_{0};

  static constexpr int64_t STALE_THRESHOLD_NS = 5'000'000'000LL; // 5s

  static constexpr double MAX_PRICE_MOVE_PCT = 0.20; // 20% from open
};

} // namespace qf