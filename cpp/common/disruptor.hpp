// common/disruptor.hpp — LMAX Disruptor-style ring buffer for fan-out.
//
// One producer (TickNormalizer) publishes ticks; N consumers (strategy threads)
// each maintain an independent sequence cursor. No locks, no copies — every
// strategy reads the same slot in place.
//
// Thread safety: single producer, multiple concurrent consumers.
//

#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <vector>

#include "types.hpp"

namespace qf {

class TickDisruptor {
public:
  static constexpr size_t BUFFER_SIZE = 1 << 16; // 65 536 slots
  static constexpr size_t MASK = BUFFER_SIZE - 1;

  struct alignas(64) Slot {
    NormalizedTick tick;
    std::atomic<int64_t> sequence{-1};
  };

  TickDisruptor() = default;

  // — Producer API ------------------------------------------------

  // Step 1: claim the next publish slot; returns its sequence number.
  int64_t claim_next() noexcept {
    return producer_seq_.fetch_add(1, std::memory_order_acq_rel) + 1;
  }

  // Step 2: write tick and make it visible to consumers.
  void publish(int64_t seq, const NormalizedTick &tick) noexcept {
    auto &slot = buffer_[seq & MASK];

    slot.tick = tick;

    slot.sequence.store(seq, std::memory_order_release);
  }

  // Convenience: claim + publish in one call.
  void push(const NormalizedTick &tick) noexcept {
    int64_t seq = claim_next();
    publish(seq, tick);
  }

  // — Consumer API ------------------------------------------------

  // Non-blocking: returns true if a new tick was available.
  // consumer_seq: in/out — caller holds its own cursor (start at -1).
  bool try_consume(int64_t &consumer_seq, NormalizedTick &out) noexcept {
    int64_t next = consumer_seq + 1;

    auto &slot = buffer_[next & MASK];

    if (slot.sequence.load(std::memory_order_acquire) < next)
      return false; // not published yet

    out = slot.tick;
    consumer_seq = next;

    return true;
  }

  // — Housekeeping ------------------------------------------------

  [[nodiscard]] int64_t current_producer_seq() const noexcept {
    return producer_seq_.load(std::memory_order_acquire);
  }

  // How far is a consumer behind? (for monitoring)
  [[nodiscard]] int64_t lag(int64_t consumer_seq) const noexcept {
    return current_producer_seq() - consumer_seq;
  }

private:
  alignas(64) std::array<Slot, BUFFER_SIZE> buffer_{};

  alignas(64) std::atomic<int64_t> producer_seq_{-1};
};

} // namespace qf