// oms/fill_deduplicator.hpp
//
// Guards against duplicate fill events from the exchange.
// Upstox can send the same fill_id twice on network glitches.
// Without deduplication, positions become double-counted.
//
// Uses a flat unordered_set with shared_mutex for safe concurrent access.
// EOD: call reset() to clear (fills from today don't matter tomorrow).
//

#pragma once

#include <shared_mutex>
#include <string>
#include <unordered_set>

#include "../common/logger.hpp"

namespace qf {

class FillDeduplicator {
public:
  // Returns true if the fill_id is NEW and should be processed.
  // Returns false if it was already seen (duplicate -> discard).
  [[nodiscard]]
  bool try_mark(const std::string &trade_id) {

    // Fast path: shared lock read
    {
      std::shared_lock rl(mtx_);

      if (processed_.count(trade_id)) {
        LOG_WARN("[FillDedup] Duplicate fill detected: {}", trade_id);

        duplicates_++;
        return false;
      }
    }

    // Slow path: exclusive write
    std::unique_lock wl(mtx_);

    auto [_, inserted] = processed_.insert(trade_id);

    if (!inserted) {
      duplicates_++;
      return false;
    }

    return true;
  }

  // Call at EOD or on reconnect
  // (fills from previous session irrelevant).
  void reset() {
    std::unique_lock wl(mtx_);
    processed_.clear();
    duplicates_ = 0;
  }

  [[nodiscard]]
  size_t unique_fills() const {
    return processed_.size();
  }

  [[nodiscard]]
  size_t duplicate_count() const {
    return duplicates_;
  }

private:
  mutable std::shared_mutex mtx_;
  std::unordered_set<std::string> processed_;
  size_t duplicates_{0};
};

} // namespace qf