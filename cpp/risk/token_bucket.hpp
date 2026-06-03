#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace qf {

class TokenBucketLimiter {
public:
  // max_rate = max tokens/sec
  // burst = max accumulated tokens

  explicit TokenBucketLimiter(int max_rate = 10, int burst = -1)
      : max_rate_(max_rate), burst_(burst < 0 ? max_rate * 2 : burst),
        tokens_(static_cast<double>(burst < 0 ? max_rate * 2 : burst)),
        last_refill_ns_(now_ns()) {}
  [[nodiscard]]
  bool try_consume() noexcept {

    uint64_t now = now_ns();

    double elapsed = static_cast<double>(now - last_refill_ns_.load(
                                                   std::memory_order_relaxed)) /
                     1e9;

    last_refill_ns_.store(now, std::memory_order_relaxed);

    // Refill

    double new_tokens =
        tokens_.load(std::memory_order_relaxed) + elapsed * max_rate_;

    new_tokens = std::min(new_tokens, static_cast<double>(burst_));

    tokens_.store(new_tokens, std::memory_order_relaxed);

    if (tokens_.load(std::memory_order_relaxed) >= 1.0) {
      tokens_.fetch_sub(1.0, std::memory_order_relaxed);

      return true;
    }

    return false;
  }
  void reset() noexcept {

    tokens_.store(static_cast<double>(burst_), std::memory_order_relaxed);

    last_refill_ns_.store(now_ns(), std::memory_order_relaxed);
  }

private:
  static uint64_t now_ns() noexcept {

    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
  }

  const int max_rate_;
  const int burst_;

  std::atomic<double> tokens_;
  std::atomic<uint64_t> last_refill_ns_;
};

} // namespace qf