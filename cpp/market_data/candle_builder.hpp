// market_data/candle_builder.hpp
//
// Aggregates NormalizedTicks into OHLCV candles for all configured intervals.
// Thread-safe for single writer (TickNormalizer thread) + multiple readers
// (strategy threads via const callbacks).
//
// Supported intervals: 60s (1m), 300s (5m), 900s (15m), 3600s (1h)
//

#pragma once

#include <array>
#include <functional>
#include <mutex>
#include <unordered_map>

#include "../common/logger.hpp"
#include "../common/types.hpp"

namespace qf {

class CandleBuilder {
public:
  using CandleCallback = std::function<void(const Candle &)>;

  explicit CandleBuilder() = default;

  // Register a callback to receive completed candles.
  // Safe to call before start; callbacks fired on TickNormalizer thread.

  void register_callback(CandleCallback cb) {

    std::lock_guard lk(cb_mtx_);

    callbacks_.push_back(std::move(cb));
  }

  // Called by TickNormalizer for every validated tick.

  void on_tick(const NormalizedTick &tick) {

    for (auto interval : INTERVALS) {

      update_bar(tick, interval);
    }
  }

  // Force-close all open bars (EOD).

  void flush_all(uint64_t ts_ns = 0) {

    uint64_t now =
        ts_ns ? ts_ns
              : static_cast<uint64_t>(

                    std::chrono::duration_cast<std::chrono::nanoseconds>(

                        std::chrono::system_clock::now().time_since_epoch())

                        .count());

    for (auto &[key, bar] : open_bars_) {

      if (bar.open > 0)
        emit_candle(bar, now);
    }

    open_bars_.clear();
  }

private:
  struct BarState {

    uint64_t instrument_token{0};

    uint64_t bar_start_ns{0};

    uint32_t interval_sec{0};

    double open{0}, high{0}, low{0}, close{0};

    uint64_t volume{0}, oi{0};

    bool initialized{false};
  };

  // Key: (instrument_token, interval_sec)

  struct BarKey {

    uint64_t token;

    uint32_t interval;

    bool operator==(const BarKey &o) const noexcept {

      return token == o.token && interval == o.interval;
    }
  };

  struct BarKeyHash {

    size_t operator()(const BarKey &k) const noexcept {

      return k.token ^ (static_cast<size_t>(k.interval) << 32);
    }
  };

  void update_bar(const NormalizedTick &tick, uint32_t interval_sec) {
    // Round exchange timestamp down to the bar boundary.

    uint64_t ts =
        tick.exchange_ts_ns > 0 ? tick.exchange_ts_ns : tick.recv_ts_ns;

    uint64_t interval_ns =
        static_cast<uint64_t>(interval_sec) * 1'000'000'000ULL;

    uint64_t bar_start = (ts / interval_ns) * interval_ns;

    BarKey key{tick.instrument_token, interval_sec};

    auto &bar = open_bars_[key];

    if (!bar.initialized || bar.bar_start_ns != bar_start) {
      // Emit completed bar (if any)

      if (bar.initialized && bar.open > 0) {

        emit_candle(bar, bar.bar_start_ns + interval_ns - 1);
      }

      // Start new bar

      bar.instrument_token = tick.instrument_token;

      bar.bar_start_ns = bar_start;

      bar.interval_sec = interval_sec;

      bar.open = tick.ltp;
      bar.high = tick.ltp;
      bar.low = tick.ltp;
      bar.close = tick.ltp;

      bar.volume = tick.volume;

      bar.oi = tick.oi;

      bar.initialized = true;

      return;
    }

    // Update existing bar

    bar.high = std::max(bar.high, tick.ltp);

    bar.low = std::min(bar.low, tick.ltp);

    bar.close = tick.ltp;

    if (tick.volume > bar.volume)
      bar.volume = tick.volume; // cumulative

    bar.oi = tick.oi;
  }

  void emit_candle(const BarState &bar, uint64_t close_ts_ns) {
    Candle c;

    c.instrument_token = bar.instrument_token;

    c.open_ts_ns = bar.bar_start_ns;

    c.close_ts_ns = close_ts_ns;

    c.interval_sec = bar.interval_sec;

    c.open = bar.open;

    c.high = bar.high;

    c.low = bar.low;

    c.close = bar.close;

    c.volume = bar.volume;

    c.oi = bar.oi;

    std::lock_guard lk(cb_mtx_);

    for (auto &cb : callbacks_)
      cb(c);
  }

  static constexpr std::array<uint32_t, 4> INTERVALS{60, 300, 900, 3600};

  std::unordered_map<BarKey, BarState, BarKeyHash> open_bars_;

  std::vector<CandleCallback> callbacks_;

  mutable std::mutex cb_mtx_;
};

} // namespace qf