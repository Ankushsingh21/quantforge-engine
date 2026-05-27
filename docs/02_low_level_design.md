# 02 - Low-Level design

## 2.1 thread Model (C++ Core)

```
Main Process: quantforge-engine

├── Thread: MarketData-IO (pinned to Core 0)
│   Upstox WebSocket I/O (Boost.Asio)
│   Writes to: SPSC Ring Buffer [ticks]
│
├── Thread: TickNormalizer (pinned to Core 1)
│   Reads from: SPSC Ring Buffer [ticks]
│   Normalizes: timestamps, instrument tokens
│   Writes to: SPMC Disruptor [normalized_ticks]
│
├── Thread Pool: StrategyWorkers (Core 2-5, isolated)
│   Each strategy runs on dedicated thread
│   Reads from: Disruptor [normalized_ticks]
│   Writes to: SPSC Queue [order_requests]
│
├── Thread: RiskGate (pinned to Core 6)
│   Reads from: SPSC Queue [order_requests]
│   Validates: exposure, limits, throttle
│   Writes to: SPSC Queue [approved_orders]
│
├── Thread: OMS-Executor (pinned to Core 7)
│   Reads from: SPSC Queue [approved_orders]
│   Submits via: Upstox REST (async)
│   Manages: order state machine
│
├── Thread: FillProcessor (pinned to Core 8)
│   Processes: fill callbacks from Upstox
│   Updates: PortfolioEngine
│   Publishes to: Kafka [fills.events]
│
├── Thread: PortfolioUpdater
│   Maintains: net positions, cash, PnL
│   Syncs to: Redis (async RESP3 pipeline)
│
├── Thread: Kafka-Publisher (background)
│   Drains: Kafka publish queue
│   Batches events for throughput
│
└── Thread: Heartbeat + Monitor
    Emits: health metrics to Prometheus
    Monitors: all queues for backpressure
```

---

## 2.2 Lock-Free Queues & Ring Buffers

### SPSC Ring Buffer (Single Producer Single Consumer)

Used for : MarkerData-IO -> tickNormalizer piepline

```cpp
// cpp/common/spsc_queue.hpp


#pragma once
#include <atomic>
#include <array>
#include <optional>

template<typename T, size_t Capacity>
class SPSCQueue {
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be power of 2");

public:

    bool push(const T& item) noexcept {
        const size_t write = write_pos_.load(
            std::memory_order_relaxed);

        const size_t next = (write + 1) & mask_;

        if (next == read_pos_.load(
                std::memory_order_acquire))
            return false;

        data_[write] = item;

        write_pos_.store(
            next,
            std::memory_order_release);

        return true;
    }

    std::optional<T> pop() noexcept {

        const size_t read =
            read_pos_.load(
                std::memory_order_relaxed);

        if (read ==
            write_pos_.load(
                std::memory_order_acquire))
            return std::nullopt;

        T item = data_[read];

        read_pos_.store(
            (read + 1) & mask_,
            std::memory_order_release);

        return item;
    }

    size_t size() const noexcept {

        const size_t w =
            write_pos_.load(
                std::memory_order_acquire);

        const size_t r =
            read_pos_.load(
                std::memory_order_acquire);

        return (w - r) & mask_;
    }

private:

    static constexpr size_t mask_ =
        Capacity - 1;

    alignas(64)
    std::atomic<size_t> write_pos_{0};

    alignas(64)
    std::atomic<size_t> read_pos_{0};

    std::array<T, Capacity> data_;
};

// Usage 64k tick buffer, cache-line aligned
using TickQueue =
    SPSCQueue<NormalizedTick, 65536>;
```

### Disruptor Pattern (SPMC - One tick to all strategies)

```cpp
// cpp/common/disruptor.hpp
// Lmax disruptor-style ring buffer for fan-out
// Each Consumer (strategy) maintains its own sequence Cursor
// No locks, only atomic CAS on sequesnce number
class TickDisruptor {

    static constexpr size_t BUFFER_SIZE = 1 << 16;  // 65536
    static constexpr size_t MASK = BUFFER_SIZE - 1;

    struct alignas(64) Slot {
        NormalizedTick tick;
        std::atomic<int64_t> sequence{-1};
    };

    alignas(64)
    std::array<Slot, BUFFER_SIZE> buffer_;

    alignas(64)
    std::atomic<int64_t> producer_seq_{-1};

public:

    int64_t claim_next() {
        return producer_seq_.fetch_add(
            1,
            std::memory_order_acq_rel) + 1;
    }

    void publish(
        int64_t seq,
        const NormalizedTick& tick) {

        auto& slot =
            buffer_[seq & MASK];

        slot.tick = tick;

        slot.sequence.store(
            seq,
            std::memory_order_release);
    }

    bool try_consume(
        int64_t& consumer_seq,
        NormalizedTick& out) {

        int64_t next =
            consumer_seq + 1;

        auto& slot =
            buffer_[next & MASK];

        if (slot.sequence.load(
                std::memory_order_acquire)
            < next)
            return false;

        out = slot.tick;
        consumer_seq = next;

        return true;
    }
};
```

---

## 2.3 Memory mangement

## Arena Allocator for Hot Path

```cpp

// cpp/common/arena_allocator.hpp
//pre-allcoated pool to eliminate heap allication in hot path

class ArenaAllocator {

    static constexpr size_t POOL_SIZE =
        64 * 1024 * 1024; // 64MB

    alignas(64)
    uint8_t pool_[POOL_SIZE];

    std::atomic<size_t> offset_{0};

public:

    void* allocate(
        size_t size,
        size_t align = 8) noexcept {

        size_t aligned_size =
            (size + align - 1)
            & ~(align - 1);

        size_t pos =
            offset_.fetch_add(
                aligned_size,
                std::memory_order_relaxed);

        if (pos + aligned_size >
            POOL_SIZE)
            return nullptr;

        return pool_ + pos;
    }

    void reset() noexcept {
        offset_.store(
            0,
            std::memory_order_release);
    }
};

// thread-local arena per strategy thread
thread_local ArenaAllocator g_strategy_arena;

```

### Object pool for Orders

```cpp
// cpp/oms/order_pool.hpp
template<size_t PoolSize = 10000>
class OrderPool {
    std::array<Order, PoolSize> pool_;
    std::stack<Order*> free_list_;
    std::mutex mtx_;

public:

    OrderPool() {
        for (auto& o : pool_) free_list_.push(&o);
    }

    Order* acquire() {

        std::lock_guard lk(mtx_);

        if (free_list_.empty())
            return nullptr;

        auto* o = free_list_.top();
        free_list_.pop();
        return o;
    }

    void release(Order* o) {
        o->reset();
        std::lock_guard lk(mtx_);
        free_list_.push(o);
    }
};
```

## 2.4 Shared Memory & IPC

### Between C++ processess (if split into separate executanles)

```cpp
// cpp/transport/shm_channel.hpp
// POSIX shared memory for ultra-low IPC between processess

#include <sys/mman.h>
#include <fcntl.h>

struct SHMHeader {

    alignas(64)
    std::atomic<uint64_t> write_seq{0};

    alignas(64)
    std::atomic<uint64_t> read_seq{0};

    alignas(64)
    uint8_t data[0];//flexibe array
};

class SHMChannel {

    static constexpr size_t SHM_SIZE = 256 * 1024 * 1024; //256MB
    SHMHeader* header_{nullptr};
    int fd_{-1};

public:

    bool open(
        const char* name,
        bool create) {

        int flags =
            create
                ? (O_CREAT | O_RDWR)
                : O_RDWR;

        fd_ = shm_open(
            name,
            flags,
            0666);

        if (fd_ < 0)
            return false;

        if (create)
            ftruncate(fd_, SHM_SIZE);

        void* ptr =
            mmap(nullptr,
                 SHM_SIZE,
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED,
                 fd_,
                 0);

        if (ptr == MAP_FAILED)
            return false;

        header_ =
            static_cast<SHMHeader*>(ptr);

        return true;
    }
};
```

### Named Pipes for python <-> C++ signaling

```python
# Python/utils/ipc_client.py
# Python writes ML signals via named FIFO -> C++ reads in real time

import os
import struct
import json
class IPCSignalWriter:

    def __init__(
        self,
        pipe_path="/tmp/qf_signals"
    ):

        if not os.path.exists(pipe_path):
            os.mkfifo(pipe_path)

        self.pipe = open(
            pipe_path,
            "wb",
            buffering=0
        )

    def write_signal(
        self,
        instrument_token: int,
        signal: float,
        ts_ns: int
    ):

        payload = struct.pack(
            "<IQd",
            instrument_token,
            ts_ns,
            signal
        )

        self.pipe.write(payload)
```

---

## 2.5 WebSocket Processing (C++)

```cpp
// cpp/market_data/upstox_ws_handler.hpp
#include <boost/beast/webscoket.hpp>
#include <boost/asio.hpp>
#include <google/protobuf/...>
class UpstoxWebSocketHandler {

    using tcp =
        boost::asio::ip::tcp;

    using ws_stream =
        boost::beast::websocket::stream<
            boost::asio::ssl::stream<
                tcp::socket>>;

    boost::asio::io_context ioc_;

    boost::asio::ssl::context ssl_ctx_{
        boost::asio::ssl::context::tls_client
    };

    std::unique_ptr<ws_stream> ws_;

    TickQueue& tick_queue_;

    std::atomic<bool> connected_{false};

    std::atomic<bool> should_reconnect_{true};

    uint32_t reconnect_delay_ms_{1000};

    uint32_t max_reconnect_delay_ms_{30000};

    std::chrono::steady_clock::time_point
        last_heartbeat_;

public:

    UpstoxWebSocketHandler(
        TickQueue& queue,
        const Config& cfg)
        : tick_queue_(queue) {}

    void start() {
        // run io_context on decated thread pinnned to core 0
        boost::asio::executor_work_guard
            work_guard(
                ioc_.get_executor());

        io_thread_ =
            std::thread([this] {

                pin_to_core(0);

                ioc_.run();
            });

        connect();
    }

    void connect() {

        // Resolve
        // Connect
        // SSL handshake
        // WS upgrade
        // Subscribe instruments on successful connection

        do_connect();
    }

    void on_message(
        boost::beast::flat_buffer&
            buffer) {

        const auto ts_recv =
            get_monotonic_ns(); // capture recv timestamp FIRTST
        // upstox sends binary protobuf over WS
        auto data =
            buffer.data();

        MarketDataFeed proto_feed;

        if (!proto_feed.ParseFromArray(
                data.data(),
                data.size())) {

            metrics_.parse_errors.fetch_add(
                1,
                std::memory_order_relaxed);

            return;
        }
    }
};
```

---

## 2.6 Event_driven Architecture

## Core Event Types

```cpp
// cpp/common/events.hpp

#pragma once

enum class EventType : uint8_t {
    TICK,
    DEPTH,
    OHLC,
    ORDER_NEW,
    ORDER_ACK,
    ORDER_FILL,
    ORDER_CANCEL,
    ORDER_REJECT,
    RISK_ALERT,
    RISK_KILL,
    HEARTBEAT,
    MARKET_OPEN,
    MARKET_CLOSE,
    EOD_SNAPSHOT,
    CONFIG_UPDATE,
};

struct Event {
    EventType type;
    uint64_t timestamp_ns;
    uint32_t source_id;
    uint64_t sequence_no;

    union Payload {
        NormalizedTick tick;
        OrderEvent order;
        RiskEvent risk;
        HeartbeatEvent heartbeat;
    } payload;
};




//Event handlerinterface

class IEventHandler {
public:
    virtual ~IEventHandler() = default;

    virtual void on_tick(const NormalizedTick&) = 0;
    virtual void on_depth(const OrderBook&) = 0;
    virtual void on_fill(const Fill&) = 0;
    virtual void on_order_ack(const OrderAck&) = 0;
    virtual void on_order_reject(const Reject&) = 0;
    virtual void on_risk_alert(const RiskAlert&) = 0;
    virtual void on_market_open() = 0;
    virtual void on_market_close() = 0;
};

```

---

## 2.7 Order State Machine

```

PENDING
   |
submit()
   v
SUBMITTED
   |
 ack() -----------------> OPEN
   |                        |
 reject()                   |
   v                        |
REJECTED (terminal)         |
                            |
                  +---------+---------+
                  |                   |
               fill()         cancelRequest()
                  |                   |
                  v                   v
            FILLED(term)      CANCEL_SENT
                                   |
                              cancelled()
                                   |
                                   v
                           CANCELLED(term)

OPEN -> partial_fill() -> PARTIAL -> fill() -> FILLED
```

```cpp
// cpp/oms/order_state_machine.hpp
class Order {
public:

    enum class State {
        PENDING,
        SUBMITTED,
        OPEN,
        PARTIAL,
        FILLED,
        CANCELLED,
        REJECTED
    };

    State state() const noexcept {
        return state_.load(std::memory_order_acquire);
    }

    bool transition(State expected, State next) {
        return state_.compare_exchange_strong(
            expected,
            next,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

    void on_ack(const std::string& exchange_order_id) {
        if (!transition(State::SUBMITTED, State::OPEN)) {
            log_warn(
                "Invalid state transition: SUBMITTED->OPEN, current={}",
                state_to_string(state()));
        }

        exchange_order_id_ = exchange_order_id;
        ack_ts_ns_ = get_monotonic_ns();
    }

    void on_fill(double fill_price, int fill_qty) {
        std::lock_guard lk(mtx_);

        filled_qty_ += fill_qty;

        avg_price_ =
            (avg_price_ * (filled_qty_ - fill_qty)
            + fill_price * fill_qty)
            / filled_qty_;

        State next =
            (filled_qty_ >= quantity_)
                ? State::FILLED
                : State::PARTIAL;

        state_.store(next, std::memory_order_release);
    }

    bool is_terminal() const {
        auto s = state();

        return s == State::FILLED ||
               s == State::CANCELLED ||
               s == State::REJECTED;
    }

private:
    std::atomic<State> state_{State::PENDING};

    mutable std::mutex mtx_;

    std::string exchange_order_id_;

    double avg_price_{0.0};
    int filled_qty_{0};
    int quantity_{0};

    uint64_t ack_ts_ns_{0};
};
```

## 2.8 SerializationFormate

### Protobuf Schemas

see[proto/] (../proto) directory for ful schemas

## Why Protobuf?

### Internal Transport Foramte

```
Hot path(C++ internal): Raw structs in shared memory (no serialization)
C++ -> kafka: Protobuf binary
kafka -> python: protobuf binary decoded via protobuf-python
python -> REST: json( upstox API formate)
Config files: Yaml/ Toml
```

---

## 2.9 CPU Pinning & NUMA

```cpp
// cpp/common/cpu_affinity.hpp
#include <pthread.h>
#include <sched.h>

void pin_thread_to_core(int core_id) {
    cpu_set_t cpuset;

    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_setaffinity_np(
        pthread_self(),
        sizeof(cpu_set_t),
        &cpuset);
}
void set_thread_priority_realtime() {
    struct sched_param param;

    param.sched_priority = 90;

    pthread_setschedparam(
        pthread_self(),
        SCHED_FIFO,
        &param);
}
//Core 0  : MarketData WebSocket I/O
//Core 1  : Tick Normalization
//Core 2  : Strategy A
//Core 3  : Strategy B
//Core 4  : Strategy C
//Core 5  : Reserved / Burst Strategy
//Core 6  : Risk Engine
//Core 7  : OMS Executor
//Core 8-11 : Fill Processing + Portfolio + Redis Sync
//Core 12+  : Kafka Publisher + Heartbeat + Background Tasks
//Remaining : OS Threads
```

### NUMA Awareness

```cpp
// on multi-scoke servers: bind strategy and data threads to same NUMA ode
// to avoid cross-socket access (~60-100ns penalty)

#include <numa.h>

void bind_to_numa_node(int node) {
    if (numa_available() < 0)
        return;

    numa_run_on_node(node);
    numa_set_preferred(node);
}

// strategy engine + market data -> NUMA node 0
// Analytics + kafka Publisher -> NUMA node 1 ( if dual socket)
```

---

## 2.10 Reconciliation & Fault Tolerance

### Order Reconciliation

```cpp
// cpp/oms/reconciler.hpp
// Runs every 30 seconds + on reconnect

class OrderReconciler {
public:
    void reconcile(const std::vector<ExchangeOrderStatus>& exchange_orders) {
        for (const auto& exch : exchange_orders) {
            auto* local_order = order_registry_.find(exch.order_id);

            if (!local_order) {
                // Unknown order on exchange - critical alert
                alert("Unknown exchange order: " + exch.order_id);
                continue;
            }

            // State mismatch detection
            if (exch.status == "COMPLETE" && !local_order->is_filled()) {
                log_warn("Missed fill detected for order {}", exch.order_id);
                process_missed_fill(local_order, exch);
            }

            if (exch.status == "REJECTED" &&
                local_order->state() != Order::State::REJECTED) {

                log_warn("Missed rejection for order {}", exch.order_id);
                local_order->force_state(Order::State::REJECTED);
            }
        }

        // Check for stale open orders (not acknowledged > 10s)
        for (auto& [id, order] : order_registry_) {
            if (order.state() == Order::State::SUBMITTED) {
                auto age_ms =
                    (get_monotonic_ns() - order.submit_ts()) / 1'000'000;

                if (age_ms > 10000) {
                    log_warn(
                        "Order {} stale for {}ms, querying exchange",
                        id,
                        age_ms
                    );

                    query_order_async(id);
                }
            }
        }
    }
};
```

### Heartbeat System

```cpp
// cpp/common/heartbeat.hpp

class HeartbeatMonitor {
    struct ServiceEntry {
        std::string name;
        std::atomic<uint64_t> last_heartbeat_ns{0};
        uint64_t max_gap_ns{5'000'000'000ULL}; // 5 seconds
    };

public:
    void beat(const std::string& service_name) {
        auto& entry = services_.at(service_name);

        entry.last_heartbeat_ns.store(
            get_monotonic_ns(),
            std::memory_order_release
        );
    }

    void check_all() {
        uint64_t now = get_monotonic_ns();

        for (auto& [name, entry] : services_) {
            uint64_t last =
                entry.last_heartbeat_ns.load(std::memory_order_acquire);

            if (now - last > entry.max_gap_ns) {
                handle_service_failure(
                    name,
                    (now - last) / 1'000'000
                );
            }
        }
    }

    void handle_service_failure(
        const std::string& name,
        uint64_t gap_ms
    ) {
        alert_manager_.fire(
            AlertLevel::CRITICAL,
            "Service {} missed heartbeat for {}ms",
            name,
            gap_ms
        );

        // Auto-halt trading if critical service is down
        if (name == "risk-engine" || name == "oms") {
            emergency_halt_.store(
                true,
                std::memory_order_release
            );
        }
    }

private:
    std::unordered_map<std::string, ServiceEntry> services_;
    AlertManager& alert_manager_;
    std::atomic<bool> emergency_halt_{false};
};
```

---

## 2.11 C++ Project Structure

```text
cpp/
├── CMakeLists.txt
├── conanfile.txt                     # Dependencies (Boost, Protobuf, spdlog, etc.)

├── common/
│   ├── types.hpp                     # NormalizedTick, Order, Fill, Position, etc.
│   ├── events.hpp                    # Event types, EventBus interface
│   ├── spsc_queue.hpp                # Lock-free SPSC queue
│   ├── disruptor.hpp                 # LMAX Disruptor ring buffer
│   ├── arena_allocator.hpp           # Arena memory allocator
│   ├── cpu_affinity.hpp              # Thread pinning, NUMA
│   ├── logger.hpp                    # spdlog wrapper
│   ├── metrics.hpp                   # Prometheus metrics
│   ├── config.hpp                    # YAML config loader
│   └── clock.hpp                     # Monotonic + wall clock utilities

├── market_data/
│   ├── upstox_ws_handler.hpp/cpp
│   ├── tick_normalizer.hpp/cpp
│   ├── candle_builder.hpp/cpp
│   ├── depth_handler.hpp/cpp
│   ├── options_chain.hpp/cpp
│   └── market_data_gateway.hpp/cpp   # Main entry point

├── strategy/
│   ├── strategy_interface.hpp        # Abstract base class
│   ├── strategy_manager.hpp/cpp      # Plugin loader, lifecycle
│   ├── strategy_context.hpp          # Per-strategy shared state
│   ├── event_dispatcher.hpp/cpp      # Tick fan-out
│   └── plugins/
│       ├── momentum_strategy.hpp/cpp
│       ├── mean_reversion_strategy.hpp/cpp
│       └── options_strategy.hpp/cpp

├── risk/
│   ├── risk_engine.hpp/cpp
│   ├── pre_trade_checks.hpp/cpp
│   ├── exposure_tracker.hpp/cpp
│   ├── greeks_monitor.hpp/cpp
│   ├── drawdown_limiter.hpp/cpp
│   └── kill_switch.hpp/cpp

├── oms/
│   ├── order.hpp/cpp
│   ├── order_pool.hpp
│   ├── order_registry.hpp/cpp
│   ├── order_state_machine.hpp/cpp
│   ├── reconciler.hpp/cpp
│   └── oms.hpp/cpp

├── ems/
│   ├── execution_engine.hpp/cpp
│   ├── upstox_rest_client.hpp/cpp    # Upstox REST adapter
│   ├── twap_executor.hpp/cpp
│   ├── vwap_executor.hpp/cpp
│   └── slippage_model.hpp/cpp

├── portfolio/
│   ├── portfolio_engine.hpp/cpp
│   ├── position_tracker.hpp/cpp
│   ├── pnl_engine.hpp/cpp
│   └── greeks_calculator.hpp/cpp

├── transport/
│   ├── kafka_producer.hpp/cpp
│   ├── kafka_consumer.hpp/cpp
│   ├── redis_client.hpp/cpp
│   └── shm_channel.hpp/cpp

└── main/
    ├── engine_main.cpp               # Main entry point
    └── paper_trader_main.cpp         # Paper trading mode
```

---

## 2.12 Python Package Structure

```text
python/
├── pyproject.toml
├── requirements.txt

├── quantforge/
│   ├── __init__.py
│   ├── config.py                     # Config loading, env management

│   ├── data/
│   │   ├── upstox_client.py          # Upstox REST API wrapper
│   │   ├── historical.py             # Historical data download
│   │   ├── tick_store.py             # ClickHouse tick reader/writer
│   │   ├── feature_store.py          # Feature engineering, caching
│   │   └── market_calendar.py        # NSE/BSE trading calendar

│   ├── research/
│   │   ├── factor_engine.py          # Factor computation
│   │   ├── alpha_models.py           # Alpha signal models
│   │   ├── correlation.py            # Cross-asset correlation
│   │   └── universe.py               # Universe selection

│   ├── backtesting/
│   │   ├── vectorized_bt.py          # Vectorized backtester (pandas/numpy)
│   │   ├── event_driven_bt.py        # Event-driven backtester
│   │   ├── execution_sim.py          # Realistic execution simulation
│   │   ├── slippage.py               # Slippage models
│   │   ├── transaction_costs.py
│   │   ├── options_bt.py            # Options-specific backtesting
│   │   ├── walk_forward.py          # Walk-forward optimization
│   │   ├── monte_carlo.py           # Monte Carlo analysis
│   │   └── distributed_bt.py        # Dask/Ray distributed backtest

│   ├── ml/
│   │   ├── feature_engineering.py
│   │   ├── model_trainer.py         # Train sklearn/torch/xgboost models
│   │   ├── onnx_exporter.py         # Export trained models to ONNX
│   │   ├── model_server.py          # FastAPI ONNX model server
│   │   └── feature_pipeline.py      # Real-time feature computation

│   ├── analytics/
│   │   ├── pnl_attribution.py
│   │   ├── risk_metrics.py          # Sharpe, Sortino, VaR, CVaR
│   │   ├── drawdown.py
│   │   ├── performance_report.py
│   │   └── strategy_dashboard.py

│   ├── signals/
│   │   ├── signal_interface.py      # Abstract signal producer
│   │   ├── technical.py             # TA-Lib based signals
│   │   ├── ml_signal.py             # ML model signals
│   │   └── composite.py             # Signal aggregation

│   ├── utils/
│   │   ├── kafka_utils.py
│   │   ├── redis_utils.py
│   │   ├── datetime_utils.py
│   │   ├── logging_utils.py
│   │   └── ipc_client.py

│   ├── notebooks/
│   │   ├── research/
│   │   │   ├── 01_data_exploration.ipynb
│   │   │   ├── 02_factor_research.ipynb
│   │   │   ├── 03_ml_training.ipynb
│   │   │   └── 04_strategy_prototyping.ipynb
│   │   └── analytics/
│   │       ├── daily_pnl_report.ipynb
│   │       └── strategy_review.ipynb

│   └── scripts/
│       ├── download_historical_data.py
│       ├── train_models.py
│       ├── run_backtest.py
│       ├── deploy_strategy.py
│       └── eod_reconciliation.py
```
