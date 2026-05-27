# 01-High-Level Systen Architecture

## 1.1 System Overview

QuantForge is structured as a **polyglot microservice platform** where:

- **C++ services** own the hot path: market data ingestion, strategy execution, OMS, EMS, and risk
- **Python services** own the cold/warm path: research, backtesting, analytics, ML inference, and scheduling
- **Kafka** is the backbone for all inter-service async communication
- **Redis** serves as the shared in-memory state store (positions, risk counters, tick cache)
- **ClickHouse** stores all historical tick, order, fill, and analytics data
- **Upstox API 2.0** is the sole exchange connectivity layer (market data execution)

---

## 1.2 Top-Level Service Map

```
+---------------------------------------------------------+
|                  EXTERNAL BOUNDARY                      |
|                                                         |
|                    Upstox API 2.0                       |
| WebSocket Feed | REST Orders | REST Positions | Account |
+---------------------------------------------------------+

        |
        v

+---------------------+
| Market Data Gateway |
|        C++          |
+---------------------+
        |
        v

+---------------------+
| Tick Bus (Kafka)    |
| Topics: ticks,      |
| depth, ohlc         |
+---------------------+
        |
        v

+---------------------+
| Strategy Engine C++ |
| Strategy Manager    |
| Strategy A          |
| Strategy B          |
| Strategy N          |
+---------------------+
        |
        v

+---------------------+      +---------------------+
| Risk Engine C++     | ---> | OMS C++             |
| Pre-trade checks    |      | Order state machine |
| Exposure limits     |      | Fill handling       |
| Greeks monitoring   |      | Reconciliation      |
+---------------------+      +---------------------+

               |
               v

+------------------------------------------------+
| Portfolio Engine C++                           |
| Positions | PnL | Greeks | Exposure            |
+------------------------------------------------+

        |                             |
        v                             v

+----------------------+   +----------------------+
| Redis State Store    |   | ClickHouse DB        |
| Live positions       |   | Ticks, fills, orders |
| Risk counters        |   | Analytics, factors   |
| Tick snapshot cache  |   | Strategy metrics     |
+----------------------+   +----------------------+

                |
                v

+----------------------------------------------------------+
| Python Services Layer                                    |
+----------------------------------------------------------+
| Research      | Backtesting      | Analytics & Reporting |
| Jupyter       | Engine           | PnL attribution       |
| Factor Lib    | VBT/Zipline      | Strategy performance  |
+----------------------------------------------------------+
| ML Models     | Scheduler        | Data Pipeline         |
| ONNX Svc      | Airflow          | Upstox historical API |
| Feature Eng   | Cron Jobs        | Data cleaning/storage |
+----------------------------------------------------------+

+----------------------------------------------------------+
| Observability Stack                                      |
| Prometheus | Grafana | OpenTelemetry | Loki | AlertMgr  |
+----------------------------------------------------------+
```

---

## 1.3 Service Boundaries

### C++ Core Services (Hot Path)

| Service               | Responsibility                                      | Latency Target     |
| --------------------- | --------------------------------------------------- | ------------------ |
| `market-data-gateway` | Upstox WS connection, tick parsing, normalization   | < 50µs per tick    |
| `strategy-engine `    | Hosts all strategy plugins, dispatches tick events  | < 100µs signal     |
| `risk-engine `        | Pre-trade checks, real-time Greeks, exposure limits | < 20µs gate        |
| `oms`                 | Order lifecycle, state machine, fill processing     | < 200µs round trip |
| `ems `                | Smart order routing, slippage, TWAP/VWAP algos      | < 500µs            |
| `portfolio-engine `   | Real-time positions, PnL, net exposure              | < 10µs update      |

### Python Services (Warm/Cold Path)

| Service              | Responsibility                                     | Latency Target |
| -------------------- | -------------------------------------------------- | -------------- |
| `research-lab `      | Jupyter notebooks, factor research, model training | Offline        |
| `backtesting-engine` | Vectorized + event-driven simulation               | Minutes-hours  |
| `data-pipeline`      | Historical data fetch, clean, store                | Batch          |
| `analytics-service`  | Daily PnL reports, attribution, drawdown           | Minutes        |
| `ml-model-server`    | ONNX model serving, feature inference              | 1-5ms          |
| `scheduler `         | Market open/close jobs, EOD reconciliation         | Scheduled      |
| `paper-trader `      | Paper trading simulation against live feed         | ~10ms          |

## 1.4 Event Flow Architecture

### Hot Path (Latency-Sensitive)

```
Upstox WS
    -> [C++] MarketDataGateway (parse + normalize)
        -> [Shared Memory Ring Buffer] Ticks
            -> [C++] StrategyEngine (per-strategy tick callbacks)
                -> [C++] RiskEngine (pre-trade gate)
                    -> [C++] OMS (submit order -> Upstox REST)
                        -> [C++] PortfolioEngine (fill update)
                            -> [Redis] Position update
                            -> [Kafka] fill.events (async, non-blocking)
```

### Warm Path (Analytics, ML Signals)

```
[Kafka] tick.raw
    -> [Python] FeatureEngine
        -> rolling features
        -> technical indicators
            -> [Python] MLModelServer (ONNX inference)
                -> [Kafka] signals.ml
                    -> [C++] StrategyEngine
```

### Cold Path (Research, Data)

```
[ClickHouse] historical_ticks
    -> [Python] Research Notebooks
        -> Factor models
        -> ML training
        -> Backtest runs
            -> [ClickHouse] backtest_results
                -> [Grafana] Strategy performance dashboards
```

## 1.5 Async Pipelines

### Kafka Topic Architecture

```

Topics (Partitioned by instrument token):
market.ticks → raw tick from Upstox WS
market.depth → order book L2 data
market.ohlc → candle data (1m, 5m, 15m, day)
market.options_chain → processed options chain snapshots

    orders.new → new order request from strategy
    orders.cancel → cancellation requests
    orders.status → order status updates (ack, reject, fill)
    orders.fills → confirmed fill events

    risk.alerts → risk threshold breaches
    risk.kill → emergency halt signals

    portfolio.updates → position changes, Pnl snapshots

    signals.alpha → alpha signals from strategies
    signals.ml → ML model signals

    system.heartbeat → per-service health pings
    system.logs → structured log events

```

---

## 1.5 Async Pipelines

### Kafka Topic Architecture

Topics (Partitioned by instrument token):

market.ticks -> raw tick from Upstox WS
market.depth -> order book L2 data
market.ohlc -> candle data (1m, 5m, 15m, day)
market.options_chain -> processed options chain snapshots

orders.new -> new order request from strategy
orders.cancel -> cancellation requests
orders.status -> order status updates (ack, reject, fill)
orders.fills -> confirmed fill events

risk.alerts -> risk threshold breaches
risk.kill -> emergency halt signals

portfolio.updates -> position changes, PnL snapshots

signals.alpha -> alpha signals from strategies
signals.ml -> ML model signals

system.heartbeat -> per-service health pings
system.logs -> structured log events

## 1.6 Hot vs Cold Paths

| Path      | Services                              | Data               | Latency         | Technology                      |
| --------- | ------------------------------------- | ------------------ | --------------- | ------------------------------- |
| Ultra-Hot | MarketData -> Strategy -> Risk -> OMS | Live ticks, orders | < 1ms           | C++, shared memory, SPSC queues |
| Hot       | Portfolio updates, Redis sync         | Fills, positions   | 1-10ms          | C++, Redis                      |
| Warm      | ML signal generation, paper trading   | Feature vectors    | 10ms-1s         | Python, Kafka                   |
| Cool      | EOD analytics, reporting              | Aggregated data    | Seconds-minutes | Python, ClickHouse              |
| Cold      | Research, backtesting, training       | Historical data    | Minutes-hours   | Python, Parquet, ClickHouse     |

## 1.7 Synchronization Strategy

### Cross-Language State Sync (C++ ↔ Python)

```
C++ writes positions -> Redis Hash (atomic HSET)
Python reads from Redis -> near-real-time state visibility

C++ publishes fills -> Kafka topic fills.events
Python consumes fills -> analytics, reporting, ML

Python writes ML signals -> Kafka topic signals.ml
C++ consumes ML signals -> alpha factors in strategy

Config changes -> YAML file / Redis pub-sub
C++ hot-reloads strategy params -> config watcher thread
```

### Clock Synchronization

```
All services synchronized to:

- System NTP (chrony/NTP server) -> ±1ms accuracy
- Exchange timestamps embedded in tick data
- Monotonic clocks (CLOCK_MONOTONIC) for internal timing
- UTC timestamps in all stored records + latency metadata
```

## 1.8 Security Boundaries

```
+-----------------------------------+
|DMZ / API Gateway                  |
|Kong / Nginx -> Auth -> Rate Limit |
+-----------------------------------+
                |
                | mTLS
+-------------------------------------+
|Internal Service Mesh                |
|(Consul / Istio)                     |
|All inter-service calls mTLS         |
+-------------------------------------+
                |
                |
+--------------------------------------+
| Secret Management                    |
| HashiCorp Vault / AWS Secrets Manager|
| API keys, DB credentials             |
+---------------------------------------
```

### Authentication Flow

- Upstox API key stored in Vault, injected as env var at startup
- All internal APIs secured with JWT + service-level API keys
- Grafana/monitoring dashboards behind SSO
- No secrets in code or version control (enforced via pre-commit hooks)
