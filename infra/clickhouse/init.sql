-- infra/clickhouse/init.sql
-- QuantForge ClickHouse schema
-- Runs automatically when the clickhouse container first starts.
-- All tables use MergeTree family for time-series performance.

CREATE DATABASE IF NOT EXISTS quantforge;

-- Tick data (main time-series fact table)

CREATE TABLE IF NOT EXISTS quantforge.ticks
(
    instrument_token UInt64,
    exchange_id UInt8,       -- 1=NSE_EQ 2=NSE_FO 3=BSE_EQ 4=BSE_FO
    symbol LowCardinality(String),
    ltp Float64,
    prev_close Float64,
    open Float64,
    high Float64,
    low Float64,
    volume UInt64,
    oi UInt64,

    bid_price Float64,
    bid_qty UInt32,

    ask_price Float64,
    ask_qty UInt32,

    exchange_ts DateTime64(9, 'Asia/Kolkata'),
    recv_ts DateTime64(9, 'Asia/Kolkata'),

    network_latency_ns Int64,

    date Date MATERIALIZED toDate(exchange_ts)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(exchange_ts)
ORDER BY (instrument_token, exchange_ts)
TTL exchange_ts + INTERVAL 5 YEAR DELETE
SETTINGS index_granularity = 8192;

-- 1-minute OHLCV candles

CREATE TABLE IF NOT EXISTS quantforge.candles_1m
(
    instrument_token UInt64,
    symbol LowCardinality(String),
    ts DateTime('Asia/Kolkata'),

    open Float64,
    high Float64,
    low Float64,
    close Float64,

    volume UInt64,
    oi UInt64,
    tick_count UInt32,

    date Date MATERIALIZED toDate(ts)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (instrument_token, ts)
TTL ts + INTERVAL 10 YEAR DELETE;

-----materialized view auto aggregate ticks ->  1-minute candles-----
CREATE MATERIALIZED VIEW IF NOT EXISTS quantforge.mv_candles_1m
TO quantforge.candles_1m AS
SELECT
    instrument_token,
    symbol,
    toStartOfMinute(exchange_ts) AS ts,

    argMin(ltp, exchange_ts) AS open,
    max(ltp) AS high,
    min(ltp) AS low,
    argMax(ltp, exchange_ts) AS close,

    max(volume) AS volume,
    argMax(oi, exchange_ts) AS oi,
    count() AS tick_count

FROM quantforge.ticks
GROUP BY instrument_token, symbol, ts;

-- 5-minute candles

CREATE TABLE IF NOT EXISTS quantforge.candles_5m
AS quantforge.candles_1m
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (instrument_token, ts)
TTL ts + INTERVAL 10 YEAR DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS quantforge.mv_candles_5m
TO quantforge.candles_5m AS
SELECT
    instrument_token,
    symbol,
    toStartOfFiveMinutes(exchange_ts) AS ts,

    argMin(ltp, exchange_ts) AS open,
    max(ltp) AS high,
    min(ltp) AS low,
    argMax(ltp, exchange_ts) AS close,

    max(volume) AS volume,
    argMax(oi, exchange_ts) AS oi,
    count() AS tick_count

FROM quantforge.ticks
GROUP BY instrument_token, symbol, ts;

-- Orders (regulatory record - 7 year TTL)

CREATE TABLE IF NOT EXISTS quantforge.orders
(
    order_id String,
    internal_order_id String,
    strategy_id LowCardinality(String),
    instrument_token UInt64,
    symbol LowCardinality(String),

    side Enum8('BUY' = 1, 'SELL' = 2),

    order_type Enum8(
        'MARKET' = 1,
        'LIMIT' = 2,
        'SL' = 3,
        'SL_M' = 4
    ),

    product Enum8(
        'INTRADAY' = 1,
        'DELIVERY' = 2
    ),

    quantity Int32,
    limit_price Nullable(Float64),
    trigger_price Nullable(Float64),
    status LowCardinality(String),

    fill_qty Int32,
    fill_price Nullable(Float64),
    commission Float64,
    reject_reason Nullable(String),
    tag String,

    submit_ts DateTime64(9, 'Asia/Kolkata'),
    ack_ts Nullable(DateTime64(9, 'Asia/Kolkata')),
    fill_ts Nullable(DateTime64(9, 'Asia/Kolkata')),

    date Date MATERIALIZED toDate(submit_ts)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(submit_ts)
ORDER BY (strategy_id, submit_ts, order_id)
TTL submit_ts + INTERVAL 7 YEAR DELETE;


----- fills (trade records - 7 year TTL) -----
CREATE TABLE IF NOT EXISTS quantforge.fills
(
    trade_id String,
    order_id String,
    strategy_id LowCardinality(String),
    instrument_token UInt64,
    symbol LowCardinality(String),

    side Enum8('BUY' = 1, 'SELL' = 2),

    quantity Int32,
    price Float64,

    fill_ts DateTime64(9, 'Asia/Kolkata'),

    date Date MATERIALIZED toDate(fill_ts)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(fill_ts)
ORDER BY (strategy_id, fill_ts, trade_id)
TTL fill_ts + INTERVAL 7 YEAR DELETE;

---- Positions (daily snapshots, EOD)-----------------
CREATE TABLE IF NOT EXISTS quantforge.position_snapshots
(
    snapshot_date Date,
    strategy_id LowCardinality(String),
    instrument_token UInt64,
    symbol LowCardinality(String),

    quantity Int32,
    avg_price Float64,
    closing_price Float64,

    realized_pnl Float64,
    unrealized_pnl Float64,
    total_pnl Float64,

    net_delta Float64,
    net_vega Float64,

    recorded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (strategy_id, snapshot_date, instrument_token)
TTL snapshot_date + INTERVAL 3 YEAR DELETE;

----- Strategy performance metrics (daily) -----------------
CREATE TABLE IF NOT EXISTS quantforge.strategy_metrics
(
    date Date,
    strategy_id LowCardinality(String),

    realized_pnl Float64,
    unrealized_pnl Float64,

    total_trades UInt32,
    win_trades UInt32,
    loss_trades UInt32,

    gross_pnl Float64,
    commission_paid Float64,
    net_pnl Float64,

    max_drawdown Float64,
    sharpe_daily Float64,
    avg_hold_minutes Float64,

    recorded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (strategy_id, date);


--- Factor data (cross-sectional alpha factors)--------------
CREATE TABLE IF NOT EXISTS quantforge.factors
(
    date Date,
    instrument_token UInt64,
    symbol LowCardinality(String),

    factor_name LowCardinality(String),
    value Float64,
    universe LowCardinality(String),

    date_created DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (factor_name, date, instrument_token)
TTL date + INTERVAL 5 YEAR DELETE;




--- Backtesting results (for storing historical strategy performance) ----
CREATE TABLE IF NOT EXISTS quantforge.backtest_results
(
    run_id String,
    strategy_id LowCardinality(String),
    strategy_version String,

    start_date Date,
    end_date Date,

    initial_capital Float64,
    final_equity Float64,

    total_return Float64,
    cagr Float64,
    sharpe Float64,
    sortino Float64,
    max_drawdown Float64,
    calmar Float64,

    win_rate Float64,
    profit_factor Float64,

    total_trades UInt32,

    params String, -- JSON of strategy parameters

    run_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(run_at)
ORDER BY (strategy_id, run_at, run_id);


--- Engines heartbeat /health log---------
CREATE TABLE IF NOT EXISTS quantforge.engine_heartbeats
(
    ts DateTime64(3, 'Asia/Kolkata'),

    ticks_per_sec Float64,
    open_orders UInt32,
    available_capital Float64,

    realized_pnl Float64,
    unrealized_pnl Float64,

    ws_connected UInt8,
    kafka_healthy UInt8,
    redis_connected UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts
TTL ts + INTERVAL 30 DAY DELETE;


-- Usseful views-------------


---todays P&L summary-
CREATE VIEW IF NOT EXISTS quantforge.v_today_pnl AS
SELECT
    strategy_id,
    sum(quantity * price * if(side = 'SELL', 1, -1)) AS gross_pnl,
    count() AS trade_count,
    sum(price * quantity * 0.0003) AS commission_est
FROM quantforge.fills
WHERE date = today()
GROUP BY strategy_id;


--Current open postions (from fills =FIFO)
CREATE VIEW IF NOT EXISTS quantforge.v_open_positions AS
SELECT
    strategy_id,
    instrument_token,
    symbol,
    sum(if(side = 'BUY', quantity, -quantity)) AS net_qty,
    count() AS fill_count
FROM quantforge.fills
WHERE date >= today() - INTERVAL 1 DAY
GROUP BY strategy_id, instrument_token, symbol
HAVING abs(net_qty) > 0;
