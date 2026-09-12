CREATE DATABASE IF NOT EXISTS sales_ads;

CREATE TABLE IF NOT EXISTS sales_ads.rt_order_events
(
    event_id String,
    run_id String,
    event_type String,
    order_id UInt64,
    user_id UInt64,
    channel_id UInt32,
    total_amount Decimal(18, 2),
    event_time DateTime,
    kafka_timestamp DateTime,
    topic String,
    partition Int32,
    offset UInt64,
    raw_event String,
    ingested_at DateTime
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (event_id, order_id);

CREATE TABLE IF NOT EXISTS sales_ads.rt_order_minute_summary
(
    event_minute DateTime,
    event_count UInt64,
    paid_order_count UInt64,
    sales_amount Decimal(18, 2),
    refund_count UInt64,
    cancel_count UInt64,
    first_ingested_at DateTime,
    last_ingested_at DateTime,
    refreshed_at DateTime
)
ENGINE = ReplacingMergeTree(refreshed_at)
ORDER BY event_minute;
