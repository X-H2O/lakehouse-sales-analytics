CREATE DATABASE IF NOT EXISTS sales_ads;

CREATE TABLE IF NOT EXISTS sales_ads.ads_daily_sales
(
    sale_date Date,
    paid_orders UInt64,
    paid_users UInt64,
    sales_amount Decimal(18, 2),
    avg_order_amount Decimal(18, 2),
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY sale_date;

CREATE TABLE IF NOT EXISTS sales_ads.ads_channel_conversion
(
    channel_id UInt32,
    channel_name String,
    channel_type String,
    visits UInt64,
    paid_orders UInt64,
    paid_users UInt64,
    conversion_rate Decimal(10, 4),
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY channel_id;

CREATE TABLE IF NOT EXISTS sales_ads.ads_user_ltv
(
    user_id UInt64,
    user_name String,
    province String,
    city String,
    paid_orders UInt64,
    total_amount Decimal(18, 2),
    first_order_date Date,
    last_order_date Date,
    ltv_rank UInt32,
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY (ltv_rank, user_id);

CREATE TABLE IF NOT EXISTS sales_ads.ads_repeat_purchase
(
    metric_date Date,
    paid_users UInt64,
    repeat_users UInt64,
    repeat_purchase_rate Decimal(10, 4),
    avg_paid_orders_per_user Decimal(10, 4),
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY metric_date;

CREATE TABLE IF NOT EXISTS sales_ads.ads_product_contribution
(
    product_id UInt64,
    product_name String,
    category String,
    brand String,
    sold_quantity UInt64,
    sales_amount Decimal(18, 2),
    contribution_rate Decimal(10, 4),
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY (category, contribution_rate, product_id);

CREATE TABLE IF NOT EXISTS sales_ads.ads_abnormal_refund
(
    category String,
    sold_items UInt64,
    refund_count UInt64,
    refund_amount Decimal(18, 2),
    refund_rate Decimal(10, 4),
    avg_refund_rate Decimal(10, 4),
    abnormal_flag UInt8,
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY (abnormal_flag, refund_rate, category);
