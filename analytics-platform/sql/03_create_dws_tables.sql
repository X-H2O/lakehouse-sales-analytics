CREATE DATABASE IF NOT EXISTS sales_ads;

CREATE TABLE IF NOT EXISTS sales_ads.dws_daily_sales_summary
(
    sale_date Date,
    paid_orders UInt64,
    paid_users UInt64,
    paid_order_items UInt64,
    sales_amount Decimal(18, 2),
    avg_order_amount Decimal(18, 2),
    gross_profit Decimal(18, 2),
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY sale_date;

CREATE TABLE IF NOT EXISTS sales_ads.dws_channel_summary
(
    channel_id UInt32,
    channel_name String,
    channel_type String,
    visits UInt64,
    visit_users UInt64,
    paid_orders UInt64,
    paid_users UInt64,
    sales_amount Decimal(18, 2),
    conversion_rate Decimal(10, 4),
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY channel_id;

CREATE TABLE IF NOT EXISTS sales_ads.dws_user_value_summary
(
    user_id UInt64,
    user_name String,
    province String,
    city String,
    paid_orders UInt64,
    total_amount Decimal(18, 2),
    first_order_date Date,
    last_order_date Date,
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY (user_id);

CREATE TABLE IF NOT EXISTS sales_ads.dws_product_sales_summary
(
    product_id UInt64,
    product_name String,
    category String,
    brand String,
    sold_quantity UInt64,
    sales_amount Decimal(18, 2),
    gross_profit Decimal(18, 2),
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY (category, product_id);

CREATE TABLE IF NOT EXISTS sales_ads.dws_category_refund_summary
(
    category String,
    sold_items UInt64,
    refund_count UInt64,
    refund_amount Decimal(18, 2),
    refund_rate Decimal(10, 4),
    computed_at DateTime
)
ENGINE = MergeTree
ORDER BY category;
