from __future__ import annotations

from pathlib import Path

import clickhouse_connect


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ADS_SCHEMA_FILE = PROJECT_ROOT / "sql" / "02_create_ads_tables.sql"

CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "port": 8123,
    "database": "sales_ads",
    "username": "sales_user",
    "password": "sales_password",
}

ADS_TABLES = [
    "ads_daily_sales",
    "ads_channel_conversion",
    "ads_user_ltv",
    "ads_repeat_purchase",
    "ads_product_contribution",
    "ads_abnormal_refund",
]


def clickhouse_client():
    return clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)


def execute_clickhouse_sql_file(client) -> None:
    statements = [stmt.strip() for stmt in ADS_SCHEMA_FILE.read_text(encoding="utf-8").split(";")]
    for statement in statements:
        if statement:
            client.command(statement)


def reset_ads_tables(client) -> None:
    for table_name in ADS_TABLES:
        client.command(f"TRUNCATE TABLE IF EXISTS sales_ads.{table_name}")


def insert_ads_daily_sales(client) -> None:
    client.command(
        """
        INSERT INTO sales_ads.ads_daily_sales
        SELECT
            sale_date,
            paid_orders,
            paid_users,
            sales_amount,
            avg_order_amount,
            now() AS computed_at
        FROM sales_ads.dws_daily_sales_summary
        ORDER BY sale_date
        """
    )


def insert_ads_channel_conversion(client) -> None:
    client.command(
        """
        INSERT INTO sales_ads.ads_channel_conversion
        SELECT
            channel_id,
            channel_name,
            channel_type,
            visits,
            paid_orders,
            paid_users,
            conversion_rate,
            now() AS computed_at
        FROM sales_ads.dws_channel_summary
        ORDER BY conversion_rate DESC
        """
    )


def insert_ads_user_ltv(client) -> None:
    client.command(
        """
        INSERT INTO sales_ads.ads_user_ltv
        SELECT
            user_id,
            user_name,
            province,
            city,
            paid_orders,
            total_amount,
            first_order_date,
            last_order_date,
            toUInt32(row_number() OVER (ORDER BY total_amount DESC, user_id ASC)) AS ltv_rank,
            now() AS computed_at
        FROM sales_ads.dws_user_value_summary
        ORDER BY ltv_rank
        """
    )


def insert_ads_repeat_purchase(client) -> None:
    client.command(
        """
        INSERT INTO sales_ads.ads_repeat_purchase
        SELECT
            (SELECT max(sale_date) FROM sales_ads.dws_daily_sales_summary) AS metric_date,
            count() AS paid_users,
            countIf(paid_orders >= 2) AS repeat_users,
            round(repeat_users / paid_users, 4) AS repeat_purchase_rate,
            round(avg(paid_orders), 4) AS avg_paid_orders_per_user,
            now() AS computed_at
        FROM sales_ads.dws_user_value_summary
        """
    )


def insert_ads_product_contribution(client) -> None:
    client.command(
        """
        INSERT INTO sales_ads.ads_product_contribution
        SELECT
            product_id,
            product_name,
            category,
            brand,
            sold_quantity,
            sales_amount,
            round(toFloat64(sales_amount) / sum(toFloat64(sales_amount)) OVER (), 4) AS contribution_rate,
            now() AS computed_at
        FROM sales_ads.dws_product_sales_summary
        ORDER BY contribution_rate DESC
        """
    )


def insert_ads_abnormal_refund(client) -> None:
    client.command(
        """
        INSERT INTO sales_ads.ads_abnormal_refund
        WITH avg_refund AS (
            SELECT round(avg(refund_rate), 4) AS avg_refund_rate
            FROM sales_ads.dws_category_refund_summary
        )
        SELECT
            category,
            sold_items,
            refund_count,
            refund_amount,
            refund_rate,
            avg_refund.avg_refund_rate,
            toUInt8(refund_rate > avg_refund.avg_refund_rate * 1.8 AND refund_count >= 50) AS abnormal_flag,
            now() AS computed_at
        FROM sales_ads.dws_category_refund_summary
        CROSS JOIN avg_refund
        ORDER BY abnormal_flag DESC, refund_rate DESC
        """
    )


def print_ads_counts(client) -> None:
    print("\nADS row counts")
    for table_name in ADS_TABLES:
        count = client.query(f"SELECT count() FROM sales_ads.{table_name}").result_rows[0][0]
        print(f"  {table_name:<28} {count:>10}")


def main() -> None:
    client = clickhouse_client()
    execute_clickhouse_sql_file(client)
    reset_ads_tables(client)

    insert_ads_daily_sales(client)
    insert_ads_channel_conversion(client)
    insert_ads_user_ltv(client)
    insert_ads_repeat_purchase(client)
    insert_ads_product_contribution(client)
    insert_ads_abnormal_refund(client)

    print_ads_counts(client)
    print("\nADS computation from DWS completed.")


if __name__ == "__main__":
    main()
