from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import clickhouse_connect
import psycopg2


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_HADOOP_HOME = PROJECT_ROOT / "tools" / "hadoop"

os.environ.setdefault("HADOOP_HOME", str(LOCAL_HADOOP_HOME))
os.environ.setdefault("hadoop.home.dir", str(LOCAL_HADOOP_HOME))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession


ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"

MINIO_ENDPOINT = "http://localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
WAREHOUSE_PATH = "s3a://warehouse/iceberg"

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sales_ods",
    "user": "sales_user",
    "password": "sales_password",
}

CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "port": 8123,
    "database": "sales_ads",
    "username": "sales_user",
    "password": "sales_password",
}

ODS_TABLES = {
    "channels": "channel_id",
    "users": "user_id",
    "products": "product_id",
    "visits": "visit_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "refunds": "refund_id",
}

DWD_COUNT_RULES = {
    "lake.sales_dwd.dwd_order_detail": "lake.sales_ods.ods_order_items",
    "lake.sales_dwd.dwd_visit_detail": "lake.sales_ods.ods_visits",
    "lake.sales_dwd.dwd_refund_detail": "lake.sales_ods.ods_refunds",
}

DWD_PRIMARY_KEYS = {
    "lake.sales_dwd.dwd_order_detail": "order_item_id",
    "lake.sales_dwd.dwd_visit_detail": "visit_id",
    "lake.sales_dwd.dwd_refund_detail": "refund_id",
}

DWS_TABLES = [
    "dws_daily_sales_summary",
    "dws_channel_summary",
    "dws_user_value_summary",
    "dws_product_sales_summary",
    "dws_category_refund_summary",
]

ADS_TABLES = [
    "ads_daily_sales",
    "ads_channel_conversion",
    "ads_user_ltv",
    "ads_repeat_purchase",
    "ads_product_contribution",
    "ads_abnormal_refund",
]


@dataclass
class CheckResult:
    layer: str
    name: str
    passed: bool
    detail: str


def create_spark() -> SparkSession:
    packages = ",".join([ICEBERG_PACKAGE, HADOOP_AWS_PACKAGE])
    return (
        SparkSession.builder.appName("data-quality-checks")
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", WAREHOUSE_PATH)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .getOrCreate()
    )


def postgres_scalar(sql: str):
    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()[0]


def spark_scalar(spark: SparkSession, sql: str):
    return spark.sql(sql).first()[0]


def clickhouse_scalar(client, sql: str):
    return client.query(sql).result_rows[0][0]


def as_decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def equal_money(left, right, tolerance: Decimal = Decimal("0.01")) -> bool:
    return abs(as_decimal(left) - as_decimal(right)) <= tolerance


def add_check(results: list[CheckResult], layer: str, name: str, passed: bool, detail: str) -> None:
    results.append(CheckResult(layer=layer, name=name, passed=passed, detail=detail))


def check_ods(spark: SparkSession, results: list[CheckResult]) -> None:
    for table_name, pk in ODS_TABLES.items():
        pg_count = postgres_scalar(f"SELECT COUNT(*) FROM {table_name}")
        iceberg_table = f"lake.sales_ods.ods_{table_name}"
        iceberg_count = spark_scalar(spark, f"SELECT COUNT(*) FROM {iceberg_table}")
        add_check(
            results,
            "ODS",
            f"{table_name} source count equals Iceberg count",
            pg_count == iceberg_count,
            f"postgres={pg_count}, iceberg={iceberg_count}",
        )

        duplicate_count = spark_scalar(
            spark,
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {pk}
                FROM {iceberg_table}
                GROUP BY {pk}
                HAVING COUNT(*) > 1
            )
            """,
        )
        add_check(results, "ODS", f"{iceberg_table} has no duplicate {pk}", duplicate_count == 0, f"duplicates={duplicate_count}")

        null_count = spark_scalar(spark, f"SELECT COUNT(*) FROM {iceberg_table} WHERE {pk} IS NULL")
        add_check(results, "ODS", f"{iceberg_table} primary key is not null", null_count == 0, f"nulls={null_count}")


def check_dwd(spark: SparkSession, results: list[CheckResult]) -> None:
    for dwd_table, ods_table in DWD_COUNT_RULES.items():
        dwd_count = spark_scalar(spark, f"SELECT COUNT(*) FROM {dwd_table}")
        ods_count = spark_scalar(spark, f"SELECT COUNT(*) FROM {ods_table}")
        add_check(results, "DWD", f"{dwd_table} row count matches {ods_table}", dwd_count == ods_count, f"dwd={dwd_count}, ods={ods_count}")

    for table_name, pk in DWD_PRIMARY_KEYS.items():
        duplicate_count = spark_scalar(
            spark,
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {pk}
                FROM {table_name}
                GROUP BY {pk}
                HAVING COUNT(*) > 1
            )
            """,
        )
        add_check(results, "DWD", f"{table_name} has no duplicate {pk}", duplicate_count == 0, f"duplicates={duplicate_count}")

    dwd_null_rules = {
        "lake.sales_dwd.dwd_order_detail": ["order_item_id", "order_id", "user_id", "product_id", "order_date", "order_status"],
        "lake.sales_dwd.dwd_visit_detail": ["visit_id", "session_id", "user_id", "product_id", "visit_date", "event_type"],
        "lake.sales_dwd.dwd_refund_detail": ["refund_id", "order_id", "order_item_id", "user_id", "product_id", "refund_date", "refund_status"],
    }
    for table_name, columns in dwd_null_rules.items():
        condition = " OR ".join(f"{col} IS NULL" for col in columns)
        null_count = spark_scalar(spark, f"SELECT COUNT(*) FROM {table_name} WHERE {condition}")
        add_check(results, "DWD", f"{table_name} critical columns are not null", null_count == 0, f"bad_rows={null_count}")

    negative_order_amounts = spark_scalar(
        spark,
        """
        SELECT COUNT(*)
        FROM lake.sales_dwd.dwd_order_detail
        WHERE quantity < 0 OR unit_price < 0 OR item_amount < 0 OR order_total_amount < 0
        """,
    )
    add_check(results, "DWD", "dwd_order_detail has no negative sales amounts", negative_order_amounts == 0, f"bad_rows={negative_order_amounts}")

    negative_refunds = spark_scalar(
        spark,
        "SELECT COUNT(*) FROM lake.sales_dwd.dwd_refund_detail WHERE refund_amount < 0",
    )
    add_check(results, "DWD", "dwd_refund_detail has no negative refund amounts", negative_refunds == 0, f"bad_rows={negative_refunds}")


def check_dws(spark: SparkSession, client, results: list[CheckResult]) -> None:
    for table_name in DWS_TABLES:
        row_count = clickhouse_scalar(client, f"SELECT count() FROM {table_name}")
        add_check(results, "DWS", f"{table_name} is not empty", row_count > 0, f"rows={row_count}")

    dwd_distinct_paid_order_sales = spark_scalar(
        spark,
        """
        SELECT ROUND(SUM(order_total_amount), 2)
        FROM (
            SELECT DISTINCT order_id, order_total_amount
            FROM lake.sales_dwd.dwd_order_detail
            WHERE is_paid = 1
        )
        """,
    )
    dws_daily_sales = clickhouse_scalar(client, "SELECT round(sum(sales_amount), 2) FROM dws_daily_sales_summary")
    add_check(
        results,
        "DWS",
        "daily sales amount matches DWD distinct paid orders",
        equal_money(dwd_distinct_paid_order_sales, dws_daily_sales),
        f"dwd={dwd_distinct_paid_order_sales}, dws={dws_daily_sales}",
    )

    dwd_paid_item_sales = spark_scalar(
        spark,
        "SELECT ROUND(SUM(item_amount), 2) FROM lake.sales_dwd.dwd_order_detail WHERE is_paid = 1",
    )
    dws_product_sales = clickhouse_scalar(client, "SELECT round(sum(sales_amount), 2) FROM dws_product_sales_summary")
    add_check(
        results,
        "DWS",
        "product sales amount matches DWD paid item amount",
        equal_money(dwd_paid_item_sales, dws_product_sales),
        f"dwd={dwd_paid_item_sales}, dws={dws_product_sales}",
    )

    dwd_approved_refunds = spark_scalar(
        spark,
        "SELECT COUNT(*) FROM lake.sales_dwd.dwd_refund_detail WHERE is_approved = 1",
    )
    dws_refunds = clickhouse_scalar(client, "SELECT sum(refund_count) FROM dws_category_refund_summary")
    add_check(results, "DWS", "refund count matches DWD approved refunds", int(dwd_approved_refunds) == int(dws_refunds), f"dwd={dwd_approved_refunds}, dws={dws_refunds}")

    invalid_rates = clickhouse_scalar(
        client,
        """
        SELECT
            (SELECT count() FROM dws_channel_summary WHERE conversion_rate < 0 OR conversion_rate > 1)
            +
            (SELECT count() FROM dws_category_refund_summary WHERE refund_rate < 0 OR refund_rate > 1)
        """,
    )
    add_check(results, "DWS", "DWS rate fields are between 0 and 1", invalid_rates == 0, f"bad_rows={invalid_rates}")


def check_ads(client, results: list[CheckResult]) -> None:
    row_pairs = [
        ("ads_daily_sales", "dws_daily_sales_summary"),
        ("ads_channel_conversion", "dws_channel_summary"),
        ("ads_user_ltv", "dws_user_value_summary"),
        ("ads_product_contribution", "dws_product_sales_summary"),
        ("ads_abnormal_refund", "dws_category_refund_summary"),
    ]
    for ads_table, dws_table in row_pairs:
        ads_count = clickhouse_scalar(client, f"SELECT count() FROM {ads_table}")
        dws_count = clickhouse_scalar(client, f"SELECT count() FROM {dws_table}")
        add_check(results, "ADS", f"{ads_table} row count matches {dws_table}", ads_count == dws_count, f"ads={ads_count}, dws={dws_count}")

    repeat_count = clickhouse_scalar(client, "SELECT count() FROM ads_repeat_purchase")
    add_check(results, "ADS", "ads_repeat_purchase has exactly one row", repeat_count == 1, f"rows={repeat_count}")

    ads_daily_sales = clickhouse_scalar(client, "SELECT round(sum(sales_amount), 2) FROM ads_daily_sales")
    dws_daily_sales = clickhouse_scalar(client, "SELECT round(sum(sales_amount), 2) FROM dws_daily_sales_summary")
    add_check(results, "ADS", "ADS daily sales amount matches DWS", equal_money(ads_daily_sales, dws_daily_sales), f"ads={ads_daily_sales}, dws={dws_daily_sales}")

    contribution_sum = clickhouse_scalar(client, "SELECT round(sum(contribution_rate), 4) FROM ads_product_contribution")
    add_check(
        results,
        "ADS",
        "product contribution rates sum close to 1",
        abs(as_decimal(contribution_sum) - Decimal("1")) <= Decimal("0.01"),
        f"sum={contribution_sum}",
    )

    invalid_rates = clickhouse_scalar(
        client,
        """
        SELECT
            (SELECT count() FROM ads_channel_conversion WHERE conversion_rate < 0 OR conversion_rate > 1)
            +
            (SELECT count() FROM ads_repeat_purchase WHERE repeat_purchase_rate < 0 OR repeat_purchase_rate > 1)
            +
            (SELECT count() FROM ads_product_contribution WHERE contribution_rate < 0 OR contribution_rate > 1)
            +
            (SELECT count() FROM ads_abnormal_refund WHERE refund_rate < 0 OR refund_rate > 1 OR avg_refund_rate < 0 OR avg_refund_rate > 1)
        """,
    )
    add_check(results, "ADS", "ADS rate fields are between 0 and 1", invalid_rates == 0, f"bad_rows={invalid_rates}")


def print_results(results: list[CheckResult]) -> None:
    print("\nData quality check results")
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.layer:<3} {result.name} ({result.detail})")

    passed = sum(1 for result in results if result.passed)
    failed = len(results) - passed
    print(f"\nSummary: passed={passed}, failed={failed}, total={len(results)}")


def main() -> None:
    results: list[CheckResult] = []
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

    try:
        check_ods(spark, results)
        check_dwd(spark, results)
        check_dws(spark, client, results)
        check_ads(client, results)
    finally:
        spark.stop()

    print_results(results)
    if any(not result.passed for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
