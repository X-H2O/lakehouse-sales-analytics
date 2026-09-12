from __future__ import annotations

import os
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import clickhouse_connect


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DWS_SCHEMA_FILE = PROJECT_ROOT / "sql" / "03_create_dws_tables.sql"
LOCAL_HADOOP_HOME = PROJECT_ROOT / "tools" / "hadoop"

os.environ.setdefault("HADOOP_HOME", str(LOCAL_HADOOP_HOME))
os.environ.setdefault("hadoop.home.dir", str(LOCAL_HADOOP_HOME))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"

MINIO_ENDPOINT = "http://localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
WAREHOUSE_PATH = "s3a://warehouse/iceberg"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "sales_user"
CLICKHOUSE_PASSWORD = "sales_password"
CLICKHOUSE_DATABASE = "sales_ads"


def create_spark() -> SparkSession:
    packages = ",".join([ICEBERG_PACKAGE, HADOOP_AWS_PACKAGE])
    return (
        SparkSession.builder.appName("sales-dws-tables")
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


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def execute_clickhouse_sql_file(client) -> None:
    statements = [stmt.strip() for stmt in DWS_SCHEMA_FILE.read_text(encoding="utf-8").split(";")]
    for statement in statements:
        if statement:
            client.command(statement)


def reset_dws_tables(client) -> None:
    for table_name in [
        "dws_daily_sales_summary",
        "dws_channel_summary",
        "dws_user_value_summary",
        "dws_product_sales_summary",
        "dws_category_refund_summary",
    ]:
        client.command(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{table_name}")


def to_python_value(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


def insert_dataframe(client, table_name: str, df: DataFrame, columns: list[str]) -> int:
    rows = [tuple(to_python_value(row[col]) for col in columns) for row in df.collect()]
    if rows:
        client.insert(f"{CLICKHOUSE_DATABASE}.{table_name}", rows, column_names=columns)
    print(f"Inserted {len(rows):>5} rows into {table_name}")
    return len(rows)


def compute_daily_sales(order_detail: DataFrame, computed_at: datetime) -> DataFrame:
    paid_items = order_detail.filter(F.col("is_paid") == 1)
    paid_orders = paid_items.select(
        "order_id",
        "user_id",
        "order_date",
        "order_total_amount",
    ).dropDuplicates(["order_id"])

    order_stats = paid_orders.groupBy("order_date").agg(
        F.count("*").cast("long").alias("paid_orders"),
        F.countDistinct("user_id").cast("long").alias("paid_users"),
        F.round(F.sum("order_total_amount"), 2).alias("sales_amount"),
        F.round(F.avg("order_total_amount"), 2).alias("avg_order_amount"),
    )
    item_stats = paid_items.groupBy("order_date").agg(
        F.count("*").cast("long").alias("paid_order_items"),
        F.round(F.sum("gross_profit"), 2).alias("gross_profit"),
    )

    return (
        order_stats.join(item_stats, "order_date", "inner")
        .withColumnRenamed("order_date", "sale_date")
        .withColumn("computed_at", F.lit(computed_at))
        .select(
            "sale_date",
            "paid_orders",
            "paid_users",
            "paid_order_items",
            "sales_amount",
            "avg_order_amount",
            "gross_profit",
            "computed_at",
        )
        .orderBy("sale_date")
    )


def compute_channel_summary(
    order_detail: DataFrame,
    visit_detail: DataFrame,
    computed_at: datetime,
) -> DataFrame:
    visit_stats = visit_detail.groupBy("channel_id", "channel_name", "channel_type").agg(
        F.count("*").cast("long").alias("visits"),
        F.countDistinct("user_id").cast("long").alias("visit_users"),
    )

    paid_orders = (
        order_detail.filter(F.col("is_paid") == 1)
        .select("order_id", "user_id", "channel_id", "order_total_amount")
        .dropDuplicates(["order_id"])
    )
    order_stats = paid_orders.groupBy("channel_id").agg(
        F.count("*").cast("long").alias("paid_orders"),
        F.countDistinct("user_id").cast("long").alias("paid_users"),
        F.round(F.sum("order_total_amount"), 2).alias("sales_amount"),
    )

    return (
        visit_stats.join(order_stats, "channel_id", "left")
        .fillna({"paid_orders": 0, "paid_users": 0, "sales_amount": 0})
        .withColumn(
            "conversion_rate",
            F.when(F.col("visits") > 0, F.round(F.col("paid_orders") / F.col("visits"), 4)).otherwise(F.lit(0.0)),
        )
        .withColumn("computed_at", F.lit(computed_at))
        .select(
            "channel_id",
            "channel_name",
            "channel_type",
            "visits",
            "visit_users",
            "paid_orders",
            "paid_users",
            "sales_amount",
            "conversion_rate",
            "computed_at",
        )
        .orderBy(F.desc("conversion_rate"))
    )


def compute_user_value_summary(order_detail: DataFrame, computed_at: datetime) -> DataFrame:
    paid_orders = (
        order_detail.filter(F.col("is_paid") == 1)
        .select("order_id", "user_id", "user_name", "province", "city", "order_date", "order_total_amount")
        .dropDuplicates(["order_id"])
    )
    return (
        paid_orders.groupBy("user_id", "user_name", "province", "city")
        .agg(
            F.count("*").cast("long").alias("paid_orders"),
            F.round(F.sum("order_total_amount"), 2).alias("total_amount"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .withColumn("computed_at", F.lit(computed_at))
        .select(
            "user_id",
            "user_name",
            "province",
            "city",
            "paid_orders",
            "total_amount",
            "first_order_date",
            "last_order_date",
            "computed_at",
        )
        .orderBy(F.desc("total_amount"))
    )


def compute_product_sales_summary(order_detail: DataFrame, computed_at: datetime) -> DataFrame:
    return (
        order_detail.filter(F.col("is_paid") == 1)
        .groupBy("product_id", "product_name", "category", "brand")
        .agg(
            F.sum("quantity").cast("long").alias("sold_quantity"),
            F.round(F.sum("item_amount"), 2).alias("sales_amount"),
            F.round(F.sum("gross_profit"), 2).alias("gross_profit"),
        )
        .withColumn("computed_at", F.lit(computed_at))
        .select(
            "product_id",
            "product_name",
            "category",
            "brand",
            "sold_quantity",
            "sales_amount",
            "gross_profit",
            "computed_at",
        )
        .orderBy(F.desc("sales_amount"))
    )


def compute_category_refund_summary(
    order_detail: DataFrame,
    refund_detail: DataFrame,
    computed_at: datetime,
) -> DataFrame:
    sold_stats = (
        order_detail.filter(F.col("is_paid") == 1)
        .groupBy("category")
        .agg(F.count("*").cast("long").alias("sold_items"))
    )
    refund_stats = (
        refund_detail.filter(F.col("is_approved") == 1)
        .groupBy("category")
        .agg(
            F.count("*").cast("long").alias("refund_count"),
            F.round(F.sum("refund_amount"), 2).alias("refund_amount"),
        )
    )

    return (
        sold_stats.join(refund_stats, "category", "left")
        .fillna({"refund_count": 0, "refund_amount": 0})
        .withColumn(
            "refund_rate",
            F.when(F.col("sold_items") > 0, F.round(F.col("refund_count") / F.col("sold_items"), 4)).otherwise(F.lit(0.0)),
        )
        .withColumn("computed_at", F.lit(computed_at))
        .select("category", "sold_items", "refund_count", "refund_amount", "refund_rate", "computed_at")
        .orderBy(F.desc("refund_rate"))
    )


def main() -> None:
    computed_at = datetime.now()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    client = clickhouse_client()

    execute_clickhouse_sql_file(client)
    reset_dws_tables(client)

    order_detail = spark.table("lake.sales_dwd.dwd_order_detail").cache()
    visit_detail = spark.table("lake.sales_dwd.dwd_visit_detail").cache()
    refund_detail = spark.table("lake.sales_dwd.dwd_refund_detail").cache()

    dws_tables = [
        (
            "dws_daily_sales_summary",
            compute_daily_sales(order_detail, computed_at),
            [
                "sale_date",
                "paid_orders",
                "paid_users",
                "paid_order_items",
                "sales_amount",
                "avg_order_amount",
                "gross_profit",
                "computed_at",
            ],
        ),
        (
            "dws_channel_summary",
            compute_channel_summary(order_detail, visit_detail, computed_at),
            [
                "channel_id",
                "channel_name",
                "channel_type",
                "visits",
                "visit_users",
                "paid_orders",
                "paid_users",
                "sales_amount",
                "conversion_rate",
                "computed_at",
            ],
        ),
        (
            "dws_user_value_summary",
            compute_user_value_summary(order_detail, computed_at),
            [
                "user_id",
                "user_name",
                "province",
                "city",
                "paid_orders",
                "total_amount",
                "first_order_date",
                "last_order_date",
                "computed_at",
            ],
        ),
        (
            "dws_product_sales_summary",
            compute_product_sales_summary(order_detail, computed_at),
            ["product_id", "product_name", "category", "brand", "sold_quantity", "sales_amount", "gross_profit", "computed_at"],
        ),
        (
            "dws_category_refund_summary",
            compute_category_refund_summary(order_detail, refund_detail, computed_at),
            ["category", "sold_items", "refund_count", "refund_amount", "refund_rate", "computed_at"],
        ),
    ]

    for table_name, df, columns in dws_tables:
        insert_dataframe(client, table_name, df, columns)

    spark.stop()
    print("DWS table computation completed.")


if __name__ == "__main__":
    main()
