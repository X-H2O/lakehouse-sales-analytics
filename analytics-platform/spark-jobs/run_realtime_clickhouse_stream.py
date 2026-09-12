from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from urllib.request import urlopen

import clickhouse_connect


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RT_SCHEMA_FILE = PROJECT_ROOT / "sql" / "04_create_rt_tables.sql"
LOCAL_HADOOP_HOME = PROJECT_ROOT / "tools" / "hadoop"

os.environ.setdefault("HADOOP_HOME", str(LOCAL_HADOOP_HOME))
os.environ.setdefault("hadoop.home.dir", str(LOCAL_HADOOP_HOME))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType


KAFKA_CONTAINER = "bigdata-kafka"
KAFKA_BIN = "/opt/kafka/bin"
BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "sales.order_events"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"

MINIO_ENDPOINT = "http://localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
CHECKPOINT_DIR = "s3a://warehouse/checkpoints/spark/run_realtime_clickhouse_stream_v25"

CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "port": 8123,
    "database": "sales_ads",
    "username": "sales_user",
    "password": "sales_password",
}

RT_TABLE = "rt_order_events"
RT_COLUMNS = [
    "event_id",
    "run_id",
    "event_type",
    "order_id",
    "user_id",
    "channel_id",
    "total_amount",
    "event_time",
    "kafka_timestamp",
    "topic",
    "partition",
    "offset",
    "raw_event",
    "ingested_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously stream Kafka order events into ClickHouse.")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--bootstrap-server", default=BOOTSTRAP_SERVER)
    parser.add_argument("--master", default="local[1]")
    parser.add_argument("--checkpoint", default=CHECKPOINT_DIR)
    parser.add_argument("--starting-offsets", default="earliest", choices=["latest", "earliest"])
    parser.add_argument("--run-id-prefix", default="business-")
    parser.add_argument("--trigger", default="processing-time", choices=["processing-time", "available-now"])
    parser.add_argument("--processing-time", default="5 seconds")
    parser.add_argument("--max-runtime-seconds", type=int, default=0, help="Stop after N seconds. 0 means run forever.")
    parser.add_argument("--verbose-existing", action="store_true", help="Print events that already exist in ClickHouse.")
    return parser.parse_args()


def run_command(command: list[str], *, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, timeout=timeout, check=True)


def kafka_command(args: list[str], timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return run_command(["docker", "exec", "-i", KAFKA_CONTAINER, *args], timeout=timeout)


def clickhouse_client():
    return clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)


def execute_sql_file(client) -> None:
    statements = [stmt.strip() for stmt in RT_SCHEMA_FILE.read_text(encoding="utf-8").split(";")]
    for statement in statements:
        if statement:
            client.command(statement)


def wait_for_clickhouse(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            client = clickhouse_client()
            client.command("SELECT 1")
            print("ClickHouse is reachable.", flush=True)
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("ClickHouse is not reachable.")


def wait_for_kafka(bootstrap_server: str, timeout_seconds: int = 120) -> None:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            kafka_command([f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", bootstrap_server, "--list"], timeout=10)
            print("Kafka is reachable.", flush=True)
            return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            last_error = str(exc)
            time.sleep(3)
    raise RuntimeError(f"Kafka is not reachable: {last_error}")


def wait_for_minio(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urlopen(f"{MINIO_ENDPOINT}/minio/health/live", timeout=5) as response:
                if response.status == 200:
                    print("MinIO is reachable.", flush=True)
                    return
        except Exception:
            time.sleep(2)
    raise RuntimeError("MinIO is not reachable.")


def create_topic(topic: str, bootstrap_server: str) -> None:
    kafka_command(
        [
            f"{KAFKA_BIN}/kafka-topics.sh",
            "--bootstrap-server",
            bootstrap_server,
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ]
    )
    print(f"Topic is ready: {topic}", flush=True)


def create_spark(checkpoint: str, master: str) -> SparkSession:
    packages = [KAFKA_PACKAGE]
    if checkpoint.startswith("s3a://"):
        packages.append(HADOOP_AWS_PACKAGE)

    builder = (
        SparkSession.builder.appName("run-realtime-clickhouse-stream")
        .master(master)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
    )

    if checkpoint.startswith("s3a://"):
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        )

    return builder.getOrCreate()


def event_schema() -> StructType:
    return StructType(
        [
            StructField("run_id", StringType(), nullable=False),
            StructField("event_id", StringType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("order_id", LongType(), nullable=False),
            StructField("user_id", LongType(), nullable=False),
            StructField("channel_id", IntegerType(), nullable=False),
            StructField("total_amount", DoubleType(), nullable=False),
            StructField("event_time", StringType(), nullable=False),
        ]
    )


def parse_kafka_stream(stream: DataFrame) -> DataFrame:
    raw = stream.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_event"),
    )
    parsed = raw.withColumn("event", F.from_json("raw_event", event_schema()))
    return (
        parsed.select(
            F.col("event.event_id").alias("event_id"),
            F.col("event.run_id").alias("run_id"),
            F.col("event.event_type").alias("event_type"),
            F.col("event.order_id").alias("order_id"),
            F.col("event.user_id").alias("user_id"),
            F.col("event.channel_id").alias("channel_id"),
            F.round(F.col("event.total_amount"), 2).alias("total_amount"),
            F.to_timestamp("event.event_time").alias("event_time"),
            "kafka_timestamp",
            "topic",
            "partition",
            "offset",
            "raw_event",
            F.current_timestamp().alias("ingested_at"),
        )
        .where(F.col("event_id").isNotNull())
        .where(F.col("run_id").isNotNull())
    )


def to_python_value(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


def existing_event_ids(client, event_ids: list[str]) -> set[str]:
    if not event_ids:
        return set()
    escaped = ",".join("'" + event_id.replace("\\", "\\\\").replace("'", "\\'") + "'" for event_id in event_ids)
    rows = client.query(f"SELECT event_id FROM sales_ads.{RT_TABLE} WHERE event_id IN ({escaped})").result_rows
    return {row[0] for row in rows}


def refresh_minute_summary(client) -> None:
    client.command("TRUNCATE TABLE sales_ads.rt_order_minute_summary")
    client.command(
        """
        INSERT INTO sales_ads.rt_order_minute_summary
        SELECT
            toStartOfMinute(event_time) AS event_minute,
            count() AS event_count,
            countIf(event_type = 'order_paid') AS paid_order_count,
            toDecimal64(sumIf(total_amount, event_type = 'order_paid'), 2) AS sales_amount,
            countIf(event_type = 'order_refunded') AS refund_count,
            countIf(event_type = 'order_cancelled') AS cancel_count,
            min(ingested_at) AS first_ingested_at,
            max(ingested_at) AS last_ingested_at,
            now() AS refreshed_at
        FROM sales_ads.rt_order_events
        GROUP BY event_minute
        ORDER BY event_minute
        """
    )


def process_batch(batch_df: DataFrame, batch_id: int, verbose_existing: bool) -> None:
    rows = batch_df.orderBy("event_id").collect()
    if not rows:
        return

    client = clickhouse_client()
    event_ids = [row["event_id"] for row in rows]
    already_inserted = existing_event_ids(client, event_ids)
    insert_rows = [
        tuple(to_python_value(row[column]) for column in RT_COLUMNS)
        for row in rows
        if row["event_id"] not in already_inserted
    ]

    if insert_rows:
        client.insert(f"sales_ads.{RT_TABLE}", insert_rows, column_names=RT_COLUMNS)
    refresh_minute_summary(client)

    if verbose_existing:
        for row in rows:
            print(
                f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                f"event_id={row['event_id']} order_id={row['order_id']} inserted={row['event_id'] not in already_inserted}",
                flush=True,
            )

    print(
        f"{datetime.now():%Y-%m-%d %H:%M:%S} batch={batch_id} "
        f"received={len(rows)} inserted={len(insert_rows)} skipped={len(rows) - len(insert_rows)}",
        flush=True,
    )


def run_stream(
    spark: SparkSession,
    *,
    topic: str,
    bootstrap_server: str,
    checkpoint: str,
    starting_offsets: str,
    run_id_prefix: str,
    trigger: str,
    processing_time: str,
    max_runtime_seconds: int,
    verbose_existing: bool,
) -> None:
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = parse_kafka_stream(stream)
    if run_id_prefix:
        parsed = parsed.where(F.col("run_id").startswith(run_id_prefix))

    writer = (
        parsed.writeStream.foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, verbose_existing))
        .option("checkpointLocation", checkpoint)
    )
    if trigger == "available-now":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=processing_time)
    query = writer.start()

    print("Realtime Spark stream started.", flush=True)
    print(f"topic={topic}", flush=True)
    print(f"checkpoint={checkpoint}", flush=True)
    print(f"starting_offsets={starting_offsets}", flush=True)
    print(f"run_id_prefix={run_id_prefix or '(none)'}", flush=True)
    print(f"trigger={trigger}", flush=True)
    print(f"processing_time={processing_time}", flush=True)

    try:
        if max_runtime_seconds > 0:
            query.awaitTermination(max_runtime_seconds)
        else:
            query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()


def main() -> None:
    args = parse_args()

    run_command(["docker", "compose", "up", "-d", "kafka", "minio", "minio-init", "clickhouse"], timeout=180)
    wait_for_kafka(args.bootstrap_server)
    if args.checkpoint.startswith("s3a://"):
        wait_for_minio()
    wait_for_clickhouse()

    client = clickhouse_client()
    execute_sql_file(client)
    create_topic(args.topic, args.bootstrap_server)

    spark = create_spark(args.checkpoint, args.master)
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_stream(
            spark,
            topic=args.topic,
            bootstrap_server=args.bootstrap_server,
            checkpoint=args.checkpoint,
            starting_offsets=args.starting_offsets,
            run_id_prefix=args.run_id_prefix,
            trigger=args.trigger,
            processing_time=args.processing_time,
            max_runtime_seconds=args.max_runtime_seconds,
            verbose_existing=args.verbose_existing,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
