from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import clickhouse_connect

ROOT = Path(__file__).resolve().parents[1]
AP = ROOT / "analytics-platform"
CH = dict(host="localhost", port=8123, database="sales_ads", username="sales_user", password="sales_password")
KAFKA_CONTAINER = "bigdata-kafka"
KAFKA_BIN = "/opt/kafka/bin"
TOPIC = "sales.order_events"
N_EVENTS = 200


def kafka_cmd(args, input_text=None, timeout=60):
    return subprocess.run(["docker", "exec", "-i", KAFKA_CONTAINER, *args],
                          input=input_text, text=True, capture_output=True, timeout=timeout)


def ensure_setup(ch):
    kafka_cmd([f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
               "--create", "--if-not-exists", "--topic", TOPIC, "--partitions", "1", "--replication-factor", "1"])
    for stmt in (AP / "sql" / "04_create_rt_tables.sql").read_text(encoding="utf-8").split(";"):
        if stmt.strip():
            ch.command(stmt)


def produce_events(n, run_id):
    now = datetime.now(timezone.utc)
    payload_lines = []
    for i in range(n):
        e = {
            "run_id": run_id,
            "event_id": f"bench-{i:05d}",
            "event_type": "order_paid",
            "order_id": 700000 + i,
            "user_id": 1000 + (i % 50),
            "channel_id": 1 + (i % 8),
            "total_amount": round(10 + (i % 100) * 1.5, 2),
            "event_time": (now - timedelta(seconds=1)).isoformat(),
        }
        payload_lines.append(json.dumps(e))
    payload = "\n".join(payload_lines) + "\n"
    kafka_cmd([f"{KAFKA_BIN}/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092", "--topic", TOPIC], payload)
    return now


def main():
    ch = clickhouse_connect.get_client(**CH)
    ensure_setup(ch)

    run_id = "bench-" + uuid.uuid4().hex[:8]
    checkpoint = f"s3a://warehouse/checkpoints/bench-{int(time.time())}"

    stream = subprocess.Popen(
        [sys.executable, str(AP / "spark-jobs" / "run_realtime_clickhouse_stream.py"),
         "--trigger", "processing-time", "--processing-time", "2 seconds",
         "--starting-offsets", "latest", "--checkpoint", checkpoint, "--run-id-prefix", "bench-"],
        cwd=AP, stdout=open(str(ROOT / "benchmarks" / "results" / "stream_job.log"), "w"), stderr=subprocess.STDOUT,
    )

    # wait for Spark streaming to boot (JVM + Kafka connector jar download may happen first time)
    print("waiting 40s for streaming job to boot...", flush=True)
    time.sleep(60)

    produce_time = produce_events(N_EVENTS, run_id)
    t0 = time.time()
    print(f"produced {N_EVENTS} events at {produce_time.isoformat()}", flush=True)

    # poll until all ingested
    ingested = 0
    deadline = time.time() + 180
    while time.time() < deadline:
        ingested = ch.query(f"SELECT count() FROM sales_ads.rt_order_events WHERE run_id='{run_id}'").result_rows[0][0]
        if ingested >= N_EVENTS:
            break
        time.sleep(2)

    t1 = time.time()
    rows = ch.query(f"SELECT event_time, ingested_at FROM sales_ads.rt_order_events WHERE run_id='{run_id}' ORDER BY event_id").result_rows
    latencies_ms = sorted((b - a).total_seconds() * 1000 for a, b in rows)

    stream.terminate()
    try:
        stream.wait(timeout=30)
    except subprocess.TimeoutExpired:
        stream.kill()

    results = {
        "events_produced": N_EVENTS,
        "events_ingested": ingested,
        "end_to_end_wall_seconds": round(t1 - t0, 2),
        "latency_ms": {
            "count": len(latencies_ms),
            "median": round(latencies_ms[len(latencies_ms)//2], 1) if latencies_ms else None,
            "min": round(min(latencies_ms), 1) if latencies_ms else None,
            "max": round(max(latencies_ms), 1) if latencies_ms else None,
        },
        "throughput_events_per_second": round(N_EVENTS / (t1 - t0), 2) if t1 > t0 else None,
    }
    print(json.dumps(results, indent=2, ensure_ascii=False))

    out = ROOT / "benchmarks" / "results" / "streaming_latency.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()