from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime, timezone


KAFKA_CONTAINER = "bigdata-kafka"
BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "sales.order_events"
KAFKA_BIN = "/opt/kafka/bin"


def run_command(command: list[str], *, input_text: str | None = None, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            input=input_text,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        print(exc.stdout)
        print(exc.stderr)
        raise


def kafka_command(args: list[str], *, input_text: str | None = None, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return run_command(["docker", "exec", "-i", KAFKA_CONTAINER, *args], input_text=input_text, timeout=timeout)


def wait_for_kafka(timeout_seconds: int = 120) -> None:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            kafka_command([f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", BOOTSTRAP_SERVER, "--list"], timeout=10)
            print("Kafka is reachable.")
            return
        except subprocess.CalledProcessError as exc:
            last_error = exc.stderr.strip() or exc.stdout.strip()
            time.sleep(3)
        except subprocess.TimeoutExpired:
            last_error = "timeout while listing topics"
            time.sleep(3)
    raise RuntimeError(f"Kafka is not reachable: {last_error}")


def create_topic() -> None:
    kafka_command(
        [
            f"{KAFKA_BIN}/kafka-topics.sh",
            "--bootstrap-server",
            BOOTSTRAP_SERVER,
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ]
    )
    print(f"Topic is ready: {TOPIC}")


def sample_events() -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    return [
        {
            "event_id": "evt-0001",
            "event_type": "order_paid",
            "order_id": 900001,
            "user_id": 101,
            "channel_id": 1,
            "total_amount": 239.90,
            "event_time": now,
        },
        {
            "event_id": "evt-0002",
            "event_type": "order_paid",
            "order_id": 900002,
            "user_id": 102,
            "channel_id": 4,
            "total_amount": 1288.00,
            "event_time": now,
        },
        {
            "event_id": "evt-0003",
            "event_type": "order_refunded",
            "order_id": 900001,
            "user_id": 101,
            "channel_id": 1,
            "total_amount": 89.90,
            "event_time": now,
        },
    ]


def produce_events(events: list[dict]) -> None:
    payload = "\n".join(json.dumps(event, ensure_ascii=False, sort_keys=True) for event in events) + "\n"
    kafka_command(
        [f"{KAFKA_BIN}/kafka-console-producer.sh", "--bootstrap-server", BOOTSTRAP_SERVER, "--topic", TOPIC],
        input_text=payload,
    )
    print(f"Produced {len(events)} order events.")


def consume_events(expected_count: int) -> list[dict]:
    result = kafka_command(
        [
            f"{KAFKA_BIN}/kafka-console-consumer.sh",
            "--bootstrap-server",
            BOOTSTRAP_SERVER,
            "--topic",
            TOPIC,
            "--from-beginning",
            "--timeout-ms",
            "10000",
            "--max-messages",
            str(expected_count),
        ],
        timeout=30,
    )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    events = [json.loads(line) for line in lines[-expected_count:]]
    print(f"Consumed {len(events)} order events.")
    return events


def main() -> None:
    run_command(["docker", "compose", "up", "-d", "kafka"], timeout=180)
    wait_for_kafka()
    create_topic()

    events = sample_events()
    produce_events(events)
    consumed = consume_events(len(events))

    produced_ids = {event["event_id"] for event in events}
    consumed_ids = {event["event_id"] for event in consumed}
    if produced_ids != consumed_ids:
        raise RuntimeError(f"Consumed event ids do not match. produced={produced_ids}, consumed={consumed_ids}")

    print("Kafka order event smoke test completed.")


if __name__ == "__main__":
    main()
