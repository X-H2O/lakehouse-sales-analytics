from __future__ import annotations

import json
import os
import subprocess


KAFKA_CONTAINER = "bigdata-kafka"
KAFKA_BIN = "/opt/kafka/bin"
BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sales.order_events")


def run_command(command: list[str], *, input_text: str | None = None, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=True,
    )


def kafka_command(args: list[str], *, input_text: str | None = None, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return run_command(["docker", "exec", "-i", KAFKA_CONTAINER, *args], input_text=input_text, timeout=timeout)


def ensure_topic() -> None:
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


def produce_events(events: list[dict]) -> None:
    if not events:
        return
    ensure_topic()
    payload = "\n".join(json.dumps(event, ensure_ascii=False, sort_keys=True) for event in events) + "\n"
    kafka_command(
        [f"{KAFKA_BIN}/kafka-console-producer.sh", "--bootstrap-server", BOOTSTRAP_SERVER, "--topic", TOPIC],
        input_text=payload,
    )
