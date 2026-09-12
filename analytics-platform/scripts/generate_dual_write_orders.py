from __future__ import annotations

import argparse
import json
import random
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values


POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sales_ods",
    "user": "sales_user",
    "password": "sales_password",
}

KAFKA_CONTAINER = "bigdata-kafka"
KAFKA_BIN = "/opt/kafka/bin"
BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "sales.order_events"

PAYMENT_METHODS = ["alipay", "wechat_pay", "credit_card", "debit_card"]
REFUND_REASONS = ["quality_issue", "late_delivery", "changed_mind", "damaged_package"]


@dataclass
class Product:
    product_id: int
    list_price: Decimal


@dataclass
class GeneratedOrder:
    order_id: int
    status: str
    total_amount: Decimal


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate same-source orders into PostgreSQL and Kafka.")
    parser.add_argument("--orders", type=int, default=10, help="Number of source orders to append.")
    parser.add_argument("--refund-rate", type=float, default=0.2, help="Refund probability for paid orders.")
    parser.add_argument("--cancel-rate", type=float, default=0.1, help="Cancel probability for generated orders.")
    parser.add_argument("--seed", type=int, default=20260619)
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


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
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            last_error = str(exc)
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


def produce_events(events: list[dict]) -> None:
    if not events:
        return
    payload = "\n".join(json.dumps(event, ensure_ascii=False, sort_keys=True) for event in events) + "\n"
    kafka_command(
        [f"{KAFKA_BIN}/kafka-console-producer.sh", "--bootstrap-server", BOOTSTRAP_SERVER, "--topic", TOPIC],
        input_text=payload,
    )
    print(f"Produced {len(events)} Kafka events.")


def fetch_source_dimensions(cursor) -> tuple[list[int], list[int], list[Product]]:
    cursor.execute("SELECT user_id FROM users ORDER BY user_id")
    user_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT channel_id FROM channels ORDER BY channel_id")
    channel_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT product_id, list_price FROM products ORDER BY product_id")
    products = [Product(product_id=row[0], list_price=Decimal(row[1])) for row in cursor.fetchall()]

    if not user_ids or not channel_ids or not products:
        raise RuntimeError("PostgreSQL source dimensions are incomplete. Run generate_ods_data.py first.")
    return user_ids, channel_ids, products


def next_ids(cursor) -> dict[str, int]:
    cursor.execute(
        """
        SELECT
            COALESCE((SELECT max(order_id) FROM orders), 0) + 1,
            COALESCE((SELECT max(order_item_id) FROM order_items), 0) + 1,
            COALESCE((SELECT max(refund_id) FROM refunds), 0) + 1
        """
    )
    order_id, order_item_id, refund_id = cursor.fetchone()
    return {"order_id": order_id, "order_item_id": order_item_id, "refund_id": refund_id}


def money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def build_rows(
    *,
    run_id: str,
    order_count: int,
    refund_rate: float,
    cancel_rate: float,
    user_ids: list[int],
    channel_ids: list[int],
    products: list[Product],
    ids: dict[str, int],
) -> tuple[list[tuple], list[tuple], list[tuple], list[dict], list[GeneratedOrder]]:
    orders = []
    order_items = []
    refunds = []
    events = []
    generated_orders = []

    base_time = datetime.now(timezone.utc).replace(microsecond=0)
    order_id = ids["order_id"]
    order_item_id = ids["order_item_id"]
    refund_id = ids["refund_id"]

    for index in range(order_count):
        current_order_id = order_id + index
        user_id = random.choice(user_ids)
        channel_id = random.choice(channel_ids)
        order_time = (base_time + timedelta(seconds=index * 5)).replace(tzinfo=None)
        status = "cancelled" if random.random() < cancel_rate else "paid"
        item_count = random.choice([1, 1, 2, 3])
        selected_products = random.sample(products, k=min(item_count, len(products)))
        payment_method = random.choice(PAYMENT_METHODS)

        current_items = []
        total_amount = Decimal("0.00")
        for product in selected_products:
            quantity = random.choice([1, 1, 2])
            unit_price = money(product.list_price * Decimal(str(random.uniform(0.82, 1.0))))
            item_amount = money(unit_price * quantity)
            if status != "paid":
                item_amount = Decimal("0.00")
            total_amount += item_amount
            current_items.append((order_item_id, current_order_id, product.product_id, quantity, unit_price, item_amount))
            order_item_id += 1

        total_amount = money(total_amount)
        orders.append((current_order_id, user_id, channel_id, order_time, status, payment_method, total_amount))
        order_items.extend(current_items)
        generated_orders.append(GeneratedOrder(order_id=current_order_id, status=status, total_amount=total_amount))

        event_time = order_time.replace(tzinfo=timezone.utc).isoformat()
        event_type = "order_cancelled" if status == "cancelled" else "order_paid"
        events.append(
            {
                "run_id": run_id,
                "event_id": f"{run_id}-{event_type}-{current_order_id}",
                "event_type": event_type,
                "order_id": current_order_id,
                "user_id": user_id,
                "channel_id": channel_id,
                "total_amount": float(total_amount),
                "event_time": event_time,
            }
        )

        if status == "paid" and current_items and random.random() < refund_rate:
            item = random.choice(current_items)
            refund_amount = money(Decimal(item[5]) * Decimal(str(random.uniform(0.4, 1.0))))
            refund_time = order_time + timedelta(minutes=random.randint(1, 30))
            refunds.append(
                (
                    refund_id,
                    current_order_id,
                    item[0],
                    user_id,
                    item[2],
                    refund_time,
                    refund_amount,
                    random.choice(REFUND_REASONS),
                    "approved",
                )
            )
            events.append(
                {
                    "run_id": run_id,
                    "event_id": f"{run_id}-order_refunded-{current_order_id}-{refund_id}",
                    "event_type": "order_refunded",
                    "order_id": current_order_id,
                    "user_id": user_id,
                    "channel_id": channel_id,
                    "total_amount": float(refund_amount),
                    "event_time": refund_time.replace(tzinfo=timezone.utc).isoformat(),
                }
            )
            refund_id += 1

    return orders, order_items, refunds, events, generated_orders


def insert_source_rows(cursor, orders: list[tuple], order_items: list[tuple], refunds: list[tuple]) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO orders (order_id, user_id, channel_id, order_time, status, payment_method, total_amount)
        VALUES %s
        """,
        orders,
    )
    execute_values(
        cursor,
        """
        INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, item_amount)
        VALUES %s
        """,
        order_items,
    )
    if refunds:
        execute_values(
            cursor,
            """
            INSERT INTO refunds (refund_id, order_id, order_item_id, user_id, product_id, refund_time, refund_amount, reason, status)
            VALUES %s
            """,
            refunds,
        )


def verify_postgres(cursor, first_order_id: int, last_order_id: int) -> tuple[int, Decimal, int]:
    cursor.execute(
        """
        SELECT
            count(*) FILTER (WHERE status = 'paid'),
            COALESCE(round(sum(total_amount) FILTER (WHERE status = 'paid'), 2), 0),
            count(*) FILTER (WHERE status = 'cancelled')
        FROM orders
        WHERE order_id BETWEEN %s AND %s
        """,
        (first_order_id, last_order_id),
    )
    paid_count, paid_amount, cancel_count = cursor.fetchone()
    return int(paid_count), Decimal(paid_amount), int(cancel_count)


def main() -> None:
    args = parse_args()
    random.seed(args.seed)
    run_id = args.run_id or f"dual-{uuid.uuid4().hex[:8]}"

    run_command(["docker", "compose", "up", "-d", "postgres", "kafka"], timeout=180)
    wait_for_kafka()
    create_topic()

    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cursor:
            user_ids, channel_ids, products = fetch_source_dimensions(cursor)
            ids = next_ids(cursor)
            orders, order_items, refunds, events, generated_orders = build_rows(
                run_id=run_id,
                order_count=args.orders,
                refund_rate=args.refund_rate,
                cancel_rate=args.cancel_rate,
                user_ids=user_ids,
                channel_ids=channel_ids,
                products=products,
                ids=ids,
            )
            insert_source_rows(cursor, orders, order_items, refunds)

            first_order_id = orders[0][0]
            last_order_id = orders[-1][0]
            paid_count, paid_amount, cancel_count = verify_postgres(cursor, first_order_id, last_order_id)

    produce_events(events)

    event_paid_count = sum(1 for event in events if event["event_type"] == "order_paid")
    event_paid_amount = money(sum((Decimal(str(event["total_amount"])) for event in events if event["event_type"] == "order_paid"), Decimal("0")))
    event_cancel_count = sum(1 for event in events if event["event_type"] == "order_cancelled")
    event_refund_count = sum(1 for event in events if event["event_type"] == "order_refunded")

    if paid_count != event_paid_count or paid_amount != event_paid_amount or cancel_count != event_cancel_count:
        raise RuntimeError(
            "Dual-write verification failed: "
            f"postgres_paid={paid_count}/{paid_amount}, kafka_paid={event_paid_count}/{event_paid_amount}, "
            f"postgres_cancelled={cancel_count}, kafka_cancelled={event_cancel_count}"
        )

    print("Dual-write order generation completed.")
    print(f"run_id={run_id}")
    print(f"orders={len(generated_orders)} order_id_range={orders[0][0]}..{orders[-1][0]}")
    print(f"order_items={len(order_items)} refunds={len(refunds)} kafka_events={len(events)}")
    print(f"paid_orders={paid_count} paid_amount={paid_amount} cancelled_orders={cancel_count} refund_events={event_refund_count}")


if __name__ == "__main__":
    main()
