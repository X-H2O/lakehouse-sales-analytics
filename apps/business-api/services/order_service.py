from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from db import dict_cursor, get_connection
from kafka_producer import produce_events
from schemas import CreateOrderRequest


PAYMENT_METHOD = "simulated_pay"
LOCAL_TIMEZONE = ZoneInfo("Asia/Shanghai")


def money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def _next_ids(cursor) -> tuple[int, int]:
    cursor.execute(
        """
        SELECT
            COALESCE((SELECT max(order_id) FROM orders), 0) + 1 AS next_order_id,
            COALESCE((SELECT max(order_item_id) FROM order_items), 0) + 1 AS next_order_item_id
        """
    )
    row = cursor.fetchone()
    return row["next_order_id"], row["next_order_item_id"]


def _validate_user_and_channel(cursor, user_id: int, channel_id: int) -> None:
    cursor.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
    if cursor.fetchone() is None:
        raise HTTPException(status_code=404, detail="User not found")

    cursor.execute("SELECT 1 FROM channels WHERE channel_id = %s", (channel_id,))
    if cursor.fetchone() is None:
        raise HTTPException(status_code=404, detail="Channel not found")


def _load_products(cursor, product_ids: list[int]) -> dict[int, dict]:
    cursor.execute(
        """
        SELECT product_id, product_name, list_price
        FROM products
        WHERE product_id = ANY(%s)
        """,
        (product_ids,),
    )
    products = {row["product_id"]: dict(row) for row in cursor.fetchall()}
    missing = sorted(set(product_ids) - set(products))
    if missing:
        raise HTTPException(status_code=404, detail=f"Products not found: {missing}")
    return products


def create_order(payload: CreateOrderRequest) -> dict:
    run_id = f"business-{uuid.uuid4().hex[:8]}"
    event_time = datetime.now(LOCAL_TIMEZONE).replace(microsecond=0)
    order_time = event_time.replace(tzinfo=None)

    with get_connection() as conn:
        with dict_cursor(conn) as cursor:
            _validate_user_and_channel(cursor, payload.user_id, payload.channel_id)
            product_ids = [item.product_id for item in payload.items]
            products = _load_products(cursor, product_ids)
            order_id, next_order_item_id = _next_ids(cursor)

            response_items = []
            insert_items = []
            total_amount = Decimal("0.00")

            for item in payload.items:
                product = products[item.product_id]
                unit_price = money(Decimal(product["list_price"]))
                item_amount = money(unit_price * item.quantity)
                total_amount += item_amount
                response_items.append(
                    {
                        "order_item_id": next_order_item_id,
                        "product_id": item.product_id,
                        "product_name": product["product_name"],
                        "quantity": item.quantity,
                        "unit_price": unit_price,
                        "item_amount": item_amount,
                    }
                )
                insert_items.append((next_order_item_id, order_id, item.product_id, item.quantity, unit_price, item_amount))
                next_order_item_id += 1

            total_amount = money(total_amount)
            cursor.execute(
                """
                INSERT INTO orders (order_id, user_id, channel_id, order_time, status, payment_method, total_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (order_id, payload.user_id, payload.channel_id, order_time, "paid", PAYMENT_METHOD, total_amount),
            )
            cursor.executemany(
                """
                INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, item_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                insert_items,
            )

    produce_events(
        [
            {
                "run_id": run_id,
                "event_id": f"{run_id}-order_paid-{order_id}",
                "event_type": "order_paid",
                "order_id": order_id,
                "user_id": payload.user_id,
                "channel_id": payload.channel_id,
                "total_amount": float(total_amount),
                "event_time": event_time.isoformat(),
            }
        ]
    )

    return {
        "order_id": order_id,
        "user_id": payload.user_id,
        "channel_id": payload.channel_id,
        "status": "paid",
        "total_amount": total_amount,
        "kafka_run_id": run_id,
        "items": response_items,
    }


def recent_orders(limit: int = 20) -> list[dict]:
    limit = max(1, min(limit, 100))
    with get_connection() as conn:
        with dict_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT
                    o.order_id,
                    o.user_id,
                    u.user_name,
                    o.order_time::text AS order_time,
                    o.status,
                    o.total_amount
                FROM orders o
                JOIN users u ON o.user_id = u.user_id
                ORDER BY o.order_time DESC, o.order_id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in cursor.fetchall()]
