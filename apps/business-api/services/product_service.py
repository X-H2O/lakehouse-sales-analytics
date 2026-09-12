from __future__ import annotations

from db import dict_cursor, get_connection


def list_products(limit: int = 30, category: str | None = None) -> list[dict]:
    limit = max(1, min(limit, 100))
    with get_connection() as conn:
        with dict_cursor(conn) as cursor:
            if category:
                cursor.execute(
                    """
                    SELECT product_id, sku, product_name, category, brand, list_price
                    FROM products
                    WHERE category = %s
                    ORDER BY product_id
                    LIMIT %s
                    """,
                    (category, limit),
                )
            else:
                cursor.execute(
                    """
                    SELECT product_id, sku, product_name, category, brand, list_price
                    FROM products
                    ORDER BY product_id
                    LIMIT %s
                    """,
                    (limit,),
                )
            return [dict(row) for row in cursor.fetchall()]


def list_categories() -> list[str]:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT category FROM products ORDER BY category")
            return [row[0] for row in cursor.fetchall()]
