from __future__ import annotations

from fastapi import HTTPException

from db import dict_cursor, get_connection


def login(user_id: int) -> dict:
    with get_connection() as conn:
        with dict_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT user_id, user_name, province, city, channel_id
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            user = cursor.fetchone()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {"token": f"dev-user-{user_id}", "user": dict(user)}
