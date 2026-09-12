from __future__ import annotations

from contextlib import contextmanager
import os
from typing import Iterator

import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor


POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "sales_ods"),
    "user": os.getenv("POSTGRES_USER", "sales_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "sales_password"),
}


@contextmanager
def get_connection() -> Iterator[connection]:
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def dict_cursor(conn: connection):
    return conn.cursor(cursor_factory=RealDictCursor)
