"""Thin DB connection helper — keeps connection logic in one place."""
import os
import psycopg2
from psycopg2.extras import execute_values


def get_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


def bulk_upsert(conn, table: str, rows: list[dict], conflict_cols: list[str], update_cols: list[str]) -> int:
    """Generic UPSERT. Returns number of rows affected."""
    if not rows:
        return 0

    cols = list(rows[0].keys())
    values = [[r[c] for c in cols] for r in rows]
    conflict = ", ".join(conflict_cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {table} ({", ".join(cols)})
        VALUES %s
        ON CONFLICT ({conflict}) DO UPDATE SET {updates}
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
        return cur.rowcount
