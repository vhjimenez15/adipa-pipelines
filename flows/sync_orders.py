"""
Pipeline liviano — sync_orders
Ejecuta cada 15 min (configurable via SYNC_ORDERS_SCHEDULE).
Trae órdenes nuevas de WooCommerce (mock) por país y hace UPSERT en raw_orders.
"""
import os
from datetime import datetime, timezone

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact

from db import get_conn, bulk_upsert
from woocommerce_mock import fetch_orders

COUNTRIES = [c.strip() for c in os.environ.get("WOOCOMMERCE_COUNTRIES", "CL,MX,CO").split(",")]


@task(retries=3, retry_delay_seconds=30)
def pull_country_orders(country_code: str) -> list[dict]:
    logger = get_run_logger()
    orders = fetch_orders(country_code)
    logger.info(f"[{country_code}] Fetched {len(orders)} orders from WooCommerce")
    return orders


@task(retries=2, retry_delay_seconds=10)
def upsert_orders(country_code: str, orders: list[dict]) -> int:
    logger = get_run_logger()
    if not orders:
        return 0

    rows = [
        {
            "order_id": o["id"],
            "country_code": country_code,
            "status": o["status"],
            "currency": o["currency"],
            "total": float(o["total"]),
            "product_name": o["line_items"][0]["name"],
            "quantity": o["line_items"][0]["quantity"],
            "customer_email": o["billing"].get("email"),
            "order_date": o["date_created"],
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }
        for o in orders
    ]

    with get_conn() as conn:
        affected = bulk_upsert(
            conn, "raw_orders", rows,
            conflict_cols=["order_id", "country_code"],
            update_cols=["status", "total", "synced_at"],
        )
        conn.commit()

    logger.info(f"[{country_code}] Upserted {affected} rows into raw_orders")
    return affected


@task
def log_sync(pipeline: str, country_code: str, rows_affected: int, started_at: datetime):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sync_log (pipeline, country_code, status, rows_affected, started_at)
                VALUES (%s, %s, 'success', %s, %s)
                """,
                (pipeline, country_code, rows_affected, started_at),
            )
        conn.commit()


@task
def publish_summary(results: list[dict]):
    """Publica una tabla en la UI de Prefect con el resumen de esta ejecución."""
    create_table_artifact(
        key="sync-orders-summary",
        table=results,
        description="Órdenes sincronizadas en esta ejecución por país",
    )


@flow(name="sync_orders", log_prints=True)
def sync_orders():
    """Pulls WooCommerce orders for all countries and persists them idempotently."""
    started_at = datetime.now(timezone.utc)
    summary = []

    for country in COUNTRIES:
        orders = pull_country_orders(country)
        affected = upsert_orders(country, orders)
        log_sync("sync_orders", country, affected, started_at)
        summary.append({
            "país": country,
            "órdenes_recibidas": len(orders),
            "filas_upserted": affected,
            "revenue_local": round(sum(float(o["total"]) for o in orders), 2),
            "moneda": orders[0]["currency"] if orders else "-",
        })

    publish_summary(summary)
