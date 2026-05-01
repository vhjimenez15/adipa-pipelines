"""
Pipeline pesado — daily_kpi_report
Ejecuta 1x/día (00:00 America/Bogotá, configurable via KPI_REPORT_TIMEZONE).
Lee raw_orders del día anterior, normaliza a USD con Pandas y persiste KPIs por país.
Pandas vive aquí — este flow corre SOLO en worker-heavy, nunca en el orquestador.
"""
import os
import json
import requests
import logging
from datetime import datetime, timedelta, timezone, date

import pandas as pd
from prefect import flow, task, get_run_logger

from db import get_conn, bulk_upsert

COUNTRIES = [c.strip() for c in os.environ.get("WOOCOMMERCE_COUNTRIES", "CL,MX,CO").split(",")]
EXCHANGE_API = os.environ.get("EXCHANGE_RATE_API_URL", "https://api.frankfurter.app")

CURRENCY_MAP = {"CL": "CLP", "MX": "MXN", "CO": "COP"}


@task(retries=3, retry_delay_seconds=60)
def fetch_exchange_rates(target_currencies: list[str]) -> dict[str, float]:
    """
    Fetches CLP, MXN, COP → USD from open.er-api.com (free, no key needed).
    Supports LatAm currencies unlike frankfurter.app.
    """
    logger = get_run_logger()
    resp = requests.get(f"{EXCHANGE_API}/v6/latest/USD", timeout=10)
    resp.raise_for_status()
    all_rates = resp.json()["rates"]  # USD-based: {"CLP": 950.5, "MXN": 17.2, ...}
    usd_rates = {}
    for currency in target_currencies:
        if currency in all_rates:
            usd_rates[currency] = 1 / all_rates[currency]
        else:
            logger.warning(f"{currency} not found in exchange rate API, defaulting to 1.0")
            usd_rates[currency] = 1.0
    logger.info(f"Exchange rates (local → USD): {usd_rates}")
    return usd_rates


@task
def load_raw_orders(report_date: date) -> pd.DataFrame:
    """Loads raw_orders for report_date from Postgres."""
    logger = get_run_logger()
    query = """
        SELECT order_id, country_code, status, currency, total, product_name, quantity, order_date
        FROM raw_orders
        WHERE order_date::date = %s
          AND status IN ('completed', 'processing')
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (report_date,))
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=cols)
    df["total"] = df["total"].astype(float)
    logger.info(f"Loaded {len(df)} orders for {report_date}")
    return df


@task
def compute_kpis(df: pd.DataFrame, rates: dict[str, float]) -> list[dict]:
    """Aggregates orders by country and computes KPIs. Returns list of report rows."""
    logger = get_run_logger()

    if df.empty:
        logger.warning("No orders to process — skipping KPI computation")
        return []

    df["revenue_usd"] = df.apply(
        lambda r: r["total"] * rates.get(r["currency"], 1.0), axis=1
    )

    results = []
    for country, group in df.groupby("country_code"):
        top_courses = (
            group.groupby("product_name")["quantity"]
            .sum()
            .sort_values(ascending=False)
            .head(3)
            .reset_index()
            .rename(columns={"product_name": "name", "quantity": "units_sold"})
            .to_dict(orient="records")
        )
        currency = CURRENCY_MAP.get(country, "USD")
        results.append({
            "country_code": country,
            "total_orders": int(len(group)),
            "revenue_local": float(group["total"].sum()),
            "currency": currency,
            "revenue_usd": float(group["revenue_usd"].sum()),
            "exchange_rate": float(rates.get(currency, 1.0)),
            "top_courses": json.dumps(top_courses),
        })
        logger.info(f"[{country}] orders={len(group)}, revenue_usd={group['revenue_usd'].sum():.2f}")

    return results


@task
def fetch_prev_day_revenue(report_date: date) -> dict[str, float]:
    """Returns {country_code: revenue_usd} for the day before report_date."""
    prev = report_date - timedelta(days=1)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT country_code, revenue_usd FROM kpi_daily_report WHERE report_date = %s",
                (prev,),
            )
            return {row[0]: float(row[1]) for row in cur.fetchall()}


@task(retries=2)
def upsert_kpi_report(report_date: date, kpis: list[dict], prev_revenues: dict[str, float]) -> int:
    logger = get_run_logger()
    if not kpis:
        return 0

    rows = []
    for kpi in kpis:
        prev = prev_revenues.get(kpi["country_code"])
        if prev and prev > 0:
            vs_prev = round(((kpi["revenue_usd"] - prev) / prev) * 100, 2)
        else:
            vs_prev = None

        rows.append({
            "report_date": report_date.isoformat(),
            "country_code": kpi["country_code"],
            "total_orders": kpi["total_orders"],
            "revenue_local": kpi["revenue_local"],
            "currency": kpi["currency"],
            "revenue_usd": kpi["revenue_usd"],
            "exchange_rate": kpi["exchange_rate"],
            "top_courses": kpi["top_courses"],
            "vs_prev_day_pct": vs_prev,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

    with get_conn() as conn:
        affected = bulk_upsert(
            conn, "kpi_daily_report", rows,
            conflict_cols=["report_date", "country_code"],
            update_cols=["total_orders", "revenue_local", "revenue_usd", "exchange_rate",
                         "top_courses", "vs_prev_day_pct", "updated_at"],
        )
        conn.commit()

    logger.info(f"Upserted {affected} KPI rows for {report_date}")
    return affected


@task
def log_sync(pipeline: str, rows_affected: int, started_at: datetime):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sync_log (pipeline, status, rows_affected, started_at)
                VALUES (%s, 'success', %s, %s)
                """,
                (pipeline, rows_affected, started_at),
            )
        conn.commit()


@flow(name="daily_kpi_report", log_prints=True)
def daily_kpi_report():
    """
    Consumes raw_orders from yesterday, normalizes to USD, computes per-country KPIs.
    Depends on sync_orders having run throughout the day before.
    """
    started_at = datetime.now(timezone.utc)
    # Report always covers yesterday so the day's data is complete
    report_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    currencies = list(CURRENCY_MAP.values())
    rates = fetch_exchange_rates(currencies)
    df = load_raw_orders(report_date)
    kpis = compute_kpis(df, rates)
    prev_revenues = fetch_prev_day_revenue(report_date)
    affected = upsert_kpi_report(report_date, kpis, prev_revenues)
    log_sync("daily_kpi_report", affected, started_at)
