from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query
from app.auth import get_current_user
from app.db import get_conn

router = APIRouter(prefix="/orders", tags=["Orders"])


@router.get("/", summary="List raw orders")
def list_orders(
    country: Optional[str] = Query(None, description="Filter by country code: CL, MX, CO"),
    order_date: Optional[date] = Query(None, description="Filter by order date (YYYY-MM-DD)"),
    status: Optional[str] = Query(None, description="Filter by status: completed, processing, on-hold"),
    limit: int = Query(50, le=500),
    _: str = Depends(get_current_user),
):
    filters = []
    params = []

    if country:
        filters.append("country_code = %s")
        params.append(country.upper())
    if order_date:
        filters.append("order_date::date = %s")
        params.append(order_date)
    if status:
        filters.append("status = %s")
        params.append(status)

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM raw_orders {where} ORDER BY order_date DESC LIMIT %s",
                params,
            )
            return cur.fetchall()


@router.get("/summary", summary="Orders grouped by country and date")
def orders_summary(_: str = Depends(get_current_user)):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    country_code,
                    order_date::date AS date,
                    COUNT(*)         AS total_orders,
                    SUM(total)       AS revenue,
                    currency
                FROM raw_orders
                GROUP BY country_code, order_date::date, currency
                ORDER BY date DESC, country_code
            """)
            return cur.fetchall()
