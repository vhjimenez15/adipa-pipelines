from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query
from app.auth import get_current_user
from app.db import get_conn

router = APIRouter(prefix="/kpis", tags=["KPIs"])


@router.get("/", summary="List KPI daily reports")
def list_kpis(
    country: Optional[str] = Query(None, description="Filter by country: CL, MX, CO"),
    from_date: Optional[date] = Query(None, description="From date (YYYY-MM-DD)"),
    to_date: Optional[date] = Query(None, description="To date (YYYY-MM-DD)"),
    limit: int = Query(30, le=365),
    _: str = Depends(get_current_user),
):
    filters = []
    params = []

    if country:
        filters.append("country_code = %s")
        params.append(country.upper())
    if from_date:
        filters.append("report_date >= %s")
        params.append(from_date)
    if to_date:
        filters.append("report_date <= %s")
        params.append(to_date)

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM kpi_daily_report {where} ORDER BY report_date DESC, country_code LIMIT %s",
                params,
            )
            return cur.fetchall()


@router.get("/latest", summary="Latest KPI report per country")
def latest_kpis(_: str = Depends(get_current_user)):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (country_code) *
                FROM kpi_daily_report
                ORDER BY country_code, report_date DESC
            """)
            return cur.fetchall()
