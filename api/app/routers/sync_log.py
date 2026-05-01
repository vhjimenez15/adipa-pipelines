from typing import Optional
from fastapi import APIRouter, Depends, Query
from app.auth import get_current_user
from app.db import get_conn

router = APIRouter(prefix="/sync-log", tags=["Sync Log"])


@router.get("/", summary="Recent pipeline execution log")
def list_sync_log(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline: sync_orders, daily_kpi_report"),
    status: Optional[str] = Query(None, description="Filter by status: success, error"),
    limit: int = Query(20, le=200),
    _: str = Depends(get_current_user),
):
    filters = []
    params = []

    if pipeline:
        filters.append("pipeline = %s")
        params.append(pipeline)
    if status:
        filters.append("status = %s")
        params.append(status)

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM sync_log {where} ORDER BY finished_at DESC LIMIT %s",
                params,
            )
            return cur.fetchall()
