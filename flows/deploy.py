"""
Registra los dos deployments en Prefect Server con sus schedules.
Ejecutar una sola vez al levantar el stack: lo corre el contenedor `deploy`.
"""
import os
import asyncio
from prefect import serve
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.client.schemas.schedules import CronSchedule

from sync_orders import sync_orders
from daily_kpi_report import daily_kpi_report

LIGHT_SCHEDULE = os.environ.get("SYNC_ORDERS_SCHEDULE", "*/15 * * * *")
HEAVY_SCHEDULE = os.environ.get("KPI_REPORT_SCHEDULE", "0 5 * * *")
TIMEZONE = os.environ.get("KPI_REPORT_TIMEZONE", "America/Bogota")


async def ensure_work_pools():
    """Crea los work pools si no existen — idempotente."""
    async with get_client() as client:
        for pool_name in ("light-pool", "heavy-pool"):
            try:
                await client.create_work_pool(
                    WorkPoolCreate(name=pool_name, type="process")
                )
                print(f"Created work pool: {pool_name}")
            except Exception:
                print(f"Work pool already exists: {pool_name}")


if __name__ == "__main__":
    asyncio.run(ensure_work_pools())

    light_deployment = sync_orders.to_deployment(
        name="sync-orders-scheduled",
        work_pool_name="light-pool",
        schedule=CronSchedule(cron=LIGHT_SCHEDULE, timezone="UTC"),
    )

    heavy_deployment = daily_kpi_report.to_deployment(
        name="daily-kpi-report-scheduled",
        work_pool_name="heavy-pool",
        schedule=CronSchedule(cron=HEAVY_SCHEDULE, timezone=TIMEZONE),
    )

    serve(light_deployment, heavy_deployment)
