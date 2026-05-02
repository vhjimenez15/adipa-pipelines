"""
Registra los deployments en Prefect Server y sale.
Los workers (worker-light / worker-heavy) son quienes ejecutan los flows.
"""
import os
import asyncio
from prefect.deployments import Deployment
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.client.schemas.schedules import CronSchedule

from sync_orders import sync_orders
from daily_kpi_report import daily_kpi_report

LIGHT_SCHEDULE = os.environ.get("SYNC_ORDERS_SCHEDULE", "*/15 * * * *")
HEAVY_SCHEDULE = os.environ.get("KPI_REPORT_SCHEDULE", "0 5 * * *")
TIMEZONE       = os.environ.get("KPI_REPORT_TIMEZONE", "America/Bogota")
FLOWS_PATH     = "/app/flows"


async def ensure_work_pools():
    async with get_client() as client:
        for pool_name in ("light-pool", "heavy-pool"):
            try:
                await client.create_work_pool(
                    WorkPoolCreate(name=pool_name, type="process")
                )
                print(f"Created work pool: {pool_name}")
            except Exception:
                print(f"Work pool already exists: {pool_name}")


async def main():
    await ensure_work_pools()

    light = await Deployment.build_from_flow(
        flow=sync_orders,
        name="sync-orders-scheduled",
        work_pool_name="light-pool",
        schedules=[CronSchedule(cron=LIGHT_SCHEDULE, timezone="UTC")],
        path=FLOWS_PATH,
        entrypoint="sync_orders.py:sync_orders",
    )
    await light.apply()
    print("Registered: sync-orders-scheduled")

    heavy = await Deployment.build_from_flow(
        flow=daily_kpi_report,
        name="daily-kpi-report-scheduled",
        work_pool_name="heavy-pool",
        schedules=[CronSchedule(cron=HEAVY_SCHEDULE, timezone=TIMEZONE)],
        path=FLOWS_PATH,
        entrypoint="daily_kpi_report.py:daily_kpi_report",
    )
    await heavy.apply()
    print("Registered: daily-kpi-report-scheduled")

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
