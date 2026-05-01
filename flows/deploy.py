"""
Registra los dos deployments en Prefect Server con sus schedules.
Ejecutar una sola vez al levantar el stack: `python deploy.py`
"""
import os
from prefect import serve
from prefect.client.schemas.schedules import CronSchedule

from sync_orders import sync_orders
from daily_kpi_report import daily_kpi_report

LIGHT_SCHEDULE = os.environ.get("SYNC_ORDERS_SCHEDULE", "*/15 * * * *")
HEAVY_SCHEDULE = os.environ.get("KPI_REPORT_SCHEDULE", "0 5 * * *")
TIMEZONE = os.environ.get("KPI_REPORT_TIMEZONE", "America/Bogota")

if __name__ == "__main__":
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

    # serve() registra y sirve ambos deployments en el mismo proceso
    serve(light_deployment, heavy_deployment)
