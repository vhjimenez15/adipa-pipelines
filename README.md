# ADIPA — Pipeline de Monitoreo de Ventas Multi-País

## El Problema

ADIPA opera WooCommerce en Chile, México y Colombia. Las monedas son distintas (CLP/MXN/COP), los precios cambian, y hoy no hay visibilidad consolidada de qué está vendiendo cada tienda ni cómo evoluciona día a día.

**Solución**: dos pipelines encadenados — uno que recolecta órdenes cada 15 minutos y otro que cada madrugada procesa lo acumulado y genera KPIs normalizados a USD por país.

---

## Arquitectura

```
                    ┌──────────────────────────────┐
  Internet ──:80──▶ │         nginx                │
                    │  /        → Prefect UI (auth) │
                    │  /api/    → FastAPI (JWT)      │
                    └──────┬───────────┬────────────┘
                           │           │
               ┌───────────▼──┐   ┌───▼──────────┐   ┌──────────────────────┐
               │prefect-server│   │   api (8000) │   │      PostgreSQL       │
               │  UI :4200    │   │  FastAPI +   │   │  · raw_orders         │
               └──────┬───────┘   │  Swagger     │   │  · kpi_daily_report   │
                      │           └──────────────┘   │  · sync_log           │
          ┌───────────┴──────────┐                   └──────────────────────┘
          ▼                      ▼
┌──────────────────┐   ┌──────────────────┐
│  worker-light    │   │  worker-heavy    │
│  sync_orders     │   │  daily_kpi_      │
│  cada 15 min     │   │  report 00:00    │
│  deps: mínimas   │   │  Bogotá + pandas │
└──────────────────┘   └──────────────────┘
```

### Servicios

| Contenedor | Rol |
|---|---|
| `postgres` | Base de datos — almacena órdenes, KPIs y log de ejecuciones |
| `prefect-server` | Orquestador — UI + API interna para workers |
| `nginx` | Reverse proxy — Basic Auth para Prefect UI, sin auth para `/api` |
| `api` | FastAPI — endpoints REST + Swagger, autenticación JWT |
| `worker-light` | Ejecuta `sync_orders` cada 15 min (deps mínimas) |
| `worker-heavy` | Ejecuta `daily_kpi_report` a 00:00 Bogotá (tiene Pandas) |
| `deploy` | Init container — registra los schedules en Prefect y sale |

> **`deploy`**: arranca una sola vez, ejecuta `deploy.py` que registra ambos pipelines con sus crons en Prefect Server, y termina (exit 0). Sin él, Prefect no sabe que los flows existen ni cuándo ejecutarlos. El CD lo re-corre en cada despliegue para aplicar cambios de schedule.

### Encadenamiento
`sync_orders` popula `raw_orders` → `daily_kpi_report` la consume al día siguiente.

### Aislamiento del pipeline pesado
`pandas` vive únicamente en `worker-heavy`. El orquestador y el worker liviano no tienen dependencias pesadas.

---

## Por qué Prefect y no Airflow

| | Prefect 2.x | Airflow 2.x |
|---|---|---|
| Servicios mínimos | 2 (server + worker) | 4+ (webserver, scheduler, worker, meta-DB) |
| Workers aislados | Work Pools nativo | CeleryExecutor + Redis |
| DX Python | Flow = función Python pura | DAG con decoradores propios |
| Setup en VM Hetzner CX11 | Liviano | Demasiado pesado |

---

## Variables de entorno

### `.env.dev` — desarrollo local

```env
# ── Postgres ──────────────────────────────────────────────────────────────────
POSTGRES_USER=adipa
POSTGRES_PASSWORD=adipa_dev_pass
POSTGRES_DB=adipa_pipelines
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# ── Prefect ───────────────────────────────────────────────────────────────────
PREFECT_API_URL=http://prefect-server:4200/api
PREFECT_UI_API_URL=http://localhost:4200/api

# ── Pipeline: sync_orders (liviano) ──────────────────────────────────────────
SYNC_ORDERS_SCHEDULE=*/15 * * * *
WOOCOMMERCE_COUNTRIES=CL,MX,CO
MOCK_ORDERS_PER_RUN=5

# ── Pipeline: daily_kpi_report (pesado) ───────────────────────────────────────
# Cron en UTC. 00:00 America/Bogotá = 05:00 UTC
KPI_REPORT_SCHEDULE=0 5 * * *
KPI_REPORT_TIMEZONE=America/Bogota
EXCHANGE_RATE_API_URL=https://open.er-api.com

# ── FastAPI / JWT ─────────────────────────────────────────────────────────────
API_USER=adipa
API_PASSWORD=adipa2026
JWT_SECRET=dev_secret_change_in_prod_min_32_chars_xx
JWT_EXPIRE_HOURS=24

# ── Nginx Basic Auth (Prefect UI) ─────────────────────────────────────────────
NGINX_USER=adipa
NGINX_PASSWORD=adipa2026

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL=DEBUG
```

### `.env.prod` — producción (Hetzner)

```env
# ── Postgres ──────────────────────────────────────────────────────────────────
POSTGRES_USER=adipa
POSTGRES_PASSWORD=CHANGE_ME_STRONG_PASSWORD
POSTGRES_DB=adipa_pipelines
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# ── Prefect ───────────────────────────────────────────────────────────────────
PREFECT_API_URL=http://YOUR_HETZNER_IP:4200/api
PREFECT_UI_API_URL=http://YOUR_HETZNER_IP:4200/api

# ── Pipeline: sync_orders (liviano) ──────────────────────────────────────────
SYNC_ORDERS_SCHEDULE=*/15 * * * *
WOOCOMMERCE_COUNTRIES=CL,MX,CO
MOCK_ORDERS_PER_RUN=10

# ── Pipeline: daily_kpi_report (pesado) ───────────────────────────────────────
KPI_REPORT_SCHEDULE=0 5 * * *
KPI_REPORT_TIMEZONE=America/Bogota
EXCHANGE_RATE_API_URL=https://open.er-api.com

# ── FastAPI / JWT ─────────────────────────────────────────────────────────────
API_USER=adipa
API_PASSWORD=CHANGE_ME_STRONG_PASSWORD
JWT_SECRET=CHANGE_ME_MIN_32_CHARS_RANDOM_STRING
JWT_EXPIRE_HOURS=24

# ── Nginx Basic Auth (Prefect UI) ─────────────────────────────────────────────
NGINX_USER=adipa
NGINX_PASSWORD=CHANGE_ME_STRONG_PASSWORD

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL=INFO
```

---

## API REST + Swagger

La API corre en FastAPI con autenticación JWT. Todos los endpoints excepto `/auth/login` requieren `Authorization: Bearer <token>`.

**URLs en local:**
- Swagger UI: `http://localhost:8080/api/docs`
- API directa: `http://localhost:8000`

**Credenciales dev:** `adipa` / `adipa2026`

**Endpoints disponibles:**

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/api/auth/login` | Login — devuelve Bearer token |
| `GET` | `/api/orders/` | Órdenes con filtros (country, date, status, limit) |
| `GET` | `/api/orders/summary` | Órdenes agrupadas por país y fecha |
| `GET` | `/api/kpis/` | Reportes KPI con filtros (country, from_date, to_date) |
| `GET` | `/api/kpis/latest` | Último reporte por país |
| `GET` | `/api/sync-log/` | Log de ejecuciones de los pipelines |

**Cómo usar el Swagger:**
1. Ir a `http://localhost:8080/api/docs`
2. Click en **Authorize** (botón con candado)
3. Ingresar `adipa` / `adipa2026`
4. Todas las peticiones del Swagger usarán el token automáticamente

---

## Ver datos recolectados en Prefect UI

Cada flow run publica un **Artifact** con el resumen de lo que procesó. Para verlo:

1. Ir a `http://localhost:8080` (user: `adipa` / `adipa2026`)
2. Click en **Flow Runs** → entrar a cualquier run completado
3. Pestaña **Artifacts** — aparece la tabla con los datos del run

También en `http://localhost:8080/artifacts` se ven todos los artifacts históricos.

---

## Guía rápida — ejecutar y revisar los jobs

### 1. Levantar el stack

```bash
make dev
# Esperar ~20s hasta que todos los contenedores estén healthy
```

### 2. Disparar los pipelines manualmente

```bash
# Pipeline liviano: simula órdenes de WooCommerce y las guarda en raw_orders
make run-light

# Pipeline pesado: lee raw_orders de ayer, calcula KPIs en USD y los guarda en kpi_daily_report
make run-heavy
```

> Los pipelines también corren solos según sus crons (cada 15 min el liviano, 00:00 Bogotá el pesado).

### 3. Ver los jobs en Prefect UI

1. Ir a **http://localhost:8080** → usuario `adipa` / contraseña `adipa2026`
2. En el menú izquierdo: **Flow Runs** — aparecen todas las ejecuciones con estado (Completed / Failed)
3. Click en cualquier run → pestaña **Artifacts** → tabla con el resumen de lo que procesó ese run
4. Para ver los schedules registrados: menú **Deployments**

### 4. Ver los KPIs via API (Swagger)

1. Ir a **http://localhost:8080/api/docs**
2. Click en **Authorize** → ingresar `adipa` / `adipa2026`
3. Endpoints útiles:
   - `GET /api/kpis/latest` — último reporte por país
   - `GET /api/kpis/` — histórico con filtros de fecha y país
   - `GET /api/orders/summary` — órdenes agrupadas por país y fecha
   - `GET /api/sync-log/` — log de ejecuciones (cuándo corrió cada pipeline y cuántas filas afectó)

### 5. Ver los datos directo en Postgres

```bash
# Órdenes recolectadas
psql -h localhost -p 5433 -U adipa -d adipa_pipelines \
  -c "SELECT country_code, status, product_name, total, currency FROM raw_orders ORDER BY synced_at DESC LIMIT 10;"

# KPIs por país
psql -h localhost -p 5433 -U adipa -d adipa_pipelines \
  -c "SELECT report_date, country_code, total_orders, revenue_local, currency, revenue_usd, vs_prev_day_pct FROM kpi_daily_report ORDER BY report_date DESC;"

# Log de ejecuciones
psql -h localhost -p 5433 -U adipa -d adipa_pipelines \
  -c "SELECT pipeline, status, rows_affected, started_at FROM sync_log ORDER BY started_at DESC LIMIT 10;"
```

---

## Levantar en local (dev)

```bash
# 1. Clonar y entrar
git clone <repo>
cd adipa-pipelines

# 2. Levantar todo
make dev
# o directamente:
docker compose -f docker-compose.dev.yml --env-file .env.dev up --build

# 3. URLs disponibles
# Prefect UI:  http://localhost:8080          (adipa / adipa2026)
# Swagger:     http://localhost:8080/api/docs (adipa / adipa2026)
# API directa: http://localhost:8000
```

### Verificar que funciona

```bash
# Disparar los pipelines manualmente
make run-light
make run-heavy

# Ver datos en Postgres (puerto 5433 en dev)
psql -h localhost -p 5433 -U adipa -d adipa_pipelines -c "SELECT * FROM raw_orders LIMIT 10;"
psql -h localhost -p 5433 -U adipa -d adipa_pipelines -c "SELECT * FROM kpi_daily_report;"
psql -h localhost -p 5433 -U adipa -d adipa_pipelines -c "SELECT * FROM sync_log ORDER BY finished_at DESC LIMIT 5;"

# O desde dentro del contenedor
docker compose -f docker-compose.dev.yml --env-file .env.dev exec postgres \
  psql -U adipa -d adipa_pipelines -c "SELECT * FROM kpi_daily_report;"
```

---

## CI/CD (GitHub Actions)

| Workflow | Trigger | Qué hace |
|---|---|---|
| `ci.yml` | Push a cualquier rama / PR a main | Lint con ruff + valida los docker-compose |
| `cd.yml` | Push a `main` | SSH a Hetzner → git pull → rebuild → re-registra deployments |

### Secrets requeridos en GitHub

Ir a **Settings → Secrets and variables → Actions** y agregar:

| Secret | Valor |
|---|---|
| `HETZNER_HOST` | IP pública del servidor |
| `HETZNER_USER` | Usuario SSH (ej: `root`) |
| `HETZNER_SSH_KEY` | Clave privada SSH |
| `DEPLOY_PATH` | Ruta absoluta del repo en la VM (ej: `/root/adipa-pipelines`) |

---

## Despliegue en VM (Hetzner)

### Setup inicial (una sola vez)

```bash
# En la VM
git clone https://github.com/<tu-usuario>/adipa-pipelines.git
cd adipa-pipelines

# Editar .env.prod con los valores reales
nano .env.prod

# Levantar
docker compose -f docker-compose.prod.yml --env-file .env.prod up -d --build
```

Después de esto, cada push a `main` despliega automáticamente vía CI/CD.

### URLs en producción

```
http://<IP_HETZNER>          → Prefect UI  (NGINX_USER / NGINX_PASSWORD)
http://<IP_HETZNER>/api/docs → Swagger     (API_USER / API_PASSWORD)
```

---

## Idempotencia

Ambos pipelines usan `ON CONFLICT ... DO UPDATE` (UPSERT). Re-ejecutar varias veces el mismo período no duplica filas ni rompe datos.

---

## Qué haría en una segunda iteración

1. **Swap mock → WooCommerce real**: cambiar `woocommerce_mock.py` por llamadas reales a `/wp-json/wc/v3/orders`. La estructura del dict ya es compatible.
2. **Alertas**: si `vs_prev_day_pct` cae más de 30%, enviar notificación a Slack.
3. **BigQuery sink**: exportar `kpi_daily_report` a BigQuery para análisis histórico con Looker Studio.
4. **Tests de integración**: levantar Postgres en CI y correr los flows contra una DB real.
5. **Secrets manager**: mover contraseñas a Prefect Secrets o AWS SSM en lugar de `.env.prod`.
