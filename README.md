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

## API REST + Swagger

La API corre en FastAPI con autenticación JWT. Todos los endpoints excepto `/auth/login` requieren `Authorization: Bearer <token>`.

**URLs en local:**
- Swagger UI: `http://localhost/api/docs`
- API directa: `http://localhost:8000`

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
1. Ir a `http://localhost/api/docs`
2. Click en **Authorize** (botón con candado)
3. Ingresar usuario y contraseña (`API_USER` / `API_PASSWORD` del `.env.dev`)
4. Todas las peticiones del Swagger usarán el token automáticamente

---

## Levantar en local (dev)

```bash
# 1. Clonar y entrar
git clone <repo>
cd adipa-pipelines

# 2. Revisar variables (ya vienen con valores para dev)
cat .env.dev

# 3. Levantar todo
make dev
# o directamente:
# docker compose -f docker-compose.dev.yml --env-file .env.dev up --build

# 4. URLs disponibles
# Prefect UI:  http://localhost  (user: adipa / adipa2024)
# Swagger:     http://localhost/api/docs
# API directa: http://localhost:8000
```

### Verificar que funciona

```bash
# Disparar los pipelines manualmente
make run-light
make run-heavy

# Ver datos en Postgres (puerto 5433 en dev — 5432 está ocupado por postgres local)
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

# Editar .env.prod: POSTGRES_PASSWORD, NGINX_PASSWORD, API_PASSWORD, JWT_SECRET, IP del servidor
nano .env.prod

# Levantar
docker compose -f docker-compose.prod.yml --env-file .env.prod up -d --build
```

Después de esto, cada push a `main` despliega automáticamente vía CI/CD.

### URLs en producción

```
http://<IP_HETZNER>          → Prefect UI  (user/pass: NGINX_USER / NGINX_PASSWORD)
http://<IP_HETZNER>/api/docs → Swagger     (user/pass: API_USER / API_PASSWORD)
```

---

## Idempotencia

Ambos pipelines usan `ON CONFLICT ... DO UPDATE` (UPSERT). Re-ejecutar varias veces el mismo período no duplica filas ni rompe datos.

---

## Variables de entorno clave

| Variable | Default dev | Descripción |
|---|---|---|
| `POSTGRES_USER` | `adipa` | Usuario de PostgreSQL |
| `POSTGRES_PASSWORD` | `adipa_dev_pass` | Contraseña de PostgreSQL |
| `SYNC_ORDERS_SCHEDULE` | `*/15 * * * *` | Cron del pipeline liviano |
| `KPI_REPORT_SCHEDULE` | `0 5 * * *` | Cron del pesado (UTC — equivale a 00:00 Bogotá) |
| `KPI_REPORT_TIMEZONE` | `America/Bogota` | Timezone del pipeline pesado |
| `MOCK_ORDERS_PER_RUN` | `5` | Órdenes simuladas por país por ejecución |
| `WOOCOMMERCE_COUNTRIES` | `CL,MX,CO` | Países activos |
| `EXCHANGE_RATE_API_URL` | `https://open.er-api.com` | API de tipos de cambio (soporta CLP, MXN, COP) |
| `NGINX_USER` | `adipa` | Usuario Basic Auth para Prefect UI |
| `NGINX_PASSWORD` | `adipa2024` | Contraseña Basic Auth para Prefect UI |
| `API_USER` | `adipa` | Usuario para login en la API REST |
| `API_PASSWORD` | `adipa2024` | Contraseña para login en la API REST |
| `JWT_SECRET` | `dev_secret_...` | Secreto para firmar tokens JWT (cambiar en prod) |
| `JWT_EXPIRE_HOURS` | `24` | Duración del token JWT en horas |

---

## Qué haría en una segunda iteración

1. **Swap mock → WooCommerce real**: cambiar `woocommerce_mock.py` por llamadas reales a `/wp-json/wc/v3/orders`. La estructura del dict ya es compatible.
2. **Alertas**: si `vs_prev_day_pct` cae más de 30%, enviar notificación a Slack.
3. **BigQuery sink**: exportar `kpi_daily_report` a BigQuery para análisis histórico con Looker Studio.
4. **Tests de integración**: levantar Postgres en CI y correr los flows contra una DB real.
5. **Secrets manager**: mover contraseñas a Prefect Secrets o AWS SSM en lugar de `.env.prod`.
