# ADIPA — Pipeline de Monitoreo de Ventas Multi-País

## El Problema

ADIPA opera WooCommerce en Chile, México y Colombia. Las monedas son distintas (CLP/MXN/COP), los precios cambian, y hoy no hay visibilidad consolidada de qué está vendiendo cada tienda ni cómo evoluciona día a día.

**Solución**: dos pipelines encadenados — uno que recolecta órdenes cada 15 minutos y otro que cada madrugada procesa lo acumulado y genera KPIs normalizados a USD por país.

---

## Arquitectura

```
                    ┌─────────────────────┐
  Internet ──:80──▶ │  nginx (Basic Auth) │
                    └────────┬────────────┘
                             │ proxy_pass (interno)
                    ┌────────▼────────────┐    ┌──────────────────────┐
                    │   prefect-server    │    │      PostgreSQL       │
                    │   (UI + API :4200)  │    │  · raw_orders        │
                    └──────┬──────────────┘    │  · kpi_daily_report  │
                           │                   │  · sync_log          │
              ┌────────────┴──────────┐        └──────────────────────┘
              ▼                       ▼
   ┌──────────────────┐    ┌──────────────────┐
   │  worker-light    │    │  worker-heavy    │
   │  sync_orders     │    │  daily_kpi_      │
   │  cada 15 min     │    │  report 00:00    │
   │  deps: mínimas   │    │  Bogotá + pandas │
   └──────────────────┘    └──────────────────┘
```

### Encadenamiento
`sync_orders` popula `raw_orders` → `daily_kpi_report` la consume al día siguiente. El pesado nunca toca la API externa directamente para órdenes, solo lee lo que el liviano ya guardó.

### Aislamiento del pipeline pesado
`pandas` vive únicamente en `worker-heavy`. El contenedor del orquestador (`prefect-server`) y el worker liviano no tienen dependencias pesadas. Esto evita que una librería de procesamiento de datos contamine el ambiente del orquestador.

---

## Por qué Prefect y no Airflow

| | Prefect 2.x | Airflow 2.x |
|---|---|---|
| Servicios mínimos | 2 (server + worker) | 4+ (webserver, scheduler, worker, meta-DB) |
| Config para workers aislados | Work Pools nativo | CeleryExecutor + Redis |
| DX Python | Flow = función Python pura | DAG con decoradores propios |
| Setup en VM free tier | Liviano | Pesado para CX11 |

Para una VM Hetzner CX11 (2 vCPU, 2 GB RAM) Prefect es la elección correcta.

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
# docker compose -f docker-compose.dev.yml up --build

# 4. Abrir la UI de Prefect
open http://localhost:4200
```

### Verificar que funciona

```bash
# Disparar el pipeline liviano manualmente
make run-light

# Disparar el pipeline pesado manualmente
make run-heavy

# Ver datos en Postgres
psql -h localhost -U adipa -d adipa_pipelines -c "SELECT * FROM raw_orders LIMIT 10;"
psql -h localhost -U adipa -d adipa_pipelines -c "SELECT * FROM kpi_daily_report;"
psql -h localhost -U adipa -d adipa_pipelines -c "SELECT * FROM sync_log ORDER BY finished_at DESC LIMIT 5;"
```

---

## CI/CD (GitHub Actions)

Dos workflows automáticos:

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
| `HETZNER_SSH_KEY` | Clave privada SSH (el par de la que está en `~/.ssh/authorized_keys` en la VM) |
| `DEPLOY_PATH` | Ruta absoluta del repo en la VM (ej: `/root/adipa-pipelines`) |

### Setup inicial en la VM (una sola vez)

```bash
# Clonar el repo
git clone https://github.com/<tu-usuario>/adipa-pipelines.git /root/adipa-pipelines
cd /root/adipa-pipelines

# Crear .env.prod con los valores reales
cp .env.prod .env.prod.bak  # el del repo tiene los placeholders
nano .env.prod  # editar POSTGRES_PASSWORD, NGINX_PASSWORD, HETZNER_IP

# Primer despliegue manual
docker compose -f docker-compose.prod.yml up -d --build
```

Después de este setup, cada push a `main` despliega automáticamente.

---

## Despliegue en VM (Hetzner)

```bash
# En la VM
git clone <repo>
cd adipa-pipelines

# Editar .env.prod: cambiar POSTGRES_PASSWORD, NGINX_PASSWORD y YOUR_HETZNER_IP
nano .env.prod

# Levantar en producción
docker compose -f docker-compose.prod.yml up -d --build

# UI disponible en (con usuario/contraseña definidos en .env.prod):
# http://<IP_HETZNER>
# user: adipa  |  password: el que pusiste en NGINX_PASSWORD
```

---

## Idempotencia

Ambos pipelines usan `ON CONFLICT ... DO UPDATE` (UPSERT). Re-ejecutar varias veces el mismo período no duplica filas ni rompe datos. Los campos de control (`synced_at`, `updated_at`) se actualizan pero los datos de negocio solo se sobreescriben con valores equivalentes.

---

## Variables de entorno clave

| Variable | Default dev | Descripción |
|---|---|---|
| `SYNC_ORDERS_SCHEDULE` | `*/15 * * * *` | Cron del pipeline liviano |
| `KPI_REPORT_SCHEDULE` | `0 5 * * *` | Cron del pesado (en UTC) |
| `KPI_REPORT_TIMEZONE` | `America/Bogota` | Timezone para interpretar el cron del pesado |
| `MOCK_ORDERS_PER_RUN` | `5` | Órdenes simuladas por país por ejecución |
| `WOOCOMMERCE_COUNTRIES` | `CL,MX,CO` | Países activos |
| `EXCHANGE_RATE_API_URL` | `https://api.frankfurter.app` | API de tipos de cambio |
| `NGINX_USER` | `adipa` | Usuario para acceder a la UI de Prefect |
| `NGINX_PASSWORD` | `adipa2024` | Contraseña para acceder a la UI de Prefect |

---

## Qué haría en una segunda iteración

1. **Swap mock → WooCommerce real**: cambiar `woocommerce_mock.py` por llamadas reales a `/wp-json/wc/v3/orders` con `consumer_key`/`consumer_secret`. La estructura del dict ya es compatible.
2. **Alertas**: si `vs_prev_day_pct` cae más de 30%, enviar notificación a Slack o email.
3. **BigQuery sink**: exportar `kpi_daily_report` a BigQuery para análisis histórico con Looker Studio.
4. **Tests de integración**: levantar Postgres en CI y correr los flows contra una DB real.
5. **Secrets manager**: mover contraseñas a Prefect Secrets o AWS SSM en lugar de `.env.prod`.
