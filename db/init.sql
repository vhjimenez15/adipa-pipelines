-- Idempotent: safe to run multiple times
CREATE TABLE IF NOT EXISTS raw_orders (
    id              SERIAL PRIMARY KEY,
    order_id        VARCHAR(64)     NOT NULL,
    country_code    CHAR(2)         NOT NULL,  -- CL, MX, CO
    status          VARCHAR(32)     NOT NULL,
    currency        CHAR(3)         NOT NULL,
    total           NUMERIC(12, 2)  NOT NULL,
    product_name    TEXT            NOT NULL,
    quantity        INTEGER         NOT NULL DEFAULT 1,
    customer_email  TEXT,
    order_date      TIMESTAMPTZ     NOT NULL,
    synced_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_order_country UNIQUE (order_id, country_code)
);

CREATE TABLE IF NOT EXISTS kpi_daily_report (
    id              SERIAL PRIMARY KEY,
    report_date     DATE            NOT NULL,
    country_code    CHAR(2)         NOT NULL,
    total_orders    INTEGER         NOT NULL DEFAULT 0,
    revenue_local   NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    currency        CHAR(3)         NOT NULL,
    revenue_usd     NUMERIC(14, 4)  NOT NULL DEFAULT 0,
    exchange_rate   NUMERIC(10, 6)  NOT NULL DEFAULT 1,
    top_courses     JSONB,          -- [{name, units_sold}]
    vs_prev_day_pct NUMERIC(8, 2),  -- % change revenue_usd vs previous day
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_report_day_country UNIQUE (report_date, country_code)
);

-- Audit trail for every sync run
CREATE TABLE IF NOT EXISTS sync_log (
    id              SERIAL PRIMARY KEY,
    pipeline        VARCHAR(64)     NOT NULL,  -- 'sync_orders' | 'daily_kpi_report'
    country_code    CHAR(2),
    status          VARCHAR(16)     NOT NULL,  -- 'success' | 'error'
    rows_affected   INTEGER         NOT NULL DEFAULT 0,
    error_msg       TEXT,
    started_at      TIMESTAMPTZ     NOT NULL,
    finished_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_orders_date ON raw_orders (order_date);
CREATE INDEX IF NOT EXISTS idx_raw_orders_country ON raw_orders (country_code);
CREATE INDEX IF NOT EXISTS idx_kpi_report_date ON kpi_daily_report (report_date);
