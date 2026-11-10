-- Warehouse layers managed by the Python part of the pipeline.
-- dbt builds the `intermediate` and `marts` schemas on top of `staging`.
-- Every statement is idempotent, so this runs safely at the start of each DAG run.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS quality;

-- Landing zone: events exactly as received, one set of rows per hourly batch.
CREATE TABLE IF NOT EXISTS raw.order_events (
    raw_id      BIGSERIAL PRIMARY KEY,
    batch_start TIMESTAMPTZ NOT NULL,
    payload     JSONB       NOT NULL,
    landed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_order_events_batch ON raw.order_events (batch_start);

-- Validated, typed and de-duplicated events (one row per event_id).
CREATE TABLE IF NOT EXISTS staging.order_events (
    event_id       TEXT PRIMARY KEY,
    order_id       TEXT          NOT NULL,
    customer_id    BIGINT        NOT NULL,
    status         TEXT          NOT NULL,
    amount         NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    currency       CHAR(3)       NOT NULL,
    country        CHAR(2),
    payment_method TEXT,
    event_ts       TIMESTAMPTZ   NOT NULL,
    batch_start    TIMESTAMPTZ   NOT NULL,
    loaded_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_order_events_batch ON staging.order_events (batch_start);
CREATE INDEX IF NOT EXISTS ix_stg_order_events_order ON staging.order_events (order_id);
CREATE INDEX IF NOT EXISTS ix_stg_order_events_loaded ON staging.order_events (loaded_at);

-- Current state of every order, kept up to date with upserts.
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id       TEXT PRIMARY KEY,
    customer_id    BIGINT        NOT NULL,
    current_status TEXT          NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,
    currency       CHAR(3)       NOT NULL,
    country        CHAR(2),
    payment_method TEXT,
    last_event_ts  TIMESTAMPTZ   NOT NULL,
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Quarantine: events that failed validation, with the reasons, for investigation and replay.
CREATE TABLE IF NOT EXISTS quality.rejected_events (
    rejected_id BIGSERIAL PRIMARY KEY,
    batch_start TIMESTAMPTZ NOT NULL,
    event_id    TEXT,
    payload     JSONB       NOT NULL,
    reasons     TEXT[]      NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rejected_events_batch ON quality.rejected_events (batch_start);

-- One row per batch: volumes in, accepted, rejected and de-duplicated.
CREATE TABLE IF NOT EXISTS quality.batch_metrics (
    batch_start      TIMESTAMPTZ PRIMARY KEY,
    batch_end        TIMESTAMPTZ NOT NULL,
    total_events     INTEGER     NOT NULL,
    valid_events     INTEGER     NOT NULL,
    rejected_events  INTEGER     NOT NULL,
    duplicate_events INTEGER     NOT NULL,
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Result of the statistical volume check for each batch.
CREATE TABLE IF NOT EXISTS quality.volume_checks (
    batch_start   TIMESTAMPTZ PRIMARY KEY,
    observed      INTEGER     NOT NULL,
    baseline_mean NUMERIC,
    baseline_std  NUMERIC,
    z_score       NUMERIC,
    is_anomaly    BOOLEAN     NOT NULL,
    message       TEXT        NOT NULL,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
