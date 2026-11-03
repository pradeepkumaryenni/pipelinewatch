-- PipelineWatch Postgres schema. Applied by PostgresStore.init_schema(); safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS pipelinewatch;

-- Every alert received, including suppressed duplicates.
CREATE TABLE IF NOT EXISTS pipelinewatch.alert_log (
    alert_id       BIGSERIAL PRIMARY KEY,
    event_id       TEXT        NOT NULL UNIQUE,      -- idempotency key derived from the source event
    fingerprint    TEXT        NOT NULL,             -- "same problem" key used for de-duplication
    received_at    TIMESTAMPTZ NOT NULL,
    occurred_at    TIMESTAMPTZ,
    source         TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('FIRING', 'RESOLVED')),
    severity       TEXT        NOT NULL CHECK (severity IN ('CRITICAL', 'HIGH', 'LOW', 'INFO')),
    pipeline       TEXT        NOT NULL,
    environment    TEXT        NOT NULL,
    decision       TEXT        NOT NULL CHECK (decision IN ('PAGE', 'NOTIFY', 'DIGEST', 'SUPPRESS', 'RESOLVE', 'LOG_ONLY')),
    occurrence     INT         NOT NULL,
    severity_rule  TEXT,
    summary        TEXT,
    alert          JSONB       NOT NULL               -- full classified alert, including the raw payload
);

CREATE INDEX IF NOT EXISTS alert_log_fingerprint_idx ON pipelinewatch.alert_log (fingerprint, received_at DESC);
CREATE INDEX IF NOT EXISTS alert_log_received_idx ON pipelinewatch.alert_log (received_at DESC);

-- One row per fingerprint with an open notification window.
CREATE TABLE IF NOT EXISTS pipelinewatch.dedup_state (
    fingerprint        TEXT        PRIMARY KEY,
    window_started_at  TIMESTAMPTZ NOT NULL,
    last_seen_at       TIMESTAMPTZ NOT NULL,
    occurrences        INT         NOT NULL,
    max_severity       TEXT        NOT NULL,
    opened_by_alert_id BIGINT
);

-- One row per finished notification attempt (sent, or failed after retries).
CREATE TABLE IF NOT EXISTS pipelinewatch.delivery_log (
    delivery_id   BIGSERIAL PRIMARY KEY,
    alert_id      BIGINT      NOT NULL REFERENCES pipelinewatch.alert_log (alert_id),
    channel       TEXT        NOT NULL CHECK (channel IN ('slack', 'teams', 'telegram')),
    status        TEXT        NOT NULL CHECK (status IN ('SENT', 'FAILED')),
    detail        TEXT,
    delivered_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS delivery_log_alert_idx ON pipelinewatch.delivery_log (alert_id);
