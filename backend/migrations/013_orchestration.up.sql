CREATE TABLE storage.orchestration_batch (
    batch_id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    domain text NOT NULL CHECK (
        domain IN ('reference','market','financial','dividend','industry')
    ),
    partition_key text NOT NULL,
    cursor_kind text NOT NULL CHECK (cursor_kind IN ('trade_date','report_period','observed_at')),
    cursor_value text NOT NULL,
    status text NOT NULL CHECK (
        status IN ('queued','running','succeeded','failed','cancelled','deferred')
    ),
    parameters jsonb NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_run_id uuid REFERENCES storage.pipeline_run(run_id),
    reason_code text,
    retryable boolean NOT NULL DEFAULT false,
    result jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (reason_code IS NULL OR reason_code ~ '^[a-z][a-z0-9_]{0,79}$')
);
CREATE INDEX orchestration_batch_status ON storage.orchestration_batch(status, updated_at);
CREATE INDEX orchestration_batch_partition
    ON storage.orchestration_batch(domain, partition_key, updated_at DESC);

CREATE TABLE storage.partition_watermark (
    domain text NOT NULL CHECK (
        domain IN ('reference','market','financial','dividend','industry')
    ),
    partition_key text NOT NULL,
    cursor_kind text NOT NULL CHECK (cursor_kind IN ('trade_date','report_period','observed_at')),
    cursor_value text NOT NULL,
    batch_id uuid NOT NULL REFERENCES storage.orchestration_batch(batch_id),
    run_id uuid REFERENCES storage.pipeline_run(run_id),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (domain, partition_key)
);
CREATE TABLE storage.orchestration_failure (
    failure_id uuid PRIMARY KEY,
    batch_id uuid NOT NULL REFERENCES storage.orchestration_batch(batch_id),
    attempt integer NOT NULL CHECK (attempt > 0),
    reason_code text NOT NULL CHECK (reason_code ~ '^[a-z][a-z0-9_]{0,79}$'),
    failure_kind text NOT NULL CHECK (failure_kind IN ('transient','permission','permanent')),
    retryable boolean NOT NULL,
    run_id uuid REFERENCES storage.pipeline_run(run_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (batch_id, attempt)
);
CREATE INDEX orchestration_failure_batch ON storage.orchestration_failure(batch_id, attempt DESC);

CREATE TABLE storage.orchestration_alert (
    alert_id uuid PRIMARY KEY,
    batch_id uuid NOT NULL REFERENCES storage.orchestration_batch(batch_id),
    kind text NOT NULL CHECK (kind IN ('permission_denied')),
    status text NOT NULL DEFAULT 'open' CHECK (status IN ('open','acknowledged','resolved')),
    detail jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (batch_id, kind)
);
