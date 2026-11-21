CREATE TABLE storage.research_task (
    task_id uuid PRIMARY KEY,
    request_id text NOT NULL CHECK (request_id ~ '^[0-9a-f]{32}$'),
    owner_id text NOT NULL CHECK (length(owner_id) BETWEEN 1 AND 64),
    idempotency_key text NOT NULL CHECK (idempotency_key ~ '^[a-zA-Z0-9_.:-]{1,128}$'),
    payload_hash text NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    question text NOT NULL CHECK (length(question) BETWEEN 1 AND 4000),
    as_of timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued','running','succeeded','partial','failed','cancelled')),
    phase text NOT NULL DEFAULT 'queued'
        CHECK (phase IN ('queued','plan','execute','reflect','generate','verify','completed')),
    cancel_requested boolean NOT NULL DEFAULT false,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner text CHECK (lease_owner IS NULL OR length(lease_owner) BETWEEN 1 AND 128),
    lease_until timestamptz,
    plan jsonb,
    security jsonb,
    execution jsonb,
    report jsonb,
    verification jsonb,
    failure_code text CHECK (failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9_]{0,79}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (owner_id, idempotency_key),
    CHECK ((lease_owner IS NULL) = (lease_until IS NULL)),
    CHECK ((status IN ('succeeded','partial','failed','cancelled')) = (completed_at IS NOT NULL)),
    CHECK (status <> 'succeeded' OR (report IS NOT NULL AND verification IS NOT NULL)),
    CHECK (status <> 'partial' OR report IS NOT NULL),
    CHECK (status <> 'failed' OR failure_code IS NOT NULL),
    CHECK (status <> 'cancelled' OR cancel_requested)
);

CREATE INDEX research_task_owner_updated
    ON storage.research_task(owner_id, updated_at DESC);
CREATE INDEX research_task_claim
    ON storage.research_task(status, lease_until, created_at)
    WHERE status IN ('queued','running');

CREATE TABLE storage.research_event (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES storage.research_task(task_id) ON DELETE CASCADE,
    phase text NOT NULL
        CHECK (phase IN ('queued','plan','execute','reflect','generate','verify','completed')),
    status text NOT NULL
        CHECK (status IN ('queued','running','succeeded','partial','failed','cancelled')),
    code text NOT NULL CHECK (code ~ '^[a-z][a-z0-9_]{0,79}$'),
    occurred_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX research_event_task_sequence
    ON storage.research_event(task_id, sequence);
