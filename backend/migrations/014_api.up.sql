CREATE TABLE storage.api_task (
    task_id uuid PRIMARY KEY,
    owner_id text NOT NULL CHECK (owner_id ~ '^[a-zA-Z0-9_-]{1,64}$'),
    kind text NOT NULL CHECK (kind IN ('backfill','gap_fill','retry')),
    deployment text NOT NULL,
    status text NOT NULL CHECK (status IN ('submitting','submitted','submission_failed')),
    flow_run_id uuid,
    parameters jsonb NOT NULL,
    result jsonb NOT NULL DEFAULT '{}',
    idempotency_key text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX api_task_owner_created ON storage.api_task(owner_id, created_at DESC);
CREATE INDEX api_task_flow_run ON storage.api_task(flow_run_id) WHERE flow_run_id IS NOT NULL;
