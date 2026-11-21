CREATE TABLE storage.model_invocation (
    invocation_id uuid PRIMARY KEY,
    request_id text NOT NULL CHECK (request_id ~ '^[a-zA-Z0-9_-]{1,128}$'),
    task_id text CHECK (task_id IS NULL OR task_id ~ '^[a-zA-Z0-9_-]{1,128}$'),
    purpose text NOT NULL CHECK (purpose IN ('plan','generate','verify')),
    provider text NOT NULL CHECK (provider = 'qwen'),
    model text NOT NULL CHECK (length(model) BETWEEN 1 AND 128),
    prompt_version text NOT NULL CHECK (prompt_version ~ '^[a-zA-Z0-9_.:-]{1,128}$'),
    stream boolean NOT NULL,
    status text NOT NULL DEFAULT 'running'
        CHECK (status IN ('running','succeeded','failed','cancelled')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    duration_ms bigint CHECK (duration_ms IS NULL OR duration_ms >= 0),
    input_tokens bigint CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens bigint CHECK (output_tokens IS NULL OR output_tokens >= 0),
    total_tokens bigint CHECK (total_tokens IS NULL OR total_tokens >= 0),
    estimated_cost numeric(20,8) CHECK (estimated_cost IS NULL OR estimated_cost >= 0),
    cost_currency text CHECK (cost_currency IS NULL OR cost_currency ~ '^[A-Z]{3}$'),
    error_kind text CHECK (error_kind IS NULL OR error_kind ~ '^[a-z][a-z0-9_]{0,79}$'),
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CHECK ((estimated_cost IS NULL) = (cost_currency IS NULL)),
    CHECK ((status = 'running') = (completed_at IS NULL)),
    CHECK (
        (input_tokens IS NULL AND output_tokens IS NULL AND total_tokens IS NULL)
        OR
        (input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND total_tokens IS NOT NULL
         AND total_tokens >= input_tokens + output_tokens)
    )
);
CREATE INDEX model_invocation_started ON storage.model_invocation(started_at DESC);
CREATE INDEX model_invocation_dimensions
    ON storage.model_invocation(purpose, model, status, started_at DESC);
CREATE INDEX model_invocation_request ON storage.model_invocation(request_id, started_at DESC);
CREATE INDEX model_invocation_task
    ON storage.model_invocation(task_id, started_at DESC) WHERE task_id IS NOT NULL;
