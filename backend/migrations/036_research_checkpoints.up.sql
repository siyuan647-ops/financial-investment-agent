CREATE TABLE storage.research_role_checkpoint (
    task_id uuid NOT NULL REFERENCES storage.research_task(task_id) ON DELETE CASCADE,
    role text NOT NULL CHECK (role IN ('fundamental','news','market','investment','risk')),
    input_hash text NOT NULL CHECK (input_hash ~ '^[0-9a-f]{64}$'),
    prompt_version text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    output jsonb NOT NULL,
    tokens integer NOT NULL CHECK (tokens >= 0),
    execution_generation integer NOT NULL CHECK (execution_generation > 0),
    completed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, role)
);
