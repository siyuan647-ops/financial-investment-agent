CREATE SCHEMA IF NOT EXISTS embedding;

CREATE TABLE embedding.collection_release (
    release_id uuid PRIMARY KEY,
    knowledge_kind text NOT NULL CHECK (knowledge_kind IN ('document','planning')),
    collection_name text NOT NULL UNIQUE CHECK (collection_name ~ '^[a-z][a-z0-9_]{2,127}$'),
    model_id text NOT NULL CHECK (length(model_id) BETWEEN 1 AND 200),
    model_revision text NOT NULL CHECK (model_revision ~ '^[0-9a-f]{40}$'),
    tokenizer_revision text NOT NULL CHECK (tokenizer_revision ~ '^[0-9a-f]{40}$'),
    dimension integer NOT NULL CHECK (dimension BETWEEN 2 AND 65536),
    pooling text NOT NULL CHECK (pooling = 'cls'),
    normalized boolean NOT NULL,
    query_instruction text NOT NULL CHECK (length(query_instruction) BETWEEN 1 AND 200),
    max_input_tokens integer NOT NULL CHECK (max_input_tokens BETWEEN 1 AND 8192),
    chunk_version text NOT NULL CHECK (length(chunk_version) BETWEEN 1 AND 100),
    status text NOT NULL CHECK (status IN ('building','active','retired')),
    created_at timestamptz NOT NULL DEFAULT now(),
    activated_at timestamptz,
    CHECK ((status = 'active') = (activated_at IS NOT NULL))
);

CREATE UNIQUE INDEX collection_release_one_active_kind
    ON embedding.collection_release(knowledge_kind) WHERE status = 'active';

ALTER TABLE storage.document
    ADD COLUMN vector_visible boolean NOT NULL DEFAULT false,
    ADD COLUMN vector_state text NOT NULL DEFAULT 'not_indexed'
        CHECK (vector_state IN ('not_indexed','queued','indexing','published','revoking','failed')),
    ADD COLUMN vector_collection_name text,
    ADD COLUMN vector_model_revision text,
    ADD COLUMN vector_chunk_version text,
    ADD COLUMN vector_count integer NOT NULL DEFAULT 0 CHECK (vector_count >= 0),
    ADD COLUMN vector_updated_at timestamptz,
    ADD CONSTRAINT document_vector_visibility_consistent CHECK (
        (vector_visible AND vector_state = 'published' AND vector_count > 0
            AND vector_collection_name IS NOT NULL AND vector_model_revision IS NOT NULL
            AND vector_chunk_version IS NOT NULL)
        OR NOT vector_visible
    );

CREATE INDEX document_vector_state_updated
    ON storage.document(vector_state, vector_updated_at);

CREATE TABLE embedding.index_job (
    job_id uuid PRIMARY KEY,
    document_id uuid NOT NULL REFERENCES storage.document(document_id) ON DELETE CASCADE,
    collection_name text NOT NULL REFERENCES embedding.collection_release(collection_name),
    operation text NOT NULL CHECK (operation IN ('publish','revoke')),
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued','running','succeeded','failed')),
    expected_count integer NOT NULL DEFAULT 0 CHECK (expected_count >= 0),
    indexed_count integer NOT NULL DEFAULT 0 CHECK (indexed_count >= 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    failure_code text CHECK (
        failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9_]{0,79}$'
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE(document_id, collection_name, operation),
    CHECK ((status = 'failed') = (failure_code IS NOT NULL)),
    CHECK ((status IN ('succeeded','failed')) = (completed_at IS NOT NULL))
);

CREATE INDEX index_job_status_updated
    ON embedding.index_job(status, updated_at);

