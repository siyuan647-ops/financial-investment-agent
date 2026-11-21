CREATE TABLE storage.text_source (
    source_id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE CHECK (length(name) BETWEEN 1 AND 120),
    source_kind text NOT NULL CHECK (source_kind IN ('announcement','news')),
    authority_tier text NOT NULL CHECK (
        authority_tier IN ('primary_disclosure','authorized_media','secondary_media')
    ),
    endpoint_url text NOT NULL CHECK (endpoint_url ~ '^https://'),
    capability_status text NOT NULL CHECK (
        capability_status IN ('available','unavailable','verifying')
    ),
    permission_basis text NOT NULL CHECK (length(permission_basis) BETWEEN 1 AND 500),
    coverage_note text NOT NULL CHECK (length(coverage_note) BETWEEN 1 AND 1000),
    attention_terms text[] NOT NULL DEFAULT '{}',
    enabled boolean NOT NULL DEFAULT false,
    cursor_value text,
    last_synced_at timestamptz,
    last_sync_status text CHECK (
        last_sync_status IS NULL OR last_sync_status IN ('succeeded','failed','partial')
    ),
    last_failure_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (NOT enabled OR capability_status='available')
);

CREATE TABLE storage.text_item (
    item_id uuid PRIMARY KEY,
    source_id uuid NOT NULL REFERENCES storage.text_source(source_id) ON DELETE CASCADE,
    external_id text NOT NULL CHECK (length(external_id) BETWEEN 1 AND 300),
    canonical_url text NOT NULL CHECK (canonical_url ~ '^https://'),
    title text NOT NULL CHECK (length(title) BETWEEN 1 AND 500),
    body text NOT NULL CHECK (length(body) BETWEEN 1 AND 2000000),
    published_at timestamptz,
    published_at_status text NOT NULL CHECK (
        published_at_status IN ('verified','date_only','source_reported','unknown')
    ),
    security_codes text[] NOT NULL DEFAULT '{}',
    canonical_hash text NOT NULL CHECK (canonical_hash ~ '^[0-9a-f]{64}$'),
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    duplicate_group text NOT NULL CHECK (duplicate_group ~ '^[0-9a-f]{64}$'),
    document_id uuid REFERENCES storage.document(document_id),
    fetched_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_id, external_id),
    UNIQUE (source_id, canonical_hash)
);
CREATE INDEX text_item_published ON storage.text_item(published_at DESC NULLS LAST);
CREATE INDEX text_item_codes ON storage.text_item USING gin(security_codes);
CREATE INDEX text_item_duplicate_group ON storage.text_item(duplicate_group);

CREATE TABLE storage.text_attention_event (
    event_id uuid PRIMARY KEY,
    item_id uuid NOT NULL UNIQUE REFERENCES storage.text_item(item_id) ON DELETE CASCADE,
    attention boolean NOT NULL,
    matched_terms text[] NOT NULL DEFAULT '{}',
    coverage_complete boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX text_attention_created ON storage.text_attention_event(created_at DESC);

CREATE TABLE storage.text_sync_run (
    run_id uuid PRIMARY KEY,
    source_id uuid NOT NULL REFERENCES storage.text_source(source_id) ON DELETE CASCADE,
    status text NOT NULL CHECK (status IN ('running','succeeded','failed','partial')),
    fetched_count integer NOT NULL DEFAULT 0 CHECK (fetched_count >= 0),
    inserted_count integer NOT NULL DEFAULT 0 CHECK (inserted_count >= 0),
    failure_code text,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);
CREATE INDEX text_sync_source_started ON storage.text_sync_run(source_id,started_at DESC);
