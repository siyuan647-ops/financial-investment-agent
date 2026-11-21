CREATE TABLE storage.document (
    document_id uuid PRIMARY KEY,
    owner_id text NOT NULL CHECK (owner_id ~ '^[a-zA-Z0-9_-]{1,64}$'),
    sha256 text NOT NULL UNIQUE CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    byte_count bigint NOT NULL CHECK (byte_count BETWEEN 1 AND 52428800),
    relative_path text NOT NULL CHECK (
        relative_path ~ '^[0-9a-f]{2}/[0-9a-f]{64}\.pdf$'
    ),
    media_type text NOT NULL DEFAULT 'application/pdf'
        CHECK (media_type = 'application/pdf'),
    title text NOT NULL CHECK (length(title) BETWEEN 1 AND 300),
    document_type text NOT NULL CHECK (
        document_type IN ('annual_report','announcement','regulation','research_report')
    ),
    ts_code text CHECK (ts_code IS NULL OR ts_code ~ '^[0-9]{6}\.(SH|SZ)$'),
    publication_date date NOT NULL,
    effective_from date,
    effective_to date,
    version text NOT NULL CHECK (length(version) BETWEEN 1 AND 100),
    supersedes_document_id uuid REFERENCES storage.document(document_id),
    access_scope text NOT NULL CHECK (
        access_scope IN ('public','authenticated','restricted')
    ),
    allowed_user_ids text[] NOT NULL DEFAULT '{}',
    allowed_roles text[] NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'queued' CHECK (
        status IN ('queued','processing','review_required','approved','rejected','failed')
    ),
    flow_run_id uuid,
    parser_version text CHECK (
        parser_version IS NULL OR parser_version ~ '^step20-v[0-9]+$'
    ),
    quality jsonb,
    failure_code text CHECK (
        failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9_]{0,79}$'
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (effective_to IS NULL OR effective_from IS NOT NULL),
    CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CHECK (
        (access_scope = 'restricted' AND
            (cardinality(allowed_user_ids) > 0 OR cardinality(allowed_roles) > 0))
        OR
        (access_scope <> 'restricted' AND
            cardinality(allowed_user_ids) = 0 AND cardinality(allowed_roles) = 0)
    ),
    CHECK (status <> 'failed' OR failure_code IS NOT NULL),
    CHECK (status NOT IN ('review_required','approved','rejected') OR quality IS NOT NULL),
    CHECK (supersedes_document_id IS NULL OR document_type = 'regulation')
);

CREATE INDEX document_owner_created
    ON storage.document(owner_id, created_at DESC);
CREATE INDEX document_status_updated
    ON storage.document(status, updated_at DESC);
CREATE INDEX document_security_publication
    ON storage.document(ts_code, publication_date DESC)
    WHERE ts_code IS NOT NULL;

CREATE TABLE storage.document_chunk (
    chunk_id uuid PRIMARY KEY,
    document_id uuid NOT NULL REFERENCES storage.document(document_id) ON DELETE CASCADE,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    kind text NOT NULL CHECK (kind IN ('text','table')),
    page_start integer NOT NULL CHECK (page_start >= 1),
    page_end integer NOT NULL CHECK (page_end >= page_start),
    section_path text[] NOT NULL DEFAULT '{}',
    content text NOT NULL CHECK (length(content) BETWEEN 1 AND 8000),
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    parser_version text NOT NULL CHECK (parser_version ~ '^step20-v[0-9]+$'),
    cross_page boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (document_id, ordinal),
    CHECK (cross_page = (page_end > page_start))
);

CREATE INDEX document_chunk_document_page
    ON storage.document_chunk(document_id, page_start, ordinal);

CREATE TABLE storage.document_review (
    review_id uuid PRIMARY KEY,
    document_id uuid NOT NULL REFERENCES storage.document(document_id) ON DELETE CASCADE,
    reviewer_id text NOT NULL CHECK (reviewer_id ~ '^[a-zA-Z0-9_-]{1,64}$'),
    decision text NOT NULL CHECK (decision IN ('approve','reject')),
    reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 2000),
    accepted_low_quality boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX document_review_document_created
    ON storage.document_review(document_id, created_at DESC);
