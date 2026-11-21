DROP TABLE IF EXISTS embedding.index_job;

DROP INDEX IF EXISTS storage.document_vector_state_updated;
ALTER TABLE storage.document
    DROP CONSTRAINT IF EXISTS document_vector_visibility_consistent,
    DROP COLUMN IF EXISTS vector_updated_at,
    DROP COLUMN IF EXISTS vector_count,
    DROP COLUMN IF EXISTS vector_chunk_version,
    DROP COLUMN IF EXISTS vector_model_revision,
    DROP COLUMN IF EXISTS vector_collection_name,
    DROP COLUMN IF EXISTS vector_state,
    DROP COLUMN IF EXISTS vector_visible;

DROP TABLE IF EXISTS embedding.collection_release;
DROP SCHEMA IF EXISTS embedding;

