CREATE TABLE storage.quality_rule_catalog (
    rule_id text NOT NULL,
    processing_version text NOT NULL,
    category text NOT NULL CHECK (category IN
        ('completeness','uniqueness','freshness','period','unit','relationship','source','applicability')),
    severity text NOT NULL CHECK (severity IN ('blocking','warning','not_applicable')),
    description text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (rule_id, processing_version)
);

CREATE TABLE storage.quality_quarantine (
    quarantine_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES storage.pipeline_run(run_id),
    raw_id uuid REFERENCES storage.raw_response(raw_id),
    data_version text REFERENCES storage.record_version(data_version),
    rule_id text NOT NULL,
    rule_version text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('blocking','warning','not_applicable')),
    subject_kind text NOT NULL,
    subject_key text NOT NULL,
    reason_code text NOT NULL CHECK (reason_code ~ '^[a-z][a-z0-9_]{0,79}$'),
    original_value jsonb,
    detail jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (rule_id, rule_version)
        REFERENCES storage.quality_rule_catalog(rule_id, processing_version),
    CHECK (raw_id IS NOT NULL OR data_version IS NOT NULL OR original_value IS NOT NULL)
);
CREATE INDEX quality_quarantine_run ON storage.quality_quarantine(run_id, created_at);
CREATE INDEX quality_quarantine_subject ON storage.quality_quarantine(subject_kind, subject_key);

CREATE TABLE storage.quality_quarantine_event (
    event_id uuid PRIMARY KEY,
    quarantine_id uuid NOT NULL REFERENCES storage.quality_quarantine(quarantine_id),
    event_type text NOT NULL CHECK (event_type IN
        ('quarantined','retry_requested','retry_failed','resolved')),
    run_id uuid NOT NULL REFERENCES storage.pipeline_run(run_id),
    processing_version text NOT NULL,
    detail jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX quality_quarantine_event_latest
    ON storage.quality_quarantine_event(quarantine_id, created_at DESC, event_id DESC);

CREATE TABLE storage.completeness_assessment (
    assessment_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES storage.pipeline_run(run_id),
    domain text NOT NULL,
    processing_version text NOT NULL,
    expected_count integer NOT NULL CHECK (expected_count >= 0),
    eligible_count integer NOT NULL CHECK (eligible_count >= 0),
    received_count integer NOT NULL CHECK (received_count >= 0),
    exception_count integer NOT NULL CHECK (exception_count >= 0),
    missing_count integer NOT NULL CHECK (missing_count >= 0),
    unexpected_count integer NOT NULL CHECK (unexpected_count >= 0),
    completeness numeric NOT NULL CHECK (completeness BETWEEN 0 AND 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (eligible_count = expected_count - exception_count),
    CHECK (received_count + missing_count = eligible_count)
);
CREATE INDEX completeness_assessment_run
    ON storage.completeness_assessment(run_id, domain, created_at DESC);

CREATE TABLE storage.completeness_item (
    assessment_id uuid NOT NULL REFERENCES storage.completeness_assessment(assessment_id),
    subject_key text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('received','exception','missing','unexpected')),
    reason_code text,
    PRIMARY KEY (assessment_id, subject_key)
);

CREATE TABLE storage.lineage_node (
    node_id text PRIMARY KEY CHECK (node_id ~ '^[0-9a-f]{64}$'),
    node_type text NOT NULL CHECK (node_type IN
        ('raw','standard','derived','evidence','report','alert')),
    reference_key text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (node_type, reference_key)
);

CREATE TABLE storage.lineage_edge (
    parent_node_id text NOT NULL REFERENCES storage.lineage_node(node_id),
    child_node_id text NOT NULL REFERENCES storage.lineage_node(node_id),
    relation text NOT NULL CHECK (relation IN
        ('normalized_to','derived_to','evidenced_by','consumed_by')),
    run_id uuid NOT NULL REFERENCES storage.pipeline_run(run_id),
    processing_version text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_node_id, child_node_id, relation),
    CHECK (parent_node_id <> child_node_id)
);
CREATE INDEX lineage_edge_child ON storage.lineage_edge(child_node_id);

DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['quality_rule_catalog','quality_quarantine',
    'quality_quarantine_event','completeness_assessment','completeness_item',
    'lineage_node','lineage_edge'] LOOP
    EXECUTE format('CREATE TRIGGER immutable BEFORE UPDATE OR DELETE ON storage.%I '
      'FOR EACH ROW EXECUTE FUNCTION storage.reject_mutation()', t);
  END LOOP;
END $$;
