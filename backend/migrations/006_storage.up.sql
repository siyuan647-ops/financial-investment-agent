CREATE SCHEMA storage;
CREATE TABLE storage.pipeline_run (
    run_id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    status text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
    parameters jsonb NOT NULL,
    result jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE storage.raw_response (
    raw_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES storage.pipeline_run,
    provider text NOT NULL,
    source_api text NOT NULL,
    parameters jsonb NOT NULL,
    fetched_at timestamptz NOT NULL,
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    byte_count bigint NOT NULL CHECK (byte_count >= 0),
    relative_path text NOT NULL UNIQUE CHECK (relative_path ~ '^[0-9a-f]{32}\.bin$')
);
CREATE INDEX raw_response_hash ON storage.raw_response(sha256);
CREATE TABLE storage.record_version (
    data_version text PRIMARY KEY CHECK (data_version ~ '^[0-9a-f]{64}$'),
    kind text NOT NULL,
    business_key text NOT NULL,
    semantic_hash text NOT NULL,
    raw_id uuid NOT NULL REFERENCES storage.raw_response,
    available_at timestamptz NOT NULL,
    fetched_at timestamptz NOT NULL,
    supersedes_version text REFERENCES storage.record_version,
    UNIQUE(kind, business_key, data_version),
    CHECK (supersedes_version IS DISTINCT FROM data_version)
);
CREATE INDEX record_version_lookup ON storage.record_version
    (kind, business_key, available_at DESC, fetched_at DESC, data_version);
CREATE INDEX record_version_supersedes ON storage.record_version(supersedes_version)
    WHERE supersedes_version IS NOT NULL;
CREATE TABLE storage.security (
    business_key text NOT NULL,
    data_version text PRIMARY KEY REFERENCES storage.record_version,
    ts_code text NOT NULL,
    payload jsonb NOT NULL
);
CREATE INDEX security_code ON storage.security(ts_code);
CREATE TABLE storage.trading_calendar (
    business_key text NOT NULL,
    data_version text PRIMARY KEY REFERENCES storage.record_version,
    exchange text NOT NULL,
    calendar_date date NOT NULL,
    payload jsonb NOT NULL
);
CREATE INDEX calendar_lookup ON storage.trading_calendar(exchange, calendar_date);
CREATE TABLE storage.price_bar (
    business_key text NOT NULL,
    data_version text NOT NULL REFERENCES storage.record_version,
    ts_code text NOT NULL,
    trade_date date NOT NULL,
    close_value numeric,
    payload jsonb NOT NULL,
    PRIMARY KEY(trade_date, data_version)
);
SELECT create_hypertable('storage.price_bar', by_range('trade_date', INTERVAL '1 year'));
CREATE INDEX price_bar_range ON storage.price_bar(ts_code, trade_date DESC);
CREATE TABLE storage.adjustment_factor (
    business_key text NOT NULL,
    data_version text PRIMARY KEY REFERENCES storage.record_version,
    ts_code text NOT NULL,
    trade_date date NOT NULL,
    payload jsonb NOT NULL
);
CREATE INDEX adjustment_range ON storage.adjustment_factor(ts_code, trade_date);
CREATE TABLE storage.valuation (
    business_key text NOT NULL,
    data_version text PRIMARY KEY REFERENCES storage.record_version,
    ts_code text NOT NULL,
    trade_date date NOT NULL,
    payload jsonb NOT NULL
);
CREATE INDEX valuation_range ON storage.valuation(ts_code, trade_date);
CREATE TABLE storage.financial_fact (
    business_key text NOT NULL,
    data_version text PRIMARY KEY REFERENCES storage.record_version,
    ts_code text NOT NULL,
    period_end date NOT NULL,
    metric text NOT NULL,
    amount_value numeric,
    payload jsonb NOT NULL
);
CREATE INDEX financial_lookup ON storage.financial_fact(ts_code, period_end, metric);
CREATE TABLE storage.corporate_action (
    business_key text NOT NULL,
    data_version text PRIMARY KEY REFERENCES storage.record_version,
    ts_code text NOT NULL,
    event_id text NOT NULL,
    payload jsonb NOT NULL
);
CREATE INDEX action_lookup ON storage.corporate_action(ts_code, event_id);
CREATE TABLE storage.industry_membership (
    business_key text NOT NULL,
    data_version text PRIMARY KEY REFERENCES storage.record_version,
    ts_code text NOT NULL,
    payload jsonb NOT NULL
);
CREATE INDEX industry_lookup ON storage.industry_membership(ts_code);
CREATE TABLE storage.quality_issue (
    issue_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES storage.pipeline_run,
    raw_id uuid REFERENCES storage.raw_response,
    data_version text REFERENCES storage.record_version,
    rule text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('blocking','warning','not_applicable')),
    detail jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE storage.lineage (
    raw_id uuid NOT NULL REFERENCES storage.raw_response,
    data_version text NOT NULL REFERENCES storage.record_version,
    PRIMARY KEY(raw_id, data_version)
);
-- Versioned facts and their source registrations are immutable, including direct SQL.
CREATE FUNCTION storage.reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'append_only_storage'; END $$;
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['raw_response','record_version','security','trading_calendar',
    'price_bar','adjustment_factor','valuation','financial_fact','corporate_action',
    'industry_membership','lineage'] LOOP
    EXECUTE format('CREATE TRIGGER immutable BEFORE UPDATE OR DELETE ON storage.%I '
      'FOR EACH ROW EXECUTE FUNCTION storage.reject_mutation()', t);
  END LOOP;
END $$;
