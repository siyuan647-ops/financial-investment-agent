CREATE TABLE storage.security_alias (
    business_key text NOT NULL,
    data_version text PRIMARY KEY REFERENCES storage.record_version,
    ts_code text NOT NULL,
    payload jsonb NOT NULL
);
CREATE INDEX security_alias_code ON storage.security_alias(ts_code);
CREATE TRIGGER security_alias_immutable BEFORE UPDATE OR DELETE ON storage.security_alias
    FOR EACH ROW EXECUTE FUNCTION storage.reject_mutation();
