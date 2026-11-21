-- Five append-only business domains. Raw source fields remain in the versioned payload;
-- common dates are first-class columns so incremental and as-of queries cannot confuse them.
DO $$
DECLARE table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'company_profile_record',
    'profitability_record',
    'balance_sheet_record',
    'cash_flow_record',
    'fundamental_aux_record'
  ] LOOP
    EXECUTE format(
      'CREATE TABLE storage.%I ('
      'business_key text NOT NULL,'
      'data_version text PRIMARY KEY REFERENCES storage.record_version,'
      'ts_code text NOT NULL CHECK (ts_code ~ ''^[0-9]{6}\.(SH|SZ)$''),'
      'record_type text NOT NULL CHECK (record_type ~ ''^[a-z][a-z0-9_]{0,79}$''),'
      'source_key text NOT NULL,'
      'data_date date NOT NULL,'
      'period_end date,'
      'announced_on date,'
      'effective_on date,'
      'payload jsonb NOT NULL,'
      'CHECK (period_end IS NULL OR period_end <= data_date),'
      'CHECK (announced_on IS NULL OR period_end IS NULL OR announced_on >= period_end)'
      ')',
      table_name
    );
    EXECUTE format(
      'CREATE INDEX %I ON storage.%I(ts_code,record_type,data_date DESC)',
      table_name || '_lookup', table_name
    );
    EXECUTE format(
      'CREATE INDEX %I ON storage.%I(ts_code,period_end DESC) WHERE period_end IS NOT NULL',
      table_name || '_period', table_name
    );
    EXECUTE format(
      'CREATE TRIGGER immutable BEFORE UPDATE OR DELETE ON storage.%I '
      'FOR EACH ROW EXECUTE FUNCTION storage.reject_mutation()',
      table_name
    );
  END LOOP;
END $$;

