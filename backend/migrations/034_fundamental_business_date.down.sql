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
      'ALTER TABLE storage.%I ADD CONSTRAINT %I '
      'CHECK (period_end IS NULL OR period_end <= data_date)',
      table_name, table_name || '_period_data_date_check'
    );
  END LOOP;
END $$;

