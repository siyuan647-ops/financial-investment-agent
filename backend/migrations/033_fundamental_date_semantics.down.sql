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
      'CHECK (announced_on IS NULL OR period_end IS NULL OR announced_on >= period_end)',
      table_name, table_name || '_announcement_period_check'
    );
  END LOOP;
END $$;

