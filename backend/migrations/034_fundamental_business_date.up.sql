-- data_date is the record's query/update date, not a universal upper bound for period_end.
DO $$
DECLARE row record;
BEGIN
  FOR row IN
    SELECT conrelid::regclass AS relation_name, conname
    FROM pg_constraint
    WHERE contype = 'c'
      AND connamespace = 'storage'::regnamespace
      AND conrelid IN (
        'storage.company_profile_record'::regclass,
        'storage.profitability_record'::regclass,
        'storage.balance_sheet_record'::regclass,
        'storage.cash_flow_record'::regclass,
        'storage.fundamental_aux_record'::regclass
      )
      AND pg_get_constraintdef(oid) LIKE '%period_end%data_date%'
  LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', row.relation_name, row.conname);
  END LOOP;
END $$;

