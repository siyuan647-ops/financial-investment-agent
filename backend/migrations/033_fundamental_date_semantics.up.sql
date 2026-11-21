-- Announcement/event dates have source-specific semantics. A dividend announcement may
-- legitimately precede the source end_date, so only source-specific normalizers may compare them.
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
      AND pg_get_constraintdef(oid) LIKE '%announced_on%period_end%'
  LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', row.relation_name, row.conname);
  END LOOP;
END $$;

