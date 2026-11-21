DROP TABLE storage.fundamental_aux_record;
DROP TABLE storage.cash_flow_record;
DROP TABLE storage.balance_sheet_record;
DROP TABLE storage.profitability_record;
DROP TABLE storage.company_profile_record;
DELETE FROM storage.lineage WHERE data_version IN (
  SELECT data_version FROM storage.record_version WHERE kind IN (
    'company_profile_record',
    'profitability_record',
    'balance_sheet_record',
    'cash_flow_record',
    'fundamental_aux_record'
  )
);
DELETE FROM storage.record_version WHERE kind IN (
  'company_profile_record',
  'profitability_record',
  'balance_sheet_record',
  'cash_flow_record',
  'fundamental_aux_record'
);

