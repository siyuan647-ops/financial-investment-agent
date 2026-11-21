ALTER TABLE storage.lineage DISABLE TRIGGER immutable;
DELETE FROM storage.lineage WHERE data_version IN
    (SELECT data_version FROM storage.record_version WHERE kind = 'security_alias');
ALTER TABLE storage.lineage ENABLE TRIGGER immutable;
DROP TABLE storage.security_alias;
ALTER TABLE storage.record_version DISABLE TRIGGER immutable;
DELETE FROM storage.record_version WHERE kind = 'security_alias';
ALTER TABLE storage.record_version ENABLE TRIGGER immutable;
-- Raw responses and pipeline runs remain available for replay.
