DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['lineage_edge','lineage_node','completeness_item',
    'completeness_assessment','quality_quarantine_event','quality_quarantine',
    'quality_rule_catalog'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS immutable ON storage.%I', t);
  END LOOP;
END $$;

DROP TABLE storage.lineage_edge;
DROP TABLE storage.lineage_node;
DROP TABLE storage.completeness_item;
DROP TABLE storage.completeness_assessment;
DROP TABLE storage.quality_quarantine_event;
DROP TABLE storage.quality_quarantine;
DROP TABLE storage.quality_rule_catalog;
