DELETE FROM storage.model_invocation WHERE purpose = 'route';
ALTER TABLE storage.model_invocation
    DROP CONSTRAINT model_invocation_purpose_check;
ALTER TABLE storage.model_invocation
    ADD CONSTRAINT model_invocation_purpose_check
    CHECK (purpose IN ('plan','generate','verify'));
