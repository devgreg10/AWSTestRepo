CREATE TABLE IF NOT EXISTS ft_ds_raw.validation_errors_sf_waitlist (
    waitlist_id TEXT,
    chapter_id TEXT,
    is_deleted TEXT,
    sf_created_timestamp TEXT,
    sf_last_modified_timestamp TEXT,
    sf_system_modstamp TEXT,
    dss_ingestion_timestamp TIMESTAMPTZ,
    required_fields_validated BOOLEAN,
    optional_fields_validated BOOLEAN,
    fixed_in_source BOOLEAN
);