CREATE TABLE IF NOT EXISTS ft_ds_raw.validation_errors_dci_reference_data (
    zip_code TEXT,
    chapter_name TEXT,
    chapter_id TEXT,
    dci_score TEXT,
    required_fields_validated BOOLEAN,
    optional_fields_validated BOOLEAN,
    fixed_in_source BOOLEAN
);