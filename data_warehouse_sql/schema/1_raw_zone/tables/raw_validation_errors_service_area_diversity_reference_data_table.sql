CREATE TABLE IF NOT EXISTS ft_ds_raw.validation_errors_service_area_diversity_reference_data (
    chapter_id TEXT,
    chapter_name TEXT,
    asian_count TEXT,
    black_or_african_american_count TEXT,
    latino_or_hispanic_count TEXT,
    multi_racial_count TEXT,
    native_american_or_native_alaskan_count TEXT,
    pacific_islander_count TEXT,
    white_or_caucasian_count TEXT,
    youth_population_count TEXT,
    market_diversity_percentage TEXT,
    required_fields_validated BOOLEAN,
    optional_fields_validated BOOLEAN,
    fixed_in_source BOOLEAN
);