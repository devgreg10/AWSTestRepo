CREATE TABLE IF NOT EXISTS ft_ds_refined.service_area_diversity_reference_data (
    PRIMARY KEY (chapter_id),
    chapter_id CHAR(18),
    chapter_name VARCHAR(100),
    asian_count INTEGER,
    black_or_african_american_count INTEGER,
    latino_or_hispanic_count INTEGER,
    multi_racial_count INTEGER,
    native_american_or_native_alaskan_count INTEGER,
    pacific_islander_count INTEGER,
    white_or_caucasian_count INTEGER,
    youth_population_count INTEGER,
    market_diversity_percentage NUMERIC(5,3)
);