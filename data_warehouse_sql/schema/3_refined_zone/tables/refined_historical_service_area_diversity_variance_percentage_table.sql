CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_service_area_diversity_variance_percentage (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    service_area TEXT,
    service_area_diversity_variance_percentage NUMERIC(5,3),
    eoy_indicator VARCHAR(20)
);