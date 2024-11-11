CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_twelve_plus_retention_percentage (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    twelve_plus_retention_percentage NUMERIC(5,2)
);