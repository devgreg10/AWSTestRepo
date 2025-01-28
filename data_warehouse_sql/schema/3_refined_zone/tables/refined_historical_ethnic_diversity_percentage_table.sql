CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_ethnic_diversity_percentage (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    ethnic_diversity_percentage NUMERIC(5,3),
    eoy_indicator VARCHAR(20)
);