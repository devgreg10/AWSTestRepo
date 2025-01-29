CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_teen_percentage (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    teen_percentage NUMERIC(5,3),
    eoy_indicator VARCHAR(20)
);