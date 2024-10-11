CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_tenure_counts (
    PRIMARY KEY(metric_calc_date, chapter_id, years_tenured),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    years_tenured VARCHAR(10),
    tenure_count INTEGER
);