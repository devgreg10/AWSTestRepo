CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_twelve_up_engagement_counts (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    twelve_up_engagement_counts INTEGER,
    twelve_up_total_counts INTEGER,
    twelve_up_engagement_percentage NUMERIC(5,2)
);