CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_active_participant_counts (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    participant_count INTEGER
);