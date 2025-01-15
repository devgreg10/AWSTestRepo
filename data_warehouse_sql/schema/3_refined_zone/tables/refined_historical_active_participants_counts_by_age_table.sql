CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_active_participant_counts_by_age (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    age INTEGER,
    participant_count INTEGER,
    eoy_indicator VARCHAR(20)
);