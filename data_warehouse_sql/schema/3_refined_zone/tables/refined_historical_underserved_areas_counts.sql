CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_underserved_areas_counts (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    count_in_underserved_areas INTEGER
);