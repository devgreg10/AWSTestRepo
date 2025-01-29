CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_active_participant_counts_by_age_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        info.age,
        info.participant_count
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            age,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_active_participant_counts_by_age
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id,
            age
    ) dates
    LEFT JOIN
        ft_ds_refined.metric_historical_active_participant_counts_by_age info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
        AND dates.age = info.age
;