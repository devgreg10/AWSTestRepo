INSERT INTO ft_ds_refined.metric_historical_ACE_participant_counts
SELECT
    TO_DATE(year::TEXT || '-12-31', 'YYYY-MM-DD') AS metric_calc_date,
    chapter_id,
    COUNT(*) AS ACE_participant_count,
    year::TEXT AS eoy_indicator
FROM ft_ds_refined.current_and_historical_participants_view 
WHERE year != CAST(EXTRACT(YEAR FROM NOW()) AS INTEGER)
AND program_level = 'Ace'
GROUP BY year, chapter_id
ON CONFLICT (metric_calc_date, chapter_id) DO UPDATE SET
        ACE_participant_count = EXCLUDED.ACE_participant_count,
        eoy_indicator = EXCLUDED.eoy_indicator
;