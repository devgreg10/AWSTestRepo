INSERT INTO ft_ds_refined.metric_historical_active_participant_counts_by_age
SELECT
    TO_DATE(year::TEXT || '-12-31', 'YYYY-MM-DD') AS metric_calc_date,
    chapter_id,
    calculated_age AS age,
    COUNT(contact_id) AS participant_count,
    year::TEXT AS eoy_indicator
FROM ft_ds_refined.current_and_historical_participants_view 
WHERE year != CAST(EXTRACT(YEAR FROM NOW()) AS INTEGER)
--needs this filter since the international chapters do not have birthdates, so they will have NULL calculated ages. The presence of NULL calculated ages results in PK constraint errors
AND calculated_age IS NOT NULL
GROUP BY year, chapter_id, calculated_age
ON CONFLICT (metric_calc_date, chapter_id, age) DO UPDATE SET
        participant_count = EXCLUDED.participant_count,
        eoy_indicator = EXCLUDED.eoy_indicator
;