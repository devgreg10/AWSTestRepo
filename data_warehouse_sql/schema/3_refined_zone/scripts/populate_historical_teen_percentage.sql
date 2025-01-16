INSERT INTO ft_ds_refined.metric_historical_teen_percentage 
SELECT
    TO_DATE(year::TEXT || '-12-31', 'YYYY-MM-DD') AS metric_calc_date,
    chapter_id,
    --numerator is all those active participants 13+, cast to a float by * 1.0
    (COUNT(CASE WHEN calculated_age >= 13 THEN 1 END) * 1.0)/
    --denominator is all active participants
    COUNT(*)
    AS teen_percentage,
    year::TEXT AS eoy_indicator
FROM ft_ds_refined.current_and_historical_participants_view 
WHERE year != CAST(EXTRACT(YEAR FROM NOW()) AS INTEGER)
GROUP BY year, chapter_id
ON CONFLICT (metric_calc_date, chapter_id) DO UPDATE SET
        teen_percentage = EXCLUDED.teen_percentage,
        eoy_indicator = EXCLUDED.eoy_indicator
;