INSERT INTO ft_ds_refined.metric_historical_non_male_percentage 
SELECT
    TO_DATE(year::TEXT || '-12-31', 'YYYY-MM-DD') AS metric_calc_date,
    chapter_id,
    (COUNT(CASE WHEN gender NOT IN ('Male', 'Prefer not to respond') THEN 1 END) * 1.0)/
    NULLIF(COUNT(CASE WHEN gender <> 'Prefer not to respond' THEN 1 END), 0)
    AS non_male_percentage,
    year::TEXT AS eoy_indicator
FROM ft_ds_refined.current_and_historical_participants_view 
WHERE year != CAST(EXTRACT(YEAR FROM NOW()) AS INTEGER)
GROUP BY year, chapter_id
ON CONFLICT (metric_calc_date, chapter_id) DO UPDATE SET
        non_male_percentage = EXCLUDED.non_male_percentage,
        eoy_indicator = EXCLUDED.eoy_indicator
;