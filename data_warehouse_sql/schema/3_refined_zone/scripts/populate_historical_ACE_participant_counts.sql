INSERT INTO ft_ds_refined.metric_historical_ACE_participant_counts
SELECT
    TO_DATE(list_of_all_chapters.year::TEXT || '-12-31', 'YYYY-MM-DD') AS metric_calc_date,
    list_of_all_chapters.chapter_id,
    COALESCE(ACE_counts.ACE_participant_count, 0) AS ACE_participant_count,
    list_of_all_chapters.year::TEXT AS eoy_indicator
FROM (
        SELECT
            DISTINCT chapter_id, year
        FROM ft_ds_refined.current_and_historical_participants_view
        WHERE year != CAST(EXTRACT(YEAR FROM NOW()) AS INTEGER)
    ) list_of_all_chapters
LEFT JOIN (
    SELECT
        chapter_id,
        year,
        COUNT(*) AS ACE_participant_count
    FROM ft_ds_refined.current_and_historical_participants_view
    WHERE program_level = 'Ace'
    GROUP BY chapter_id, year
) ACE_counts
ON
    list_of_all_chapters.chapter_id = ACE_counts.chapter_id
    AND list_of_all_chapters.year = ACE_counts.year
ON CONFLICT (metric_calc_date, chapter_id) DO UPDATE SET
        ACE_participant_count = EXCLUDED.ACE_participant_count,
        eoy_indicator = EXCLUDED.eoy_indicator
;