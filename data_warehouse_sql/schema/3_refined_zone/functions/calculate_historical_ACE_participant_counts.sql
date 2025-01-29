CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_ACE_participant_counts()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE
        ft_ds_refined.metric_historical_ACE_participant_counts
    SET
        eoy_indicator = NULL
    WHERE
        eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
    ;

    INSERT INTO ft_ds_refined.metric_historical_ACE_participant_counts
    SELECT
        NOW() as metric_calc_date,
        list_of_all_chapters.chapter_id,
        COALESCE(ACE_participant_count, 0) AS ACE_participant_count,
        CAST(EXTRACT(YEAR FROM NOW()) AS TEXT) AS eoy_indicator
    FROM (
        SELECT
            DISTINCT chapter_id
        FROM ft_ds_refined.active_participants_view
    ) list_of_all_chapters
    LEFT JOIN (
        SELECT
            chapter_id,
            COUNT(*) AS ACE_participant_count
        FROM ft_ds_refined.active_participants_view
        WHERE program_level = 'Ace'
        GROUP BY
        chapter_id
    ) ACE_counts
    ON
        list_of_all_chapters.chapter_id = ACE_counts.chapter_id
    ;
END;
$$;