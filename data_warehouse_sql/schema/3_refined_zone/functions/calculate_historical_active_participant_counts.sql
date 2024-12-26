CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_active_participant_counts()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE
        ft_ds_refined.metric_historical_active_participant_counts
    SET
        eoy_indicator = NULL
    WHERE
        eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
    ;

    INSERT INTO ft_ds_refined.metric_historical_active_participant_counts
    SELECT
        NOW() as metric_calc_date,
        chapter_id,
        COUNT(contact_id_18) AS participant_count,
        CAST(EXTRACT(YEAR FROM NOW()) AS TEXT) AS eoy_indicator
    FROM ft_ds_refined.active_participants_view
    GROUP BY
    chapter_id
    ;
END;
$$;