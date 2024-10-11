CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_active_participant_counts()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_refined.metric_historical_active_participant_counts
    SELECT
        NOW() as metric_calc_date,
        chapter_id,
        COUNT(contact_id_18) AS participant_count
    FROM ft_ds_refined.active_participants_view
    GROUP BY
    chapter_id
    ;
END;
$$;