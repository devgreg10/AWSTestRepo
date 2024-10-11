CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_female_percentage()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_refined.metric_historical_female_percentage
    SELECT
        NOW() as metric_calc_date,
        chapter_id,
        (COUNT(CASE WHEN gender ='Female' THEN 1 END) * 1.0)/
        NULLIF(COUNT(CASE WHEN gender <> 'Prefer not to respond' THEN 1 END), 0)
        AS female_percentage
    FROM ft_ds_refined.active_participants_view
    GROUP BY
    chapter_id
    ;
END;
$$;