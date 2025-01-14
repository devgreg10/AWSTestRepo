CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_non_male_percentage()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE
        ft_ds_refined.metric_historical_non_male_percentage
    SET
        eoy_indicator = NULL
    WHERE
        eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
    ;

    INSERT INTO ft_ds_refined.metric_historical_non_male_percentage
    SELECT
        NOW() as metric_calc_date,
        chapter_id,
        (COUNT(CASE WHEN gender NOT IN ('Male', 'Prefer not to respond') THEN 1 END) * 1.0)/
        NULLIF(COUNT(CASE WHEN gender <> 'Prefer not to respond' THEN 1 END), 0)
        AS non_male_percentage,
        CAST(EXTRACT(YEAR FROM NOW()) AS TEXT) AS eoy_indicator
    FROM ft_ds_refined.active_participants_view
    GROUP BY
    chapter_id
    ;
END;
$$;