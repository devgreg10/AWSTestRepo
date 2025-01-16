CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_teen_percentage()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE
        ft_ds_refined.metric_historical_teen_percentage
    SET
        eoy_indicator = NULL
    WHERE
        eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
    ;

    INSERT INTO ft_ds_refined.metric_historical_teen_percentage
    SELECT
        NOW() as metric_calc_date,
        chapter_id,
        --numerator is all those active participants 13+, cast to a float by * 1.0
        (COUNT(CASE WHEN calculated_age >= 13 THEN 1 END) * 1.0)/
        --denominator is all active participants
        COUNT(*)
        AS teen_percentage,
        CAST(EXTRACT(YEAR FROM NOW()) AS TEXT) AS eoy_indicator
    FROM ft_ds_refined.active_participants_view
    GROUP BY
    chapter_id
    ;
END;
$$;