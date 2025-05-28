BEGIN
    UPDATE
        ft_ds_refined.metric_historical_waitlist_not_selected_counts
    SET
        eoy_indicator = NULL
    WHERE
        eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
    ;

    INSERT INTO ft_ds_refined.metric_historical_waitlist_not_selected_counts
    SELECT
        NOW() as metric_calc_date,
        chapter_id,
        COUNT(waitlist_id) AS waitlist_not_selected_count,
        CAST(EXTRACT(YEAR FROM NOW()) AS TEXT) AS eoy_indicator
    FROM 
        ft_ds_refined.waitlist_view
    WHERE
        status = 'Waitlist Pending'
    GROUP BY
    chapter_id
    ;
END;