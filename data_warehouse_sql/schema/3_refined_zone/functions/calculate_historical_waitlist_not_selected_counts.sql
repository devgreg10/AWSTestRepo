-- FUNCTION: ft_ds_admin.calculate_historical_waitlist_not_selected_counts()

-- DROP FUNCTION IF EXISTS ft_ds_admin.calculate_historical_waitlist_not_selected_counts();

CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_waitlist_not_selected_counts(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
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
        COUNT(contact_id_18) AS participant_count,
        CAST(EXTRACT(YEAR FROM NOW()) AS TEXT) AS eoy_indicator
    FROM ft_ds_refined.waitlist_view
    GROUP BY
    chapter_id
    ;
END;
$BODY$;

ALTER FUNCTION ft_ds_admin.calculate_historical_waitlist_not_selected_counts()
    OWNER TO ft_prod_data_warehouse_master_user;

GRANT EXECUTE ON FUNCTION ft_ds_admin.calculate_historical_waitlist_not_selected_counts() TO PUBLIC;

GRANT EXECUTE ON FUNCTION ft_ds_admin.calculate_historical_waitlist_not_selected_counts() TO ft_prod_data_warehouse_master_user;

GRANT EXECUTE ON FUNCTION ft_ds_admin.calculate_historical_waitlist_not_selected_counts() TO limited_writer_role;