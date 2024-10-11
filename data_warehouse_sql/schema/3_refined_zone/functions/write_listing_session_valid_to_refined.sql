CREATE OR REPLACE FUNCTION ft_ds_admin.write_listing_session_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.listing_session;

    INSERT INTO ft_ds_refined.listing_session
    SELECT
        *
    FROM ft_ds_valid.sf_listing_session
    WHERE
        is_deleted = false
    ;
END;
$$;