CREATE OR REPLACE FUNCTION ft_ds_admin.write_listing_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.listing;

    INSERT INTO ft_ds_refined.listing
    SELECT
        *
    FROM ft_ds_valid.sf_listing
    WHERE
        is_deleted = false
    ;
END;
$$;