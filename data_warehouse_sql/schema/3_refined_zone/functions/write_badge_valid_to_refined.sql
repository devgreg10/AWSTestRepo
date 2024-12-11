CREATE OR REPLACE FUNCTION ft_ds_admin.write_badge_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.badge;

    INSERT INTO ft_ds_refined.badge
    SELECT
        *
    FROM ft_ds_valid.sf_badge
    WHERE
        is_deleted = false
    ;
END;
$$;