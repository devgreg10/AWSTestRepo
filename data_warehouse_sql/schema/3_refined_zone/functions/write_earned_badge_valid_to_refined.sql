CREATE OR REPLACE FUNCTION ft_ds_admin.write_earned_badge_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.earned_badge;

    INSERT INTO ft_ds_refined.earned_badge
    SELECT
        *
    FROM ft_ds_valid.sf_earned_badge
    WHERE
        is_deleted = false
    ;
END;
$$;