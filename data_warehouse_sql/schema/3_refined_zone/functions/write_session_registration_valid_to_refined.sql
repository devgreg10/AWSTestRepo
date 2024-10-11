CREATE OR REPLACE FUNCTION ft_ds_admin.write_session_registration_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.session_registration;

    INSERT INTO ft_ds_refined.session_registration
    SELECT
        *
    FROM ft_ds_valid.sf_session_registration
    WHERE
        is_deleted = false
    ;
END;
$$;