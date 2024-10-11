CREATE OR REPLACE FUNCTION ft_ds_admin.write_contact_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.contact;

    INSERT INTO ft_ds_refined.contact
    SELECT
        *
    FROM ft_ds_valid.sf_contact
    WHERE
        is_deleted = false
    ;
END;
$$;