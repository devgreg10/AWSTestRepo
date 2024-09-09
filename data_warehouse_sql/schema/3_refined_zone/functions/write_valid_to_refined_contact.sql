-- CREATE OR REPLACE PROCEDURE ft_ds_admin.ft_ds_admin.valid_to_refined_sf_contact ()
-- LANGUAGE plpgsql
-- AS $$
-- BEGIN
--     TRUNCATE ft_ds_refined.sf_contact;

--     INSERT INTO ft_ds_refined.sf_contact
--     SELECT
--         *
--     FROM ft_ds_valid.sf_contact
--     ;
-- END;
-- $$;

CREATE OR REPLACE FUNCTION ft_ds_admin.write_valid_to_refined_contact ()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_refined.sf_contact
    SELECT
        *
    FROM ft_ds_valid.sf_contact
    WHERE
        is_deleted = false
    ;
END;
$$;