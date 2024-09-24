CREATE OR REPLACE FUNCTION ft_ds_admin.write_valid_to_refined ()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM ft_ds_admin.write_valid_to_refined_contact();
END;
$$;