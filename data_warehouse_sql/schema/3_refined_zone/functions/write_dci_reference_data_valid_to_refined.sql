CREATE OR REPLACE FUNCTION ft_ds_admin.write_dci_reference_data_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.dci_reference_data;

    INSERT INTO ft_ds_refined.dci_reference_data
    SELECT
        *
    FROM ft_ds_valid.dci_reference_data
    ;
END;
$$;