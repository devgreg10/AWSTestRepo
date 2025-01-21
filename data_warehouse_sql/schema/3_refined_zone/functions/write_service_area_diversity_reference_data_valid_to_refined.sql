CREATE OR REPLACE FUNCTION ft_ds_admin.write_service_area_diversity_reference_data_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.service_area_diversity_reference_data;

    INSERT INTO ft_ds_refined.service_area_diversity_reference_data
    SELECT
        *
    FROM ft_ds_valid.service_area_diversity_reference_data
    ;
END;
$$;