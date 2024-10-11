CREATE OR REPLACE FUNCTION ft_ds_admin.write_year_end_participant_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.year_end_participant;

    INSERT INTO ft_ds_refined.year_end_participant
    SELECT
        *
    FROM ft_ds_valid.sf_year_end_participant
    ;
END;
$$;