CREATE OR REPLACE FUNCTION ft_ds_admin.write_current_to_year_end_participants()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_refined.year_end_participant
        SELECT
        contact_id_18 AS contact_id,
        first_name,
        last_name,
        NULL AS program_level,
        CAST(birthdate AS DATE) AS birthdate,
        NULL AS age,
        gender,
        ethnicity,
        grade AS grade_level,
        mailing_zip_postal_code,
        NULL AS primary_contact_email,
        NULL AS email,
        participation_status,
        NULL AS legacy_participant_user_id,
        school_name,
        school_name_other,
        NULL AS additional_trade_name_account_name,
        NULL AS chapter_affiliation_account_name,
        chapter_id,
        CAST(date_part('year', CURRENT_DATE) AS NUMERIC) AS year
    FROM ft_ds_refined.active_participants_view;
END;
$$;