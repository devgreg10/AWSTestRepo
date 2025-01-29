CREATE OR REPLACE VIEW ft_ds_refined.current_and_historical_participants_view
AS
SELECT
    *
FROM (
    SELECT
        contact_id,
        first_name,
        last_name,
        program_level,
        birthdate,
        age AS recorded_age,
        --the calculation method here is to minimize differences between recorded age and calculated age on birthdays of Jan 1, which there are dozens of thousands of. We assume that their age is correct, and that the birthdate was extrapolated from that
        EXTRACT(YEAR from AGE(MAKE_DATE(CAST(year AS INTEGER), 12, 31), CAST(birthdate AS TIMESTAMP))) AS calculated_age,
        gender,
        ethnicity,
        grade_level,
        mailing_zip_postal_code,
        primary_contact_email,
        email,
        participation_status,
        legacy_participant_user_id,
        school_name,
        school_name_other,
        additional_trade_name_account_name,
        chapter_affiliation_account_name,
        chapter_id,
        year
    FROM ft_ds_refined.year_end_participant
) historical_participants
UNION
(
    SELECT
        contact_id_18 AS contact_id,
        first_name,
        last_name,
        program_level,
        CAST(birthdate AS DATE) AS birthdate,
        NULL AS recorded_age,
        calculated_age,
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
    FROM ft_ds_refined.active_participants_view
)
;