CREATE OR REPLACE VIEW ft_ds_refined.current_and_historical_participants_view
AS
SELECT
    *
FROM (
    SELECT
        *
    FROM ft_ds_refined.year_end_participant
) historical_participants
UNION
(
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
    FROM ft_ds_refined.active_participants_view
)
;