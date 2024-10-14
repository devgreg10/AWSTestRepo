CREATE OR REPLACE VIEW ft_ds_refined.active_participants_view
AS
SELECT
    contact_id_18,
    chapter_id,
    contact_type,
    --age,
    ethnicity,
    gender,
    grade,
    participation_status,
    mailing_zip_postal_code,
    mailing_street,
    mailing_city,
    mailing_state,
    school_name,
    school_name_other,
    first_name,
    last_name,
    birthdate,
    household_id,
    sf_created_timestamp,
    sf_last_modified_timestamp,
    sf_system_modstamp,
    dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.contact
WHERE
    contact_type LIKE '%Participant%'
    AND participation_status = 'Active'
    AND first_name IS NOT NULL
    AND last_name IS NOT NULL
    AND birthdate IS NOT NULL
    --AND age IS NOT NULL
    AND gender IS NOT NULL
    AND ethnicity IS NOT NULL
    AND household_id IS NOT NULL
    AND mailing_zip_postal_code IS NOT NULL
    AND chapter_id IS NOT NULL
    AND dss_ingestion_timestamp IS NOT NULL
;