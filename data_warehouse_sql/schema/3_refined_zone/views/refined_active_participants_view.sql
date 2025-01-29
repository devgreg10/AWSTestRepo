CREATE OR REPLACE VIEW ft_ds_refined.active_participants_view
AS
SELECT
    contact.contact_id_18,
    chapter_affiliation.account_name as chapter_name,
    contact.chapter_id,
    contact.contact_type,
    --contact.age,
    EXTRACT(YEAR from AGE(NOW(), CAST(contact.birthdate AS TIMESTAMP))) AS calculated_age,
    contact.ethnicity,
    contact.gender,
    contact.grade,
    contact.participation_status,
    contact.mailing_zip_postal_code,
    contact.mailing_street,
    contact.mailing_city,
    contact.mailing_state,
    contact.school_name,
    contact.school_name_other,
    contact.first_name,
    contact.last_name,
    contact.birthdate,
    household.account_name as household_name,
    contact.household_id,
    contact.sf_created_timestamp,
    contact.sf_last_modified_timestamp,
    contact.sf_system_modstamp,
    contact.dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.contact contact
LEFT JOIN ft_ds_refined.account chapter_affiliation
    ON contact.chapter_id = chapter_affiliation.account_id
LEFT JOIN ft_ds_refined.account household
    ON contact.household_id = household.account_id
WHERE
    contact.contact_type LIKE '%Participant%'
    AND contact.participation_status = 'Active'
    AND contact.first_name IS NOT NULL
    AND contact.last_name IS NOT NULL
    AND contact.birthdate IS NOT NULL
    --AND contact.age IS NOT NULL
    AND contact.gender IS NOT NULL
    AND contact.ethnicity IS NOT NULL
    AND contact.household_id IS NOT NULL
    AND contact.mailing_zip_postal_code IS NOT NULL
    AND contact.chapter_id IS NOT NULL
    AND contact.dss_ingestion_timestamp IS NOT NULL
;