CREATE OR REPLACE VIEW ft_ds_refined.refined_active_participants_view
AS
SELECT
contact_id_18,
chapter_id,
age,
gender,
ethnicity,
grade,
mailing_zip_postal_code,
participation_status,
contact_type,
sf_last_modified_date,
created_date,
dss_last_modified_timestamp
--just ommitted the is_deleted field
FROM ft_ds_refined.sf_contact
WHERE
    participation_status = 'Active'
    AND contact_type LIKE '%Participant%'
    AND age IS NOT NULL
    AND gender IS NOT NULL
    AND gender <> ''
    AND ethnicity IS NOT NULL
    AND ethnicity <> ''
    AND mailing_zip_postal_code IS NOT NULL
    AND mailing_zip_postal_code <> ''
    AND chapter_id IS NOT NULL
    AND chapter_id <> ''
;