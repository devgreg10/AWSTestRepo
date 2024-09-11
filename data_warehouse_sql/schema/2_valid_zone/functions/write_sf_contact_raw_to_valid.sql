
-- based on whole field list
-- CREATE OR REPLACE PROCEDURE ft_ds_admin.raw_to_valid_sf_contact ()
-- LANGUAGE plpgsql
-- AS $$
-- BEGIN
--     TRUNCATE ft_ds_valid.sf_contact;

--     INSERT INTO ft_ds_valid.sf_contact
--     SELECT
--         Chapter_Affiliation__c AS chapter_id,
--         AccountId AS household_id,
--         FirstName AS first_name,
--         LastName AS last_name
--         Birthdate AS birthdate,
--         Age__c AS age,
--         Gender__c AS gender,
--         Ethnicity__c AS ethnicity
--         Grade__c AS grade,
--         MailingStreet AS mailing_street,
--         MailingCity AS mailing_city,
--         MailingState AS mailing_state
--         MailingPostalCode AS mailing_zip_postal_code,
--         Primary_Contact_s_Email__c AS primary_contacts_email
--         Email AS participant_email,
--         Participation_Status__c AS participation_status,
--         School_Name__c AS school_name,
--         School_Name_Other__c AS school_name_other,
--         Contact_Type__c AS contact_type,
--         Emergency_Contact_Email__c AS emergency_contact_email,
--         Emergency_Contact_Name__c AS emergency_contact_name,
--         Emergency_Contact_Number__c AS emergency_contact_number,
--         Primary_Contact_s_Mobile__c AS primary_contacts_mobile,
--         Primary_Contact_s_Name__c AS primary_contacts_name,
--         Program_Level__c AS program_level,
--         snapshot_date
--     FROM ft_ds_raw.sf_contact
--     WHERE
--         Chapter_Affiliation__c NOT IN (
--             '0011R00002oM2hNQAS',
--             '0013600000xOm3cAAC'
--         )
--         AND Chapter_Affiliation__c IS NOT NULL
--         AND Chapter_Affiliation__c <> ''
--         AND FirstName IS NOT NULL
--         AND FirstName <> ''
--         AND LastName IS NOT NULL
--         AND LastName <> ''
--         AND Birthdate IS NOT NULL
--         AND Birthdate <> ''
--         AND Age__c IS NOT NULL
--         AND Age__c <> ''
--         AND Gender__c IS NOT NULL
--         AND Gender__c <> ''
--         AND Ethnicity__c IS NOT NULL
--         AND Ethnicity__c <> ''
--         AND MailingPostalCode IS NOT NULL
--         AND MailingPostalCode <> ''
--         AND Chapter_Affiliation__c IS NOT NULL
--         AND Chapter_Affiliation__c <> ''
--         AND Participation_Status__c = 'Active'
--         AND Contact_Type__c = 'Participant'
--     ;
-- END;
-- $$;

CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_contact_raw_to_valid ()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_valid.sf_contact
    SELECT
    all_values.*
    FROM
        (SELECT
            Id AS contact_id_18,
            Chapter_Affiliation__c AS chapter_id,
            CAST(CAST(Age__c AS NUMERIC) AS INTEGER) AS age,
            Gender__c AS gender,
            Ethnicity__c AS ethnicity,
            Grade__c AS grade,
            MailingPostalCode AS mailing_zip_postal_code,
            Participation_Status__c AS participation_status,
            Contact_Type__c AS contact_type,
            CAST(LastModifiedDate AS TIMESTAMPTZ) AS sf_last_modified_date,
            CAST(CreatedDate AS TIMESTAMPTZ) AS created_date,
            CAST(IsDeleted AS BOOLEAN) AS is_deleted,
            dss_last_modified_timestamp AS dss_last_modified_timestamp
        FROM ft_ds_raw.sf_contact
        WHERE
            --filter out testing chapters
            Chapter_Affiliation__c NOT IN (
                '0011R00002oM2hNQAS',
                '0013600000xOm3cAAC'
            )
        ) all_values
    JOIN
        (SELECT
        Id,
        CAST(MAX(LastModifiedDate) AS TIMESTAMPTZ) AS max_date
        FROM ft_ds_raw.sf_contact
        GROUP BY Id
        ) max_dates
    ON all_values.sf_last_modified_date = max_dates.max_date
    AND all_values.contact_id_18 = max_dates.Id
    ON CONFLICT (contact_id_18) DO UPDATE SET
        chapter_id = EXCLUDED.chapter_id,
        age = EXCLUDED.age,
        gender = EXCLUDED.gender,
        ethnicity = EXCLUDED.ethnicity,
        grade = EXCLUDED.grade,
        mailing_zip_postal_code = EXCLUDED.mailing_zip_postal_code,
        participation_status = EXCLUDED.participation_status,
        contact_type = EXCLUDED.contact_type,
        sf_last_modified_date = EXCLUDED.sf_last_modified_date,
        created_date = EXCLUDED.created_date,
        is_deleted = EXCLUDED.is_deleted,
        dss_last_modified_timestamp = EXCLUDED.dss_last_modified_timestamp
    ;
END;
$$;