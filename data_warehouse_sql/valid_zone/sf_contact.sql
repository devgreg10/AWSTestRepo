--based on whole field list
-- CREATE TABLE IF NOT EXISTS ft_ds_valid.sf_contact (
--     contact_id_18,
--     chapter_id,
--     household_id,
--     first_name,
--     last_name,
--     birthdate,
--     age,
--     gender,
--     ethnicity,
--     grade,
--     mailing_street,
--     mailing_city,
--     mailing_state,
--     mailing_zip_postal_code,
--     primary_contacts_email,
--     participant_email,
--     participation_status,
--     school_name,
--     school_name_other,
--     contact_type,
--     emergency_contact_email,
--     emergency_contact_name,
--     emergency_contact_number,
--     primary_contacts_mobile,
--     primary_contacts_name,
--     program_level,
--     snapshot_date
-- );

CREATE TABLE IF NOT EXISTS ft_ds_valid.sf_contact (
    PRIMARY KEY (snapshot_date, contact_id_18),
	snapshot_date TIMESTAMPTZ,
    contact_id_18 CHAR(18),
    chapter_id CHAR(18),
    age INTEGER,
    gender VARCHAR(100),
    ethnicity VARCHAR(100),
    grade VARCHAR(100),
    mailing_zip_postal_code VARCHAR(20),
    participation_status VARCHAR(100),
    contact_type VARCHAR(100)
);

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

CREATE OR REPLACE PROCEDURE ft_ds_admin.raw_to_valid_sf_contact ()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_valid.sf_contact;

    INSERT INTO ft_ds_valid.sf_contact
    SELECT
        snapshot_date AS snapshot_date,
        Id AS contact_id_18,
        Chapter_Affiliation__c AS chapter_id,
        CAST(CAST(Age__c AS NUMERIC) AS INTEGER) AS age,
        Gender__c AS gender,
        Ethnicity__c AS ethnicity,
        Grade__c AS grade,
        MailingPostalCode AS mailing_zip_postal_code,
        Participation_Status__c AS participation_status,
        Contact_Type__c AS contact_type
    FROM ft_ds_raw.sf_contact
    WHERE
        Chapter_Affiliation__c NOT IN (
            '0011R00002oM2hNQAS',
            '0013600000xOm3cAAC'
        )
        AND Chapter_Affiliation__c IS NOT NULL
        AND Chapter_Affiliation__c <> ''
        AND Age__c IS NOT NULL
        AND Age__c <> ''
        AND Gender__c IS NOT NULL
        AND Gender__c <> ''
        AND Ethnicity__c IS NOT NULL
        AND Ethnicity__c <> ''
        AND MailingPostalCode IS NOT NULL
        AND MailingPostalCode <> ''
        AND Chapter_Affiliation__c IS NOT NULL
        AND Chapter_Affiliation__c <> ''
        AND Participation_Status__c = 'Active'
        AND Contact_Type__c = 'Participant'
        AND snapshot_date = (
            SELECT MAX(snapshot_date) FROM ft_ds_raw.sf_contact
        )
    ;
END;
$$;