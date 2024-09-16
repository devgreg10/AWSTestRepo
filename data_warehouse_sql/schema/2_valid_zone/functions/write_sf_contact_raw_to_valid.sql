
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

CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_contact_raw_to_valid (load_full_data_flag BOOLEAN)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS temp_sf_contact_raw_to_valid (
        PRIMARY KEY (contact_id_18),
        contact_id_18 TEXT,
        chapter_id TEXT,
        age TEXT,
        gender TEXT,
        ethnicity TEXT,
        grade TEXT,
        mailing_zip_postal_code TEXT,
        participation_status TEXT,
        contact_type TEXT,
        sf_last_modified_date TEXT,
        created_date TEXT,
        is_deleted TEXT,
        dss_last_modified_timestamp TIMESTAMPTZ
    );

    TRUNCATE temp_sf_contact_raw_to_valid;

    --this query gets the population correct for transitioning from raw to valid. It filters out records:
    -- that should have been inserted last run
    -- that belong to testing chapters
    -- that accidentally have duplicates
    INSERT INTO temp_sf_contact_raw_to_valid
    SELECT
    all_values.*
    FROM
        (SELECT
            Id,
            MAX(LastModifiedDate) AS max_date
        FROM ft_ds_raw.sf_contact
        GROUP BY Id
        ) max_dates
    JOIN
        (SELECT
            Id AS contact_id_18,
            Chapter_Affiliation__c AS chapter_id,
            Age__c AS age,
            Gender__c AS gender,
            Ethnicity__c AS ethnicity,
            Grade__c AS grade,
            MailingPostalCode AS mailing_zip_postal_code,
            Participation_Status__c AS participation_status,
            Contact_Type__c AS contact_type,
            max(LastModifiedDate) AS sf_last_modified_date,
            CreatedDate AS created_date,
            IsDeleted AS is_deleted,
            dss_last_modified_timestamp
        FROM ft_ds_raw.sf_contact
        WHERE
            Chapter_Affiliation__c NOT IN (
                '0011R00002oM2hNQAS',
                '0013600000xOm3cAAC'
            )
            AND dss_last_modified_timestamp >
            (SELECT CASE
                WHEN load_full_data_flag THEN '1970-01-01T00:00:00.000Z'
                ELSE (SELECT MAX(execution_timestamp) FROM ft_ds_valid.raw_to_valid_execution_log)
            END)
        GROUP BY
            contact_id_18,
            chapter_id,
            age,
            gender,
            ethnicity,
            grade,
            mailing_zip_postal_code,
            participation_status,
            contact_type,
            created_date,
            is_deleted,
            dss_last_modified_timestamp
        ) all_values
    ON all_values.sf_last_modified_date = max_dates.max_date
    AND all_values.contact_id_18 = max_dates.id
    ;

    --this statement then cleans the data to get the values and dtypes correct
    INSERT INTO ft_ds_valid.sf_contact
    SELECT
        contact_id_18,
        chapter_id,
        CASE
            WHEN NULLIF(age, '') IS NULL THEN NULL
            ELSE CAST(CAST(age AS NUMERIC) AS INTEGER)
        END AS age,
        CASE
            WHEN gender LIKE 'INVALID%' THEN NULL
            ELSE gender
        END AS gender,
        ethnicity,
        grade,
        mailing_zip_postal_code,
        participation_status,
        contact_type,
        CAST(sf_last_modified_date AS TIMESTAMPTZ) AS sf_last_modified_date,
        CAST(created_date AS TIMESTAMPTZ) AS created_date,
        CAST(is_deleted AS BOOLEAN) AS is_deleted,
        dss_last_modified_timestamp AS dss_last_modified_timestamp
    FROM temp_sf_contact_raw_to_valid
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

    INSERT INTO ft_ds_valid.raw_to_valid_execution_log
    SELECT
    MAX(dss_last_modified_timestamp)
    FROM ft_ds_valid.sf_contact
    ;
END;
$$;