CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_contact_raw_to_valid (load_threshold_timestamp TIMESTAMPTZ DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS temp_sf_contact_raw_to_valid (
        --still import as all text because we want to be able to analyze it for if it will make it to valid based on dtype, and we can assume it will fit in text because that's how it already exists in raw
        contact_id_18 TEXT,
        chapter_id TEXT,
        contact_type TEXT,
        age TEXT,
        ethnicity TEXT,
        gender TEXT,
        grade TEXT,
        participation_status TEXT,
        mailing_zip_postal_code TEXT,
        mailing_street TEXT,
        mailing_city TEXT,
        mailing_state TEXT,
        school_name TEXT,
        school_name_other TEXT,
        first_name TEXT,
        last_name TEXT,
        birthdate TEXT,
        household_id TEXT,
        is_deleted TEXT,
        sf_created_timestamp TEXT,
        sf_last_modified_timestamp TEXT,
        sf_system_modstamp TEXT,
        --dss_ingestion_timestamp can be assumed to still be timestamp because that's how it exists in raw
        dss_ingestion_timestamp TIMESTAMPTZ,
        required_fields_validated BOOLEAN,
        optional_fields_validated BOOLEAN
    );

    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    TRUNCATE temp_sf_contact_raw_to_valid;

    --this statement places all of the most recently uploaded records into the temp table
    INSERT INTO temp_sf_contact_raw_to_valid
    SELECT
        Id AS contact_id_18,
        Chapter_Affiliation__c AS chapter_id,
        Contact_Type__c AS contact_type,
        Age__c AS age,
        Ethnicity__c AS ethnicity,
        Gender__c AS gender,
        Grade__c AS grade,
        Participation_Status__c AS participation_status,
        MailingPostalCode AS mailing_zip_postal_code,
        MailingStreet AS mailing_street,
        MailingCity AS mailing_city,
        MailingState AS mailing_state,
        School_Name__c AS school_name,
        School_Name_Other__c AS school_name_other,
        FirstName AS first_name,
        LastName AS last_name,
        Birthdate AS birthdate,
        AccountId AS household_id,
        IsDeleted AS is_deleted,
        CreatedDate AS sf_created_timestamp,
        LastModifiedDate AS sf_last_modified_timestamp,
        SystemModstamp AS sf_system_modstamp,
        dss_ingestion_timestamp,
        TRUE AS required_fields_validated,
        TRUE AS optional_fields_validated
    FROM ft_ds_raw.sf_contact
    WHERE
        dss_ingestion_timestamp >
        CASE
            WHEN load_threshold_timestamp IS NULL THEN (SELECT MAX(execution_timestamp) from ft_ds_valid.raw_to_valid_execution_log)
            ELSE load_threshold_timestamp
        END
    ;

    --this statement updates the required_fields_validated flag
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    --it is structured this way because if the required fields do not meet data quality, then they are not passed to valid and therefore do not need to be transformed further. Therefore, only the required_fields_validated flag needs to be updated.
    UPDATE temp_sf_contact_raw_to_valid
    SET
    required_fields_validated = FALSE
    WHERE
    --contact_id_18
        contact_id_18 IS NULL
        AND LENGTH(contact_id_18) <> 18
        AND contact_id_18 = ''
    --sf_system_modstamp
        AND sf_system_modstamp IS NULL
        AND NOT (SELECT ft_ds_admin.is_coercable_to_numeric(sf_system_modstamp))
    ;

    --these statements update the optional_fields_validated flag and swap invalid values to NULL
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    -- chapter_id
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_id = NULL
    WHERE
        chapter_id IS NULL
        OR LENGTH(chapter_id) <> 18
        OR chapter_id = ''
    ;
    -- contact_type
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    contact_type = NULL
    WHERE
        contact_type IS NULL
        --this is a multi-picklist, so the permutations and combinations are too much to list here
        OR chapter_id = ''
    ; 
    -- age
    -- ethnicity
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    ethnicity = NULL
    WHERE
        ethnicity IS NULL
        OR ethnicity NOT IN ('Asian','Black or African-American','Latino or Hispanic','Multi-Racial','Native American or Native Alaskan','Pacific Islander','White or Caucasian','Indigenous','South Asian','Chinese','Black','Filipino','Latin American','Southeast Asian','Middle Eastern','Korean','Japanese','Other','Prefer not to respond')
        OR ethnicity = ''
    ; 
    -- gender
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    gender = NULL
    WHERE
        gender IS NULL
        OR gender NOT IN ('Female','Male','Non-Binary','The options listed do not reflect me','Prefer not to respond')
        OR gender = ''
    ;
    -- grade
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    grade = NULL
    WHERE
        grade IS NULL
        OR grade NOT IN ('Pre School','Kindergarten','1','2','3','4','5','6','7','8','9','10','11','12','13','Home School','Graduate')
        OR grade = ''
    ;
    -- participation_status
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    participation_status = NULL
    WHERE
        participation_status IS NULL
        OR participation_status NOT IN ('Prospect','Active','Previous','Alumni')
        OR participation_status = ''
    ;
    -- mailing_zip_postal_code
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    mailing_zip_postal_code = NULL
    WHERE
        mailing_zip_postal_code IS NULL
        OR mailing_zip_postal_code !~ '^\d{5}(-\d{4})?$'
        OR mailing_zip_postal_code = ''
    ;
    -- mailing_street
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    mailing_street = NULL
    WHERE
        mailing_street IS NULL
        OR mailing_street = ''
    ;
    -- mailing_city
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    mailing_city = NULL
    WHERE
        mailing_city IS NULL
        OR mailing_city = ''
    ;
    -- mailing_state
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    mailing_state = NULL
    WHERE
        mailing_state IS NULL
        OR mailing_state = ''
    ;
    -- school_name
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    school_name = NULL
    WHERE
        school_name IS NULL
        OR school_name = ''
    ;
    -- school_name_other
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    school_name_other = NULL
    WHERE
        school_name_other IS NULL
        OR school_name_other = ''
    ;
    -- first_name
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    first_name = NULL
    WHERE
        first_name IS NULL
        OR first_name = ''
    ;
    -- last_name
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    last_name = NULL
    WHERE
        last_name IS NULL
        OR last_name = ''
    ;
    -- birthdate
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    birthdate = NULL
    WHERE
        birthdate IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(birthdate))
        OR birthdate = ''
    ;
    -- household_id
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    household_id = NULL
    WHERE
        household_id IS NULL
        OR LENGTH(household_id) <> 18
        OR household_id = ''
    ;
    -- is_deleted
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_deleted = NULL
    WHERE
        is_deleted IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(is_deleted))
        OR is_deleted = ''
    ;
    -- sf_created_timestamp
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_created_timestamp = NULL
    WHERE
        sf_created_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_created_timestamp))
        OR sf_created_timestamp = ''
    ;
    -- sf_last_modified_timestamp
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_timestamp = NULL
    WHERE
        sf_last_modified_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_last_modified_timestamp))
        OR sf_last_modified_timestamp = ''
    ;
    -- dss_ingestion_timestamp
    UPDATE temp_sf_contact_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    dss_ingestion_timestamp = NULL
    WHERE
        -- dont include the coerced_to_timestamp() check because this field is already a timestamp
        dss_ingestion_timestamp IS NULL
    ;

    --copy all records where required_fields_validated = FALSE OR optional_fields_validated = FALSE to a permanent errored table
    --this can be reported on later to let data source owners fix the data upstream, where it will be re-ingested and fixed throughout all zones of the data warehouse
    --the fixed_in_source field can be updated when a data owner fixes the record in the source.
    INSERT INTO ft_ds_raw.validation_errors_sf_contact
    SELECT
        contact_id_18,
        chapter_id,
        contact_type,
        age,
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
        is_deleted,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp,
        dss_ingestion_timestamp,
        required_fields_validated,
        optional_fields_validated,
        FALSE AS fixed_in_source
    FROM temp_sf_contact_raw_to_valid
    WHERE
        required_fields_validated = FALSE
        OR optional_fields_validated = FALSE
    ;

    --now that we've flagged the valid data and set optional invalid fields to NULL, we can cast the values to their valid types
    CREATE TABLE IF NOT EXISTS temp_sf_contact_raw_to_valid_validated (
        contact_id_18 CHAR(18),
        chapter_id CHAR(18),
        contact_type VARCHAR(100),
        --age INTEGER,
        ethnicity VARCHAR(100),
        gender VARCHAR(100),
        grade VARCHAR(100),
        participation_status VARCHAR(100),
        mailing_zip_postal_code VARCHAR(20),
        mailing_street VARCHAR(255),
        mailing_city VARCHAR(100),
        mailing_state VARCHAR(100),
        school_name VARCHAR(255),
        school_name_other VARCHAR(255),
        first_name VARCHAR(40),
        last_name VARCHAR(80),
        birthdate VARCHAR(100),
        household_id CHAR(18),
        is_deleted BOOLEAN,
        sf_created_timestamp TIMESTAMPTZ,
        sf_last_modified_timestamp TIMESTAMPTZ,
        sf_system_modstamp TIMESTAMPTZ,
        dss_ingestion_timestamp TIMESTAMPTZ
    );

    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    TRUNCATE temp_sf_contact_raw_to_valid_validated;

    --this statement then cleans the data to get the data types correct
    INSERT INTO temp_sf_contact_raw_to_valid_validated
    SELECT
        contact_id_18,
        chapter_id,
        contact_type,
        --CAST(CAST(age AS NUMERIC) AS INTEGER) AS age,
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
        CAST(is_deleted AS BOOLEAN) AS is_deleted,
        CAST(sf_created_timestamp AS TIMESTAMPTZ) AS sf_created_timestamp,
        CAST(sf_last_modified_timestamp AS TIMESTAMPTZ) AS sf_last_modified_timestamp,
        CAST(sf_system_modstamp AS TIMESTAMPTZ) AS sf_system_modstamp,
        dss_ingestion_timestamp
    FROM temp_sf_contact_raw_to_valid
    WHERE
        required_fields_validated = TRUE
    ;

    --this query gets the population correct for transitioning from raw to valid. It only includes records:
    -- that were inserted into the raw zone since the last raw->valid run
    -- that dont belong to testing chapters
    -- that are unique to each sf_contact_id_18 value (no dups)
    INSERT INTO ft_ds_valid.sf_contact
    SELECT
        all_values_but_dss_ingestion.*,
        dss_ingestion.dss_ingestion_timestamp
    FROM
    (SELECT
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
        is_deleted,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp
    FROM temp_sf_contact_raw_to_valid_validated
    WHERE
        chapter_id NOT IN (
            '0011R00002oM2hNQAS',
            '0013600000xOm3cAAC'
        )
    GROUP BY
        --this group by clause exists to eliminate duplicates since multiple records with the same Id and system_modstamp can exist
        --it is every field going into ft_ds_valid.sf_contact except dss_ingestion_timestamp
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
        is_deleted,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp
    ) all_values_but_dss_ingestion
    JOIN
    (SELECT
        contact_id_18,
        MAX(sf_system_modstamp) as max_date
    FROM temp_sf_contact_raw_to_valid_validated
    GROUP BY
        contact_id_18
    ) max_dates
    ON all_values_but_dss_ingestion.contact_id_18 = max_dates.contact_id_18
    AND all_values_but_dss_ingestion.sf_system_modstamp = max_dates.max_date
    JOIN
    (SELECT
        contact_id_18,
        MAX(dss_ingestion_timestamp) as dss_ingestion_timestamp
    FROM temp_sf_contact_raw_to_valid_validated
    GROUP BY
        contact_id_18
    )dss_ingestion
    ON all_values_but_dss_ingestion.contact_id_18 = dss_ingestion.contact_id_18
    ON CONFLICT (contact_id_18) DO UPDATE SET
        contact_id_18 = EXCLUDED.contact_id_18,
        chapter_id = EXCLUDED.chapter_id,
        contact_type = EXCLUDED.contact_type,
        --age = EXCLUDED.age,
        ethnicity = EXCLUDED.ethnicity,
        gender = EXCLUDED.gender,
        grade = EXCLUDED.grade,
        participation_status = EXCLUDED.participation_status,
        mailing_zip_postal_code = EXCLUDED.mailing_zip_postal_code,
        mailing_street = EXCLUDED.mailing_street,
        mailing_city = EXCLUDED.mailing_city,
        mailing_state = EXCLUDED.mailing_state,
        school_name = EXCLUDED.school_name,
        school_name_other = EXCLUDED.school_name_other,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        birthdate = EXCLUDED.birthdate,
        household_id = EXCLUDED.household_id,
        is_deleted = EXCLUDED.is_deleted,
        sf_created_timestamp = EXCLUDED.sf_created_timestamp,
        sf_last_modified_timestamp = EXCLUDED.sf_last_modified_timestamp,
        sf_system_modstamp = EXCLUDED.sf_system_modstamp,
        dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp
    ;

    INSERT INTO ft_ds_valid.raw_to_valid_execution_log
    SELECT
    MAX(dss_ingestion_timestamp)
    FROM ft_ds_valid.sf_contact
    ;
END;
$$;