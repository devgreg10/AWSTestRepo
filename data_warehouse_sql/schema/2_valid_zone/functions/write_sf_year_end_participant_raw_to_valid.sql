CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_year_end_participant_raw_to_valid ()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN

    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_year_end_participant_raw_to_valid;

    --imports all information as the source data type, in this case, TEXT, which is important so we can do typing validations on it
    CREATE TEMP TABLE IF NOT EXISTS temp_sf_year_end_participant_raw_to_valid AS
    SELECT
        "Contact ID" AS contact_id,
        "First Name" AS first_name,
        "Last Name" AS last_name,
        "Program Level" AS program_level,
        "Birthdate" AS birthdate,
        "Age" AS age,
        "Gender" AS gender,
        "Ethnicity" AS ethnicity,
        "Grade Level" AS grade_level,
        "Mailing Zip/Postal Code" AS mailing_zip_postal_code,
        "Primary Contact's Email" AS primary_contact_email,
        "Email" AS email,
        "Participation Status" AS participation_status,
        "Legacy Participant User Id" AS legacy_participant_user_id,
        "School Name" AS school_name,
        "School Name Other" AS school_name_other,
        "Additional Trade Name: Account Name" AS additional_trade_name_account_name,
        "Chapter Affiliation: Account Name" AS chapter_affiliation_account_name,
        "Chapter ID" AS chapter_id,
        "Year" AS year,
        TRUE AS required_fields_validated,
        TRUE AS optional_fields_validated
    FROM ft_ds_raw.sf_year_end_participant
    ;

    --this statement updates the required_fields_validated flag
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    --it is structured this way because if the required fields do not meet data quality, then they are not passed to valid and therefore do not need to be transformed further. Therefore, only the required_fields_validated flag needs to be updated.
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    required_fields_validated = FALSE
    WHERE
    --contact_id
        contact_id IS NULL
        OR contact_id = ''
    --year
        OR year IS NULL
        OR NOT (SELECT ft_ds_admin.is_coercable_to_numeric(year))
    ;

    --these statements update the optional_fields_validated flag and swap invalid values to NULL
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    -- first_name
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    first_name = NULL
    WHERE
        first_name IS NULL
        OR first_name = ''
    ;
    -- last_name
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    last_name = NULL
    WHERE
        last_name IS NULL
        OR last_name = ''
    ; 
    -- program_level
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    program_level = NULL
    WHERE
        program_level IS NULL
        OR program_level = ''
    ; 
    -- birthdate
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    birthdate = NULL
    WHERE
        birthdate IS NULL
        OR NOT (SELECT ft_ds_admin.is_coercable_to_timestamptz(birthdate))
        OR birthdate = ''
    ;
    -- age
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    age = NULL
    WHERE
        age IS NULL
        OR NOT (SELECT ft_ds_admin.is_coercable_to_numeric(age))
        
        OR age = ''
    ;
    -- gender
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    gender = NULL
    WHERE
        gender IS NULL
        OR gender = ''
    ;
    -- ethnicity
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    ethnicity = NULL
    WHERE
        ethnicity IS NULL
        OR ethnicity = ''
    ;
    -- grade_level
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    grade_level = NULL
    WHERE
        grade_level IS NULL
        OR grade_level = ''
    ;
    -- mailing_zip_postal_code
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    mailing_zip_postal_code = NULL
    WHERE
        mailing_zip_postal_code IS NULL
        OR mailing_zip_postal_code = ''
    ;
    -- primary_contact_email
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    primary_contact_email = NULL
    WHERE
        primary_contact_email IS NULL
        OR primary_contact_email = ''
    ;
    -- email
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    email = NULL
    WHERE
        email IS NULL
        OR email = ''
    ;
    -- participation_status
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    participation_status = NULL
    WHERE
        participation_status IS NULL
        OR participation_status = ''
    ;
    -- legacy_participant_user_id
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    legacy_participant_user_id = NULL
    WHERE
        legacy_participant_user_id IS NULL
        OR legacy_participant_user_id = ''
    ;
    -- school_name
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    school_name = NULL
    WHERE
        school_name IS NULL
        OR school_name = ''
    ;
    -- school_name_other
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    school_name_other = NULL
    WHERE
        school_name_other IS NULL
        OR school_name_other = ''
    ;
    -- additional_trade_name_account_name
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    additional_trade_name_account_name = NULL
    WHERE
        additional_trade_name_account_name IS NULL
        OR additional_trade_name_account_name = ''
    ;
    -- chapter_affiliation_account_name
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_affiliation_account_name = NULL
    WHERE
        chapter_affiliation_account_name IS NULL
        OR chapter_affiliation_account_name = ''
    ;
    -- chapter_id
    UPDATE temp_sf_year_end_participant_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_id = NULL
    WHERE
        chapter_id IS NULL
        OR chapter_id = ''
    ;

    --copy all records where required_fields_validated = FALSE OR optional_fields_validated = FALSE to a permanent errored table
    --this can be reported on later to let data source owners fix the data upstream, where it will be re-ingested and fixed throughout all zones of the data warehouse
    --the fixed_in_source field can be updated when a data owner fixes the record in the source.
    INSERT INTO ft_ds_raw.validation_errors_sf_year_end_participant
    SELECT
        contact_id,
        first_name,
        last_name,
        program_level,
        birthdate,
        age,
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
        year,
        required_fields_validated,
        optional_fields_validated,
        FALSE AS fixed_in_source
    FROM temp_sf_year_end_participant_raw_to_valid
    WHERE
        required_fields_validated = FALSE
        OR optional_fields_validated = FALSE
    ;

    --this statement then cleans the data to get the data types correct
    INSERT INTO ft_ds_valid.sf_year_end_participant
    SELECT
        contact_id,
        first_name,
        last_name,
        program_level,
        CAST(birthdate AS DATE )AS birthdate,
        CAST(CAST(age AS NUMERIC) AS INTEGER) AS age,
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
        CAST(year AS NUMERIC) AS year
    FROM temp_sf_year_end_participant_raw_to_valid
    WHERE
        required_fields_validated = TRUE
    ON CONFLICT (contact_id, year) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        program_level = EXCLUDED.program_level,
        birthdate = EXCLUDED.birthdate,
        age = EXCLUDED.age,
        gender = EXCLUDED.gender,
        ethnicity = EXCLUDED.ethnicity,
        grade_level = EXCLUDED.grade_level,
        mailing_zip_postal_code = EXCLUDED.mailing_zip_postal_code,
        primary_contact_email = EXCLUDED.primary_contact_email,
        email = EXCLUDED.email,
        participation_status = EXCLUDED.participation_status,
        legacy_participant_user_id = EXCLUDED.legacy_participant_user_id,
        school_name = EXCLUDED.school_name,
        school_name_other = EXCLUDED.school_name_other,
        additional_trade_name_account_name = EXCLUDED.additional_trade_name_account_name,
        chapter_affiliation_account_name = EXCLUDED.chapter_affiliation_account_name,
        chapter_id = EXCLUDED.chapter_id,
        year = EXCLUDED.year
    ;
END;
$$;