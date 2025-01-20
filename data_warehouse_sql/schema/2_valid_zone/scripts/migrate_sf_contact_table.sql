--this script is meant to be run manually and will include several SQL statements that can be executed in a manner determined by the person executing them, with some guidance from the comments left
--it is specifically designed to expand the valid zone sf_contact table to include an program_level field that is available in raw, but was not initially carried over to valid

--create a temp table to store the data as a backup as it is while we drop and recreate the original table with the new column added
CREATE TEMP TABLE temp_valid_sf_contact AS
SELECT
    *
FROM ft_ds_valid.sf_contact;
;

--drop the table so we can add a new column
DROP TABLE ft_ds_valid.sf_contact;

DROP TABLE ft_ds_raw.validation_errors_sf_contact;


--recreate the table with the new column
--copied from /Users/hschuster/FT-decision-support-cloud/data_warehouse_sql/schema/2_valid_zone/tables/valid_sf_contact_table.sql
CREATE TABLE IF NOT EXISTS ft_ds_valid.sf_contact (
    PRIMARY KEY (contact_id_18),
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
    program_level VARCHAR(20),
    is_deleted BOOLEAN,
    sf_created_timestamp TIMESTAMPTZ,
    sf_last_modified_timestamp TIMESTAMPTZ,
    sf_system_modstamp TIMESTAMPTZ,
    dss_ingestion_timestamp TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ft_ds_raw.validation_errors_sf_contact (
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
    program_level TEXT,
    is_deleted TEXT,
    sf_created_timestamp TEXT,
    sf_last_modified_timestamp TEXT,
    sf_system_modstamp TEXT,
    dss_ingestion_timestamp TIMESTAMPTZ,
    required_fields_validated BOOLEAN,
    optional_fields_validated BOOLEAN,
    fixed_in_source BOOLEAN
);

--we are going to do a full refresh of the valid zone, pulling in all the raw data at once since we need program_level values from records that haven't been changed in a while
select ft_ds_admin.write_sf_contact_raw_to_valid('1900-01-01');