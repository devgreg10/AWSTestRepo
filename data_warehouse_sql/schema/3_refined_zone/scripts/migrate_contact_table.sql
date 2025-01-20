--this script is meant to be run manually and will include several SQL statements that can be executed in a manner determined by the person executing them, with some guidance from the comments left
--it is specifically designed to expand the contact table in refined to include the program_level field

--create a temp table to store the data as a backup as it is while we drop and recreate the original table with the new column added
CREATE TEMP TABLE temp_refined_contact AS
SELECT
    *
FROM ft_ds_refined.contact
;

--drop the table so we can add a new column
--you will also have to drop the downstream views
DROP TABLE ft_ds_refined.contact;

--recreate the downstream views

--recreate the table with the new column
--copied from /Users/hschuster/FT-decision-support-cloud/data_warehouse_sql/schema/3_refined_zone/tables/refined_contact_table.sql
CREATE TABLE IF NOT EXISTS ft_ds_refined.contact (
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
    school_name VARCHAR(100),
    school_name_other VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    birthdate VARCHAR(100),
    household_id CHAR(18),
    program_level VARCHAR(20),
    is_deleted BOOLEAN,
    sf_created_timestamp TIMESTAMPTZ,
    sf_last_modified_timestamp TIMESTAMPTZ,
    sf_system_modstamp TIMESTAMPTZ,
    dss_ingestion_timestamp TIMESTAMPTZ
);

--we can just call the write_contact_valid_to_refined function to move the data
--the backup we first created exists but will not provide the program_level field that we want
select ft_ds_admin.write_contact_valid_to_refined();