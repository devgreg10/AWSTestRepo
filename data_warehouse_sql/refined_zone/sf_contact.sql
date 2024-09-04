--based on whole field list
-- CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_contact (
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

CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_contact (
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

-- CREATE OR REPLACE PROCEDURE ft_ds_admin.ft_ds_admin.valid_to_refined_sf_contact ()
-- LANGUAGE plpgsql
-- AS $$
-- BEGIN
--     TRUNCATE ft_ds_refined.sf_contact;

--     INSERT INTO ft_ds_refined.sf_contact
--     SELECT
--         *
--     FROM ft_ds_valid.sf_contact
--     ;
-- END;
-- $$;

CREATE OR REPLACE PROCEDURE ft_ds_admin.valid_to_refined_sf_contact ()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.sf_contact;

    INSERT INTO ft_ds_refined.sf_contact
    SELECT
        *
    FROM ft_ds_valid.sf_contact
    ;
END;
$$;