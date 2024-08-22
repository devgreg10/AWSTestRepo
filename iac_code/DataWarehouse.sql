CREATE SCHEMA IF NOT EXISTS ft_ds_raw;
CREATE SCHEMA IF NOT EXISTS ft_ds_valid;
CREATE SCHEMA IF NOT EXISTS ft_ds_refined;
CREATE SCHEMA IF NOT EXISTS ft_ds_admin;

--based on whole field list
-- CREATE TABLE IF NOT EXISTS ft_ds_raw.sf_contact (
--     IsDeleted,
--     MasterRecordId,
--     Salutation,
--     Name,
--     OtherStreet,
--     OtherCity,
--     OtherState,
--     OtherPostalCode,
--     OtherCountry,
--     OtherLatitude,
--     OtherLongitude,
--     OtherGeocodeAccuracy,
--     OtherAddress,
--     MailingCountry,
--     MailingLatitude,
--     MailingLongitude,
--     MailingGeocodeAccuracy,
--     MailingAddress,
--     Phone,
--     Fax,
--     MobilePhone,
--     HomePhone,
--     OtherPhone,
--     AssistantPhone,
--     ReportsToId,
--     Title,
--     Department,
--     AssistantName,
--     LeadSource,
--     Description,
--     OwnerId,
--     HasOptedOutOfEmail,
--     HasOptedOutOfFax,
--     DoNotCall,
--     CreatedDate,
--     CreatedById,
--     LastModifiedDate,
--     LastModifiedById,
--     SystemModstamp,
--     LastActivityDate,
--     LastCURequestDate,
--     LastCUUpdateDate,
--     LastViewedDate,
--     LastReferencedDate,
--     EmailBouncedReason,
--     EmailBouncedDate,
--     IsEmailBounced,
--     PhotoUrl,
--     Jigsaw,
--     JigsawContactId,
--     IndividualId,
--     ChapterID__c,
--     Override__c,
--     Active__c,
--     OwnerID__c,
--     Trigger_Time__c,
--     Contact_Type_Text__c,
--     Military__c,
--     Region__c,
--     ChapterID_CONTACT__c,
--     Additional_Information__c,
--     Area_of_Interest__c,
--     Attended_Coach_Advanced_Training__c,
--     Birthdate_Map__c,
--     Branded_Program_Location__c,
--     Chapter_Title__c,
--     First_Tee_Employment_Type__c,
--     Coach_Fee_Amount_Paid__c,
--     Coach_Fee_Paid_Date__c,
--     Coach_Inactive_Date__c,
--     Coach_Level__c,
--     Golf_Level__c,
--     Inactive__c,
--     Legacy_ID__c,
--     School_City__c,
--     School_State__c,
--     Transitioned__c,
--     Player_Coach_Training__c,
--     Docebo_Is_Coach__c,
--     Request_Advanced_Training__c,
--     rh2__Currency_Test__c,
--     rh2__Describe__c,
--     rh2__Formula_Test__c,
--     rh2__Integer_Test__c,
--     Participant_Level_Accomplishments__c,
--     Count_Contact__c,
--     Coach_Membership_Fee_Due__c,
--     Coach_Membership_Fee_Paid__c,
--     Coach_Program_Start_Date__c,
--     Coach_Qualified_For__c,
--     Coach_Retention_Status_Changed_Date__c,
--     Contact_Type_Helper__c,
--     Company__c,
--     Curriculum_Session_Registration_Date__c,
--     Custom_Created_Date__c,
--     Eligible_Participants__c,
--     Executive_Director_Email__c,
--     General_Availability_Days_of_the_Week__c,
--     General_Availability_Seasons__c,
--     Golf_Course_Affiliation__c,
--     Home_Phone_Map__c,
--     Hours_of_Training_Attended__c,
--     How_did_you_hear_about_us__c,
--     Is_Coach__c,
--     Lead_Coach_Total_Hours__c,
--     Allergies__c,
--     Contact_Inactive_Date__c,
--     County__c,
--     Dietary_Restrictions__c,
--     Disabilities__c,
--     Request_ACT_Training__c,
--     Emergency_Contact__c,
--     Lead_Coach_Total_Sessions__c,
--     Lead_Source_Other__c,
--     Lifetime_Member__c,
--     Listing__c,
--     Mailing_Address_Company__c,
--     Main_Program_Location__c,
--     Master_Coach_Recognition_Year__c,
--     Other_Address_Company__c,
--     Other_Golf_Certifications_Description__c,
--     Other_Qualifications__c,
--     Primary_Contact_s_Mobile_Phone_Provider__c,
--     Program_Director_Primary_Email__c,
--     Program_Director_Secondary_Email__c,
--     Progressed_Participants__c,
--     FMP_ID__c,
--     Historic_Health_Info__c,
--     Legacy_Participant_User_Id__c,
--     MRM_ID__c,
--     Membership_Created_Date__c,
--     Membership_Status__c,
--     Membership_Valid_From__c,
--     Membership_Valid_To__c,
--     Middle_Name__c,
--     Military_Affiliation__c,
--     Military_Base_Installation__c,
--     Military_Branch__c,
--     Mobile_Phone_Provider__c,
--     Nickname__c,
--     Preferred_Contact_Method__c,
--     Reggie_Id__c,
--     Reggie_Participant_Id__c,
--     Relationship_To_Participants__c,
--     Secondary_Email__c,
--     USGA_Handicap__c,
--     Work_Phone__c,
--     Progression_Status_Changed_Date__c,
--     Progression_Status__c,
--     Reason_For_Leaving__c,
--     Recognized_The_First_Tee_Coach_Year__c,
--     Retained_Coach_Count__c,
--     Retention_Status__c,
--     Secondary_Contact_s_Email__c,
--     Serve_as_National_Trainer__c,
--     Served_as_One_Day_Observer__c,
--     Session_Transfer_From__c,
--     Session_Transfer_To__c,
--     Status__c,
--     Support_Coach_Total_Coaching_Hours__c,
--     Support_Coach_Total_Sessions__c,
--     Title__c,
--     Total_Coaching_Hours1__c,
--     Total_Coaching_Hours__c,
--     Total_Listing_Sessions_Delivered__c,
--     Total_Sessions_Coach_Assigned__c,
--     Track_Program_Level__c,
--     Participant_Hours__c,
--     Parent_Contact__c,
--     Litmos__Access_Level__c,
--     Litmos__Completed_Percentage__c,
--     Litmos__Courses_Assigned__c,
--     Litmos__Courses_Completed__c,
--     Litmos__Deactivate_From_Litmos__c,
--     Litmos__Full_Name__c,
--     Litmos__Languages__c,
--     Litmos__Level__c,
--     Litmos__LitmosID__c,
--     Litmos__Litmos_Activated__c,
--     Litmos__Litmos_Login_Access__c,
--     Litmos__Litmos_UserId__c,
--     Litmos__OriginalId__c,
--     Litmos__Sync_Litmos__c,
--     Litmos__Total_Sum_Percentages__c,
--     Litmos__User_Id__c,
--     Litmos__p_Completed_Percentage__c,
--     Executive_Updates__c,
--     SafeSport_Compliance_not_Required__c,
--     Student_Notes__c,
--     Language__c,
--     FT_App_Pilot__c,
--     International_Chapter__c,
--     FYI__c,
--     Press_Clippings__c,
--     Market_Size__c,
--     Territory__c,
--     Active_Chapter__c,
--     Sports_Engine_ID__c,
--     MDR__c,
--     Merged_Email__c,
--     MDR_NID__c,
--     Ethnicity_Location__c,
--     Background_Check_Exp_bucket__c,
--     Background_Check_Expiration_Date__c,
--     Background_Check_Passed__c,
--     Background_Check_Status__c,
--     Compliant__c,
--     Days_until_Background_Check_Expires__c,
--     Days_until_SafeSport_Training_Expires__c,
--     Number_of_Safety_Records__c,
--     SafeSport_Training_Completed__c,
--     SafeSport_Training_Exp_bucket__c,
--     SafeSport_Training_Expiration_Date__c,
--     SafeSport_Training_Status__c,
--     Contact_Name__c,
--     Training_Type__c,
--     Docebo_Email__c,
--     Docebo_User_ID__c,
--     Docebo_Is_Community_Staff__c,
--     Docebo_Is_Parent__c,
--     Docebo_Is_Participant__c,
--     Docebo_Is_School_Staff__c,
--     Docebo_Username__c,
--     Salesforce_Username__c,
--     Docebo_Pilot_User__c,
--     Docebo_Is_HQ_Staff__c,
--     Docebo_Disconnect__c,
--     Docebo_Account_Active__c,
--     Master_Coach__c,
--     Primary_Contact_Home_Phone__c,
--     Impact_Today_Board_Members__c,
--     Impact_Today_Coaches__c,
--     Impact_Today_Leaders__c,
--     NCCP__c,
--     Pursuing_Ace__c,
--     Docebo_Is_Junior_Coach__c,
--     Respect_In_Sport_Certification__c,
--     Canva_Admin__c,
--     Website_Admin__c,
--     Contact_ID__c,
--     Id,
--     Impact_Today__c,
--     Docebo_Username_Override__c,
--     Partner_Org_Joined_Date__c,
--     CASESAFEID__c,
--     Chapter_Affiliation__c,
--     AccountId,
--     FirstName,
--     LastName,
--     Birthdate,
--     Age__c,
--     Gender__c,
--     Ethnicity__c,
--     Grade__c,
--     MailingStreet,
--     MailingCity,
--     MailingState,
--     MailingPostalCode,
--     Primary_Contact_s_Email__c,
--     Email,
--     Participation_Status__c,
--     School_Name__c,CONTACT_ID_18
--     School_Name_Other__c,
--     Contact_Type__c,
--     Emergency_Contact_Email__c,
--     Emergency_Contact_Name__c,
--     Emergency_Contact_Number__c,
--     Primary_Contact_s_Mobile__c,
--     Primary_Contact_s_Name__c,
--     Program_Level__c,
--     snapshot_date
-- );

CREATE TABLE IF NOT EXISTS ft_ds_raw.sf_contact_phase_zero_point_five (
    PRIMARY KEY (snapshot_date, Id),
    snapshot_date TIMESTAMPTZ,
    Id TEXT,
    OtherPostalCode TEXT,
    Chapter_Affiliation__c TEXT,
    ChapterID_CONTACT__c TEXT,
    CASESAFEID__c TEXT,
    Contact_Type__c TEXT,
    Age__c TEXT,
    Ethnicity__c TEXT,
    Gender__c TEXT,
    Grade__c TEXT,
    Participation_Status__c TEXT
);

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

CREATE TABLE IF NOT EXISTS ft_ds_valid.sf_contact_phase_zero_point_five (
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

CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_contact_phase_zero_point_five (
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

-- --
-- --sf_metric_historical_active_participants_counts various data models
-- --
-- CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_active_participants_counts (
--     snapshot_date TIMESTAMPTZ,
--     participant_count INTEGER
-- );

-- --this can also be string to keep it more flexible
-- CREATE TYPE intl_status_dtype as ENUM (
--     'International',
--     'Domestic'
-- );
CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_active_participants_counts (
    snapshot_date TIMESTAMPTZ,
    intl_status VARCHAR(100),
    participant_count INTEGER
);

-- CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_active_participants_counts (
--     snapshot_date TIMESTAMPTZ,
--     international_participant_count INTEGER,
--     domestic_participant_count INTEGER
-- );

-- --
-- --sf_metric_historical_ethnic_diversity_percentage various data models
-- --
-- CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_ethnic_diversity_percentage (
--     snapshot_date TIMESTAMPTZ,
--     ethnic_diversity_percentage NUMERIC
-- );

-- --this can also be string to keep it more flexible
-- CREATE TYPE ethnicity_dtype as ENUM (
--     'Asian',
--     'Black or African-American',
--     'Multi-Racial',
--     'Native American or Native Alaskan',
--     'Pacific Islander',
--     'White or Caucasian', 
--     'Prefer not to respond'
-- );
CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_ethnic_breakdown (
    snapshot_date TIMESTAMPTZ,
    ethnicity VARCHAR(100),
    ethnically_diverse BOOLEAN,
    participant_count INTEGER
);

-- CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_ethnic_breakdown (
--     snapshot_date TIMESTAMPTZ,
--     asian_count INTEGER,
--     black_or_african_american_count INTEGER,
--     multi_racial_count INTEGER,
--     native_american_or_native_alaskan_count INTEGER,
--     pacific_islander_count INTEGER,
--     white_or_caucasian_count INTEGER,
--     prefer_not_to_respond_count INTEGER,
--     --can also just be these two
--     ethnically_diverse_count INTEGER,
--     total_count INTEGER

-- )
-- --
-- -- sf_metric_historical_non_male_percentage various data models
-- --
-- CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_non_male_percentage (
--     snapshot_date TIMESTAMPTZ,
--     non_male_percentage NUMERIC
-- );

-- --this can also be string to keep it more flexible
-- CREATE TYPE gender_dtype as ENUM (
--     'Female',
--     'Male',
--     'Non-Binary',
--     'The options listed do not reflect me',
--     'Prefer not to respond'
-- );
CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_gender_breakdown (
    snapshot_date TIMESTAMPTZ,
    gender VARCHAR(100),
    non_male BOOLEAN,
    participant_count INTEGER
);

-- CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_gender_breakdown (
--     snapshot_date TIMESTAMPTZ,
--     female_count INTEGER,
--     male_count INTEGER,
--     multi_racial_count INTEGER,
--     non_binary_count INTEGER,
--     no_reflection_count INTEGER,
--     prefer_not_to_respond_count INTEGER,
--     --can also just be these two
--     non_male_count INTEGER,
--     total_count INTEGER
-- )

-- --
-- -- sf_metric_historical_age_breakdown various data models
-- --
-- CREATE TYPE age_group_dtype as ENUM (
--     '< 7',
--     '7 - 9',
--     '10 - 11',
--     '12 -13',
--     '14+'
-- );
CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_gender_breakdown (
    snapshot_date TIMESTAMPTZ,
    age_group VARCHAR(100),
    -- maybe a teen flag?
    participant_count INTEGER
);

-- CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_age_breakdown (
--     snapshot_date TIMESTAMPTZ,
--     count_under7 INTEGER,
--     count_7to9 INTEGER,
--     count_10to11 INTEGER,
--     count_12to13 INTEGER,
--     count14up INTEGER
-- );

--Procedures
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

CREATE OR REPLACE PROCEDURE ft_ds_admin.raw_to_valid_sf_contact_phase_zero_point_five ()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_valid.sf_contact_phase_zero_point_five;

    INSERT INTO ft_ds_valid.sf_contact_phase_zero_point_five
    SELECT
        snapshot_date AS snapshot_date,
        Id AS contact_id_18,
        Chapter_Affiliation__c AS chapter_id,
        CAST(Age__c AS INTEGER) AS age,
        Gender__c AS gender,
        Ethnicity__c AS ethnicity,
        Grade__c AS grade,
        OtherPostalCode AS mailing_zip_postal_code,
        Participation_Status__c AS participation_status,
        Contact_Type__c AS contact_type
    FROM ft_ds_raw.sf_contact_phase_zero_point_five
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
    ;
END;
$$;

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

CREATE OR REPLACE PROCEDURE ft_ds_admin.valid_to_refined_sf_contact_phase_zero_point_five ()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.sf_contact_phase_zero_point_five;

    INSERT INTO ft_ds_refined.sf_contact_phase_zero_point_five
    SELECT
        *
    FROM ft_ds_valid.sf_contact_phase_zero_point_five
    ;
END;
$$;