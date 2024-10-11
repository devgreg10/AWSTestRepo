CREATE TABLE IF NOT EXISTS ft_ds_refined.listing_session (
    PRIMARY KEY (listing_session_id_18),
    listing_session_id_18 CHAR(18),
    additional_trade_name CHAR(18),
    age_eligibility_date VARCHAR(100), --picklist
    age_restriction NUMERIC(3,0),
    allow_early_registration BOOLEAN,
    base_price NUMERIC(18,0),
    coach_assigned CHAR(18),
    curriculum_hours NUMERIC(3,0),
    days_offered VARCHAR(255), --multi picklist
    event_coordinator CHAR(18),
    event_hours NUMERIC(3,0),
    full_description TEXT,
    gender_restriction VARCHAR(20), --picklist
    lesson_plan TEXT,
    listing_session_location_address TEXT,
    program_location_id CHAR(18),
    program_location_name TEXT,
    listing_id CHAR(18),
    max_capacity NUMERIC(4,0),
    maximum_age NUMERIC(2,0),
    membership_discount_active BOOLEAN,
    membership_id VARCHAR(255),
    membership_required BOOLEAN,
    military_discount_active BOOLEAN,
    minimum_age NUMERIC(2,0),
    listing_session_name VARCHAR(80),
    number_of_classes NUMERIC(18,0),
    parent_communication_french VARCHAR(255),
    parent_communication_spanish VARCHAR(255),
    parent_communication VARCHAR(255),
    chapter_name VARCHAR(255),
    chapter_id CHAR(18),
    primary_program_level_restriction VARCHAR(50), --picklist
    priority NUMERIC(18,0),
    private_event BOOLEAN,
    program_coordinator CHAR(18),
    program_level VARCHAR(50), --picklist
    program_sub_level VARCHAR(30), --picklist
    program_type VARCHAR(30), --picklist
    publish_end_date_time TIMESTAMPTZ,
    publish_start_date_time TIMESTAMPTZ,
    record_type_id VARCHAR(100), --picklist
    register_end_date_time TIMESTAMPTZ,
    register_start_date_time TIMESTAMPTZ,
    season VARCHAR(30), --picklist
    secondary_program_level_restriction VARCHAR(30), --picklist
    session_end_date_time TIMESTAMPTZ,
    session_id CHAR(18),
    session_start_date_time TIMESTAMPTZ,
    session_start_date TIMESTAMPTZ,
    session_status VARCHAR(30), --picklist
    sibling_discount_active BOOLEAN,
    support_coach_1 CHAR(18),
    support_coach_2 CHAR(18),
    support_coach_3 CHAR(18),
    support_coach_4 CHAR(18),
    support_coach_5 CHAR(18),
    support_coach_6 CHAR(18),
    third_program_level_restriction VARCHAR(30), --picklist
    total_registrations INTEGER,
    total_space_available INTEGER,
    waitlist_space_available INTEGER,
    waitlist_capacity NUMERIC(18,0),
    waitlist_counter_new INTEGER,
    is_deleted BOOLEAN,
    sf_created_timestamp TIMESTAMPTZ,
    sf_last_modified_timestamp TIMESTAMPTZ,
    sf_system_modstamp TIMESTAMPTZ,
    dss_ingestion_timestamp TIMESTAMPTZ
);