CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_listing_session_raw_to_valid (load_threshold_timestamp TIMESTAMPTZ DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN

    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_listing_session_raw_to_valid;

    CREATE TEMP TABLE IF NOT EXISTS temp_sf_listing_session_raw_to_valid (
        --still import as all text because we want to be able to analyze it for if it will make it to valid based on dtype, and we can assume it will fit in text because that's how it already exists in raw
        listing_session_id_18 TEXT,
        additional_trade_name TEXT,
        age_eligibility_date TEXT,
        age_restriction TEXT,
        allow_early_registration TEXT,
        base_price TEXT,
        coach_assigned TEXT,
        curriculum_hours TEXT,
        days_offered TEXT,
        event_coordinator TEXT,
        event_hours TEXT,
        full_description TEXT,
        gender_restriction TEXT,
        lesson_plan TEXT,
        listing_session_location_address TEXT,
        program_location_id TEXT,
        program_location_name TEXT,
        listing_id TEXT,
        max_capacity TEXT,
        maximum_age TEXT,
        membership_discount_active TEXT,
        membership_id TEXT,
        membership_required TEXT,
        military_discount_active TEXT,
        minimum_age TEXT,
        listing_session_name TEXT,
        number_of_classes TEXT,
        parent_communication_french TEXT,
        parent_communication_spanish TEXT,
        parent_communication TEXT,
        chapter_name TEXT,
        chapter_id TEXT,
        primary_program_level_restriction TEXT,
        priority TEXT,
        private_event TEXT,
        program_coordinator TEXT,
        program_level TEXT,
        program_sub_level TEXT,
        program_type TEXT,
        publish_end_date_time TEXT,
        publish_start_date_time TEXT,
        record_type_id TEXT,
        register_end_date_time TEXT,
        register_start_date_time TEXT,
        season TEXT,
        secondary_program_level_restriction TEXT,
        session_end_date_time TEXT,
        session_id TEXT,
        session_start_date_time TEXT,
        session_start_date TEXT,
        session_status TEXT,
        sibling_discount_active TEXT,
        support_coach_1 TEXT,
        support_coach_2 TEXT,
        support_coach_3 TEXT,
        support_coach_4 TEXT,
        support_coach_5 TEXT,
        support_coach_6 TEXT,
        third_program_level_restriction TEXT,
        total_registrations TEXT,
        total_space_available TEXT,
        waitlist_space_available TEXT,
        waitlist_capacity TEXT,
        waitlist_counter_new TEXT,
        is_deleted TEXT,
        sf_created_timestamp TEXT,
        sf_last_modified_timestamp TEXT,
        sf_system_modstamp TEXT,
        --dss_ingestion_timestamp can be assumed to still be timestamp because that's how it exists in raw
        dss_ingestion_timestamp TIMESTAMPTZ,
        required_fields_validated BOOLEAN,
        optional_fields_validated BOOLEAN
    );

    --this statement places all of the most recently uploaded records into the temp table
    INSERT INTO temp_sf_listing_session_raw_to_valid
    SELECT
        id AS listing_session_id_18,
        additional_trade_name__c AS additional_trade_name,
        age_eligibility_date__c AS age_eligibility_date,
        age_restriction__c AS age_restriction,
        allow_early_registration__c AS allow_early_registration,
        base_price__c AS base_price,
        coach_assigned__c AS coach_assigned,
        curriculum_hours__c AS curriculum_hours,
        days_offered__c AS days_offered,
        event_coordinator__c AS event_coordinator,
        event_hours__c AS event_hours,
        full_description__c AS full_description,
        gender_restriction__c AS gender_restriction,
        lesson_plan__c AS lesson_plan,
        listing_session_location_address__c AS listing_session_location_address,
        listing_session_location_name__c AS program_location_id,
        listing_session_location__c AS program_location_name,
        listing__c AS listing_id,
        max_capacity__c AS max_capacity,
        maximum_age__c AS maximum_age,
        membership_discount_active__c AS membership_discount_active,
        membership_id__c AS membership_id,
        membership_required__c AS membership_required,
        military_discount_active__c AS military_discount_active,
        minimum_age__c AS minimum_age,
        name AS listing_session_name,
        number_of_classes__c AS number_of_classes,
        parent_communication_french__c AS parent_communication_french,
        parent_communication_spanish__c AS parent_communication_spanish,
        parent_communication__c AS parent_communication,
        presented_by_name__c AS chapter_name,
        presented_by__c AS chapter_id,
        primary_program_level_restriction__c AS primary_program_level_restriction,
        priority__c AS priority,
        private_event__c AS private_event,
        program_coordinator__c AS program_coordinator,
        program_level__c AS program_level,
        program_sub_level__c AS program_sub_level,
        program_type__c AS program_type,
        publish_end_date_time__c AS publish_end_date_time,
        publish_start_date_time__c AS publish_start_date_time,
        recordtypeid AS record_type_id,
        register_end_date_time__c AS register_end_date_time,
        register_start_date_time__c AS register_start_date_time,
        season__c AS season,
        secondary_program_level_restriction__c AS secondary_program_level_restriction,
        session_end_date_time__c AS session_end_date_time,
        session_id__c AS session_id,
        session_start_date_time__c AS session_start_date_time,
        session_start_date__c AS session_start_date,
        session_status__c AS session_status,
        sibling_discount_active__c AS sibling_discount_active,
        support_coach_1__c AS support_coach_1,
        support_coach_2__c AS support_coach_2,
        support_coach_3_del__c AS support_coach_3,
        support_coach_4_del__c AS support_coach_4,
        support_coach_5_del__c AS support_coach_5,
        support_coach_6_del__c AS support_coach_6,
        third_program_level_restriction__c  AS third_program_level_restriction,
        total_registrations__c AS total_registrations,
        total_space_available__c AS total_space_available,
        waitlist_space_available__c AS waitlist_space_available,
        waitlist_capacity__c AS waitlist_capacity,
        waitlist_counter_new__c AS waitlist_counter_new,
        isdeleted AS is_deleted,
        createddate AS sf_created_timestamp,
        lastmodifieddate AS sf_last_modified_timestamp,
        systemmodstamp AS sf_system_modstamp,
        dss_ingestion_timestamp,
        TRUE AS required_fields_validated,
        TRUE AS optional_fields_validated
    FROM ft_ds_raw.sf_listing_session
    WHERE
        dss_ingestion_timestamp >
        CASE
            WHEN load_threshold_timestamp IS NULL THEN (SELECT MAX(execution_timestamp) from ft_ds_valid.raw_to_valid_execution_log WHERE entity = 'sf_listing_session')
            ELSE load_threshold_timestamp
        END
    ;

    --this statement updates the required_fields_validated flag
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    --it is structured this way because if the required fields do not meet data quality, then they are not passed to valid and therefore do not need to be transformed further. Therefore, only the required_fields_validated flag needs to be updated.
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    required_fields_validated = FALSE
    WHERE
    --contact_id_18
        listing_session_id_18 IS NULL
        OR LENGTH(listing_session_id_18) <> 18
        OR listing_session_id_18 = ''
    --sf_system_modstamp
        OR sf_system_modstamp IS NULL
        OR NOT (SELECT ft_ds_admin.is_coercable_to_timestamptz(sf_system_modstamp))
    ;

    --these statements update the optional_fields_validated flag and swap invalid values to NULL
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    -- additional_trade_name
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    additional_trade_name = NULL
    WHERE
        additional_trade_name IS NULL
        OR LENGTH(additional_trade_name) <> 18
        OR additional_trade_name = ''
    ;
    -- age_eligibility_date
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    age_eligibility_date = NULL
    WHERE
        age_eligibility_date IS NULL
        OR age_eligibility_date NOT IN ('End Date Based Restriction','Start Date Based Restriction')
        OR age_eligibility_date = ''
    ; 
    -- age_restriction
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    age_restriction = NULL
    WHERE
        age_restriction IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(age_restriction))
        OR age_restriction = ''
    ; 
    -- allow_early_registration
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    allow_early_registration = NULL
    WHERE
        allow_early_registration IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(allow_early_registration))
        OR allow_early_registration = ''
    ;
    -- base_price
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    base_price = NULL
    WHERE
        base_price IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(base_price))
        OR base_price = ''
    ;
    -- coach_assigned
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    coach_assigned = NULL
    WHERE
        coach_assigned IS NULL
        OR LENGTH(coach_assigned) <> 18
        OR coach_assigned = ''
    ;
    -- curriculum_hours
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    curriculum_hours = NULL
    WHERE
        curriculum_hours IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(curriculum_hours))
        OR curriculum_hours = ''
    ;
    -- days_offered
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    days_offered = NULL
    WHERE
        days_offered IS NULL
        OR days_offered = ''
    ;
    -- event_coordinator
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    event_coordinator = NULL
    WHERE
        event_coordinator IS NULL
        OR LENGTH(event_coordinator) <> 18
        OR event_coordinator = ''
    ;
    -- event_hours
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    event_hours = NULL
    WHERE
        event_hours IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(event_hours))
        OR event_hours = ''
    ;
    -- full_description
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    full_description = NULL
    WHERE
        full_description IS NULL
        OR full_description = ''
    ;
    -- gender_restriction
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    gender_restriction = NULL
    WHERE
        gender_restriction IS NULL
        OR gender_restriction NOT IN ('Male','Female')
        OR gender_restriction = ''
    ;
    -- lesson_plan
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    lesson_plan = NULL
    WHERE
        lesson_plan IS NULL
        OR lesson_plan = ''
    ;
    -- listing_session_location_address
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    listing_session_location_address = NULL
    WHERE
        listing_session_location_address IS NULL
        OR listing_session_location_address = ''
    ;
    -- program_location_id
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    program_location_id = NULL
    WHERE
        program_location_id IS NULL
        OR LENGTH(program_location_id) <> 18
        OR program_location_id = ''
    ;
    -- program_location_name
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    program_location_name = NULL
    WHERE
        program_location_name IS NULL
        OR program_location_name = ''
    ;
    -- listing_id
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    listing_id = NULL
    WHERE
        listing_id IS NULL
        OR LENGTH(listing_id) <> 18
        OR listing_id = ''
    ;
    -- max_capacity
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    max_capacity = NULL
    WHERE
        max_capacity IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(max_capacity))
        OR max_capacity = ''
    ;
    -- maximum_age
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    maximum_age = NULL
    WHERE
        maximum_age IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(maximum_age))
        OR maximum_age = ''
    ;
    -- membership_discount_active
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    membership_discount_active = NULL
    WHERE
        membership_discount_active IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(membership_discount_active))
        OR membership_discount_active = ''
    ;
    -- membership_id
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    membership_id = NULL
    WHERE
        membership_id IS NULL
        OR membership_id = ''
    ;
    -- membership_required
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    membership_required = NULL
    WHERE
        membership_required IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(membership_required))
        OR membership_required = ''
    ;
    -- military_discount_active
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    military_discount_active = NULL
    WHERE
        military_discount_active IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(military_discount_active))
        OR military_discount_active = ''
    ;
    -- minimum_age
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    minimum_age = NULL
    WHERE
        minimum_age IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(minimum_age))
        OR minimum_age = ''
    ;
    -- listing_session_name
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    listing_session_name = NULL
    WHERE
        listing_session_name IS NULL
        OR listing_session_name = ''
    ;
    -- number_of_classes
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    number_of_classes = NULL
    WHERE
        number_of_classes IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(number_of_classes))
        OR number_of_classes = ''
    ;
    -- parent_communication_french
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    parent_communication_french = NULL
    WHERE
        parent_communication_french IS NULL
        OR parent_communication_french = ''
    ;
    -- parent_communication_spanish
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    parent_communication_spanish = NULL
    WHERE
        parent_communication_spanish IS NULL
        OR parent_communication_spanish = ''
    ;
    -- parent_communication
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    parent_communication = NULL
    WHERE
        parent_communication IS NULL
        OR parent_communication = ''
    ;
    -- chapter_name
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_name = NULL
    WHERE
        chapter_name IS NULL
        OR chapter_name = ''
    ;
    -- chapter_id
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_id = NULL
    WHERE
        chapter_id IS NULL
        OR LENGTH(chapter_id) <> 18
        OR chapter_id = ''
    ;
    -- primary_program_level_restriction
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    primary_program_level_restriction = NULL
    WHERE
        primary_program_level_restriction IS NULL
        OR primary_program_level_restriction NOT IN ('TARGET Registered','PLAYer','PLAYer Certified','Par','Par Certified','Birdie','Birdie Certified','Eagle','Eagle Certified','Ace','Ace Certified')
        OR primary_program_level_restriction = ''
    ;
    -- priority
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    priority = NULL
    WHERE
        priority IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(priority))
        OR priority = ''
    ;
    -- private_event
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    private_event = NULL
    WHERE
        private_event IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(private_event))
        OR private_event = ''
    ;
    -- program_coordinator
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    program_coordinator = NULL
    WHERE
        program_coordinator IS NULL
        OR LENGTH(program_coordinator) <> 18
        OR program_coordinator = ''
    ;
    -- program_level
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    program_level = NULL
    WHERE
        program_level IS NULL
        OR program_level NOT IN ('TARGET Registered','PLAYer','PLAYer Certified','Par','Par Certified','Birdie','Birdie Certified','Eagle','Eagle Certified','Ace','Ace Certified')
        OR program_level = ''
    ;
    -- program_sub_level
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    program_sub_level = NULL
    WHERE
        program_sub_level IS NULL
        OR program_sub_level NOT IN ('PLAYer 3 Hole','PLAYer 6 Hole','PLAYer 9 Hole','Par 3','Par 4','Par 5','Birdie Flight 1','Birdie Flight 2','Birdie Flight 3','Eagle Flight 1','Eagle Flight 2','Eagle Flight 3','Ace Project 1','Ace Project 2','Ace Project 3','Ace Project 4')
        OR program_sub_level = ''
    ;
    -- program_type
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    program_type = NULL
    WHERE
        program_type IS NULL
        OR program_type NOT IN ('General Programs','Special Events','Competition','Camps')
        OR program_type = ''
    ;
    -- publish_end_date_time
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    publish_end_date_time = NULL
    WHERE
        publish_end_date_time IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(publish_end_date_time))
        OR publish_end_date_time = ''
    ;
    -- publish_start_date_time
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    publish_start_date_time = NULL
    WHERE
        publish_start_date_time IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(publish_start_date_time))
        OR publish_start_date_time = ''
    ;
    -- record_type_id
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    record_type_id = NULL
    WHERE
        record_type_id IS NULL
        OR record_type_id NOT IN ('01236000000nmeMAAQ', '01236000000nmeLAAQ')
        OR record_type_id = ''
    ;
    -- register_end_date_time
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    register_end_date_time = NULL
    WHERE
        register_end_date_time IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(register_end_date_time))
        OR register_end_date_time = ''
    ;
    -- register_start_date_time
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    register_start_date_time = NULL
    WHERE
        register_start_date_time IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(register_start_date_time))
        OR register_start_date_time = ''
    ;
    -- season
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    season = NULL
    WHERE
        season IS NULL
        OR season NOT IN ('Spring','Summer','Fall','Winter')
        OR season = ''
    ;
    -- secondary_program_level_restriction
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    secondary_program_level_restriction = NULL
    WHERE
        secondary_program_level_restriction IS NULL
        OR secondary_program_level_restriction NOT IN ('TARGET Registered','PLAYer','PLAYer Certified','Par','Par Certified','Birdie','Birdie Certified','Eagle','Eagle Certified','Ace','Ace Certified')
        OR secondary_program_level_restriction = ''
    ;
    -- session_end_date_time
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    session_end_date_time = NULL
    WHERE
        session_end_date_time IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(session_end_date_time))
        OR session_end_date_time = ''
    ;
    -- session_id
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    session_id = NULL
    WHERE
        session_id IS NULL
        OR LENGTH(session_id) <> 18
        OR session_id = ''
    ;
    -- session_start_date_time
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    session_start_date_time = NULL
    WHERE
        session_start_date_time IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(session_start_date_time))
        OR session_start_date_time = ''
    ;
    -- session_start_date
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    session_start_date = NULL
    WHERE
        session_start_date IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(session_start_date))
        OR session_start_date = ''
    ;
    -- session_status
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    session_status = NULL
    WHERE
        session_status IS NULL
        OR session_status NOT IN ('Started')
        OR session_status = ''
    ;
    -- sibling_discount_active
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sibling_discount_active = NULL
    WHERE
        sibling_discount_active IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(sibling_discount_active))
        OR sibling_discount_active = ''
    ;
    -- support_coach_1
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    support_coach_1 = NULL
    WHERE
        support_coach_1 IS NULL
        OR LENGTH(support_coach_1) <> 18
        OR support_coach_1 = ''
    ;
    -- support_coach_2
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    support_coach_2 = NULL
    WHERE
        support_coach_2 IS NULL
        OR LENGTH(support_coach_2) <> 18
        OR support_coach_2 = ''
    ;
    -- support_coach_3
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    support_coach_3 = NULL
    WHERE
        support_coach_3 IS NULL
        OR LENGTH(support_coach_3) <> 18
        OR support_coach_3 = ''
    ;
    -- support_coach_4
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    support_coach_4 = NULL
    WHERE
        support_coach_4 IS NULL
        OR LENGTH(support_coach_4) <> 18
        OR support_coach_4 = ''
    ;
    -- support_coach_5
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    support_coach_5 = NULL
    WHERE
        support_coach_5 IS NULL
        OR LENGTH(support_coach_5) <> 18
        OR support_coach_5 = ''
    ;
    -- support_coach_6
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    support_coach_6 = NULL
    WHERE
        support_coach_6 IS NULL
        OR LENGTH(support_coach_6) <> 18
        OR support_coach_6 = ''
    ;
    -- third_program_level_restriction
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    third_program_level_restriction = NULL
    WHERE
        third_program_level_restriction IS NULL
        OR third_program_level_restriction NOT IN ('TARGET Registered','PLAYer','PLAYer Certified','Par','Par Certified','Birdie','Birdie Certified','Eagle','Eagle Certified','Ace','Ace Certified')
        OR third_program_level_restriction = ''
    ;
    -- total_registrations
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    total_registrations = NULL
    WHERE
        total_registrations IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(total_registrations))
        OR total_registrations = ''
    ;
    -- total_space_available
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    total_space_available = NULL
    WHERE
        total_space_available IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(total_space_available))
        OR total_space_available = ''
    ;
    -- waitlist_space_available
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    waitlist_space_available = NULL
    WHERE
        waitlist_space_available IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(waitlist_space_available))
        OR waitlist_space_available = ''
    ;
    -- waitlist_capacity
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    waitlist_capacity = NULL
    WHERE
        waitlist_capacity IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(waitlist_capacity))
        OR waitlist_capacity = ''
    ;
    -- waitlist_counter_new
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    waitlist_counter_new = NULL
    WHERE
        waitlist_counter_new IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(waitlist_counter_new))
        OR waitlist_counter_new = ''
    ;
    -- is_deleted
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_deleted = NULL
    WHERE
        is_deleted IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(is_deleted))
        OR is_deleted = ''
    ;
    -- sf_created_timestamp
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_created_timestamp = NULL
    WHERE
        sf_created_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_created_timestamp))
        OR sf_created_timestamp = ''
    ;
    -- sf_last_modified_timestamp
    UPDATE temp_sf_listing_session_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_timestamp = NULL
    WHERE
        sf_last_modified_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_last_modified_timestamp))
        OR sf_last_modified_timestamp = ''
    ;
    -- dss_ingestion_timestamp
    UPDATE temp_sf_listing_session_raw_to_valid
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
    INSERT INTO ft_ds_raw.validation_errors_sf_listing_session
    SELECT
        listing_session_id_18,
        additional_trade_name,
        age_eligibility_date,
        age_restriction,
        allow_early_registration,
        base_price,
        coach_assigned,
        curriculum_hours,
        days_offered,
        event_coordinator,
        event_hours,
        full_description,
        gender_restriction,
        lesson_plan,
        listing_session_location_address,
        program_location_id,
        program_location_name,
        listing_id,
        max_capacity,
        maximum_age,
        membership_discount_active,
        membership_id,
        membership_required,
        military_discount_active,
        minimum_age,
        listing_session_name,
        number_of_classes,
        parent_communication_french,
        parent_communication_spanish,
        parent_communication,
        chapter_name,
        chapter_id,
        primary_program_level_restriction,
        priority,
        private_event,
        program_coordinator,
        program_level,
        program_sub_level,
        program_type,
        publish_end_date_time,
        publish_start_date_time,
        record_type_id,
        register_end_date_time,
        register_start_date_time,
        season,
        secondary_program_level_restriction,
        session_end_date_time,
        session_id,
        session_start_date_time,
        session_start_date,
        session_status,
        sibling_discount_active,
        support_coach_1,
        support_coach_2,
        support_coach_3,
        support_coach_4,
        support_coach_5,
        support_coach_6,
        third_program_level_restriction,
        total_registrations,
        total_space_available,
        waitlist_space_available,
        waitlist_capacity,
        waitlist_counter_new,
        is_deleted,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp,
        dss_ingestion_timestamp,
        required_fields_validated,
        optional_fields_validated,
        FALSE AS fixed_in_source
    FROM temp_sf_listing_session_raw_to_valid
    WHERE
        required_fields_validated = FALSE
        OR optional_fields_validated = FALSE
    ;

    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_listing_session_raw_to_valid_validated;

    --now that we've flagged the valid data and set optional invalid fields to NULL, we can cast the values to their valid types
    CREATE TABLE IF NOT EXISTS temp_sf_listing_session_raw_to_valid_validated (
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

    --this statement then cleans the data to get the data types correct
    INSERT INTO temp_sf_listing_session_raw_to_valid_validated
    SELECT
        listing_session_id_18,
        additional_trade_name,
        age_eligibility_date, 
        CAST(age_restriction AS NUMERIC) AS age_restriction,
        CAST(allow_early_registration AS BOOLEAN) AS allow_early_registration,
        CAST(base_price AS NUMERIC) AS base_price,
        coach_assigned,
        CAST(curriculum_hours AS NUMERIC) AS curriculum_hours,
        days_offered, 
        event_coordinator,
        CAST(event_hours AS NUMERIC) AS event_hours,
        full_description,
        gender_restriction, 
        lesson_plan,
        listing_session_location_address,
        program_location_id,
        program_location_name,
        listing_id,
        CAST(max_capacity AS NUMERIC) AS max_capacity,
        CAST(maximum_age AS NUMERIC) AS maximum_age,
        CAST(membership_discount_active AS BOOLEAN) AS membership_discount_active,
        membership_id,
        CAST(membership_required AS BOOLEAN) AS membership_required,
        CAST(military_discount_active AS BOOLEAN) AS military_discount_active,
        CAST(minimum_age AS NUMERIC) AS minimum_age,
        listing_session_name,
        CAST(number_of_classes AS NUMERIC) AS number_of_classes,
        parent_communication_french,
        parent_communication_spanish,
        parent_communication,
        chapter_name,
        chapter_id,
        primary_program_level_restriction, 
        CAST(priority AS NUMERIC) AS priority,
        CAST(private_event AS BOOLEAN) AS private_event,
        program_coordinator,
        program_level, 
        program_sub_level, 
        program_type, 
        CAST(publish_end_date_time AS TIMESTAMPTZ) AS publish_end_date_time,
        CAST(publish_start_date_time AS TIMESTAMPTZ) AS publish_start_date_time,
        record_type_id, 
        CAST(register_end_date_time AS TIMESTAMPTZ) AS register_end_date_time,
        CAST(register_start_date_time AS TIMESTAMPTZ) AS register_start_date_time,
        season, 
        secondary_program_level_restriction, 
        CAST(session_end_date_time AS TIMESTAMPTZ) AS session_end_date_time,
        session_id,
        CAST(session_start_date_time AS TIMESTAMPTZ) AS session_start_date_time,
        CAST(session_start_date AS TIMESTAMPTZ) AS session_start_date,
        session_status, 
        CAST(sibling_discount_active AS BOOLEAN) AS sibling_discount_active,
        support_coach_1,
        support_coach_2,
        support_coach_3,
        support_coach_4,
        support_coach_5,
        support_coach_6,
        third_program_level_restriction,
        CAST(total_registrations AS NUMERIC) AS total_registrations,
        CAST(total_space_available AS NUMERIC) AS total_space_available,
        CAST(waitlist_space_available AS NUMERIC) AS waitlist_space_available,
        CAST(waitlist_capacity AS NUMERIC) AS waitlist_capacity,
        CAST(waitlist_counter_new AS NUMERIC) AS waitlist_counter_new,
        CAST(is_deleted AS BOOLEAN) AS is_deleted,
        CAST(sf_created_timestamp AS TIMESTAMPTZ) AS sf_created_timestamp,
        CAST(sf_last_modified_timestamp AS TIMESTAMPTZ) AS sf_last_modified_timestamp,
        CAST(sf_system_modstamp AS TIMESTAMPTZ) AS sf_system_modstamp,
        dss_ingestion_timestamp
    FROM temp_sf_listing_session_raw_to_valid
    WHERE
        required_fields_validated = TRUE
    ;

    --this query gets the population correct for transitioning from raw to valid. It only includes records:
    -- that were inserted into the raw zone since the last raw->valid run
    -- that dont belong to testing chapters
    -- that are unique to each unique ID value (no dups)
    INSERT INTO ft_ds_valid.sf_listing_session
    SELECT
        all_values_but_dss_ingestion.*,
        dss_ingestion.dss_ingestion_timestamp
    FROM
    (SELECT
        listing_session_id_18,
        additional_trade_name,
        age_eligibility_date,
        age_restriction,
        allow_early_registration,
        base_price,
        coach_assigned,
        curriculum_hours,
        days_offered,
        event_coordinator,
        event_hours,
        full_description,
        gender_restriction,
        lesson_plan,
        listing_session_location_address,
        program_location_id,
        program_location_name,
        listing_id,
        max_capacity,
        maximum_age,
        membership_discount_active,
        membership_id,
        membership_required,
        military_discount_active,
        minimum_age,
        listing_session_name,
        number_of_classes,
        parent_communication_french,
        parent_communication_spanish,
        parent_communication,
        chapter_name,
        chapter_id,
        primary_program_level_restriction,
        priority,
        private_event,
        program_coordinator,
        program_level,
        program_sub_level,
        program_type,
        publish_end_date_time,
        publish_start_date_time,
        record_type_id,
        register_end_date_time,
        register_start_date_time,
        season,
        secondary_program_level_restriction,
        session_end_date_time,
        session_id,
        session_start_date_time,
        session_start_date,
        session_status,
        sibling_discount_active,
        support_coach_1,
        support_coach_2,
        support_coach_3,
        support_coach_4,
        support_coach_5,
        support_coach_6,
        third_program_level_restriction,
        total_registrations,
        total_space_available,
        waitlist_space_available,
        waitlist_capacity,
        waitlist_counter_new,
        is_deleted,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp
    FROM temp_sf_listing_session_raw_to_valid_validated
    GROUP BY
        --this group by clause exists to eliminate duplicates since multiple records with the same Id and system_modstamp can exist
        --it is every field going into the valid table except dss_ingestion_timestamp
        listing_session_id_18,
        additional_trade_name,
        age_eligibility_date,
        age_restriction,
        allow_early_registration,
        base_price,
        coach_assigned,
        curriculum_hours,
        days_offered,
        event_coordinator,
        event_hours,
        full_description,
        gender_restriction,
        lesson_plan,
        listing_session_location_address,
        program_location_id,
        program_location_name,
        listing_id,
        max_capacity,
        maximum_age,
        membership_discount_active,
        membership_id,
        membership_required,
        military_discount_active,
        minimum_age,
        listing_session_name,
        number_of_classes,
        parent_communication_french,
        parent_communication_spanish,
        parent_communication,
        chapter_name,
        chapter_id,
        primary_program_level_restriction,
        priority,
        private_event,
        program_coordinator,
        program_level,
        program_sub_level,
        program_type,
        publish_end_date_time,
        publish_start_date_time,
        record_type_id,
        register_end_date_time,
        register_start_date_time,
        season,
        secondary_program_level_restriction,
        session_end_date_time,
        session_id,
        session_start_date_time,
        session_start_date,
        session_status,
        sibling_discount_active,
        support_coach_1,
        support_coach_2,
        support_coach_3,
        support_coach_4,
        support_coach_5,
        support_coach_6,
        third_program_level_restriction,
        total_registrations,
        total_space_available,
        waitlist_space_available,
        waitlist_capacity,
        waitlist_counter_new,
        is_deleted,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp
    ) all_values_but_dss_ingestion
    JOIN
    (SELECT
        listing_session_id_18,
        MAX(sf_system_modstamp) as max_date
    FROM temp_sf_listing_session_raw_to_valid_validated
    GROUP BY
        listing_session_id_18
    ) max_dates
    ON all_values_but_dss_ingestion.listing_session_id_18 = max_dates.listing_session_id_18
    AND all_values_but_dss_ingestion.sf_system_modstamp = max_dates.max_date
    JOIN
    (SELECT
        listing_session_id_18,
        MAX(dss_ingestion_timestamp) as dss_ingestion_timestamp
    FROM temp_sf_listing_session_raw_to_valid_validated
    GROUP BY
        listing_session_id_18
    )dss_ingestion
    ON all_values_but_dss_ingestion.listing_session_id_18 = dss_ingestion.listing_session_id_18
    ON CONFLICT (listing_session_id_18) DO UPDATE SET
        additional_trade_name = EXCLUDED.additional_trade_name,
        age_eligibility_date = EXCLUDED.age_eligibility_date,
        age_restriction = EXCLUDED.age_restriction,
        allow_early_registration = EXCLUDED.allow_early_registration,
        base_price = EXCLUDED.base_price,
        coach_assigned = EXCLUDED.coach_assigned,
        curriculum_hours = EXCLUDED.curriculum_hours,
        days_offered = EXCLUDED.days_offered,
        event_coordinator = EXCLUDED.event_coordinator,
        event_hours = EXCLUDED.event_hours,
        full_description = EXCLUDED.full_description,
        gender_restriction = EXCLUDED.gender_restriction,
        lesson_plan = EXCLUDED.lesson_plan,
        listing_session_location_address = EXCLUDED.listing_session_location_address,
        program_location_id = EXCLUDED.program_location_id,
        program_location_name = EXCLUDED.program_location_name,
        listing_id = EXCLUDED.listing_id,
        max_capacity = EXCLUDED.max_capacity,
        maximum_age = EXCLUDED.maximum_age,
        membership_discount_active = EXCLUDED.membership_discount_active,
        membership_id = EXCLUDED.membership_id,
        membership_required = EXCLUDED.membership_required,
        military_discount_active = EXCLUDED.military_discount_active,
        minimum_age = EXCLUDED.minimum_age,
        listing_session_name = EXCLUDED.listing_session_name,
        number_of_classes = EXCLUDED.number_of_classes,
        parent_communication_french = EXCLUDED.parent_communication_french,
        parent_communication_spanish = EXCLUDED.parent_communication_spanish,
        parent_communication = EXCLUDED.parent_communication,
        chapter_name = EXCLUDED.chapter_name,
        chapter_id = EXCLUDED.chapter_id,
        primary_program_level_restriction = EXCLUDED.primary_program_level_restriction,
        priority = EXCLUDED.priority,
        private_event = EXCLUDED.private_event,
        program_coordinator = EXCLUDED.program_coordinator,
        program_level = EXCLUDED.program_level,
        program_sub_level = EXCLUDED.program_sub_level,
        program_type = EXCLUDED.program_type,
        publish_end_date_time = EXCLUDED.publish_end_date_time,
        publish_start_date_time = EXCLUDED.publish_start_date_time,
        record_type_id = EXCLUDED.record_type_id,
        register_end_date_time = EXCLUDED.register_end_date_time,
        register_start_date_time = EXCLUDED.register_start_date_time,
        season = EXCLUDED.season,
        secondary_program_level_restriction = EXCLUDED.secondary_program_level_restriction,
        session_end_date_time = EXCLUDED.session_end_date_time,
        session_id = EXCLUDED.session_id,
        session_start_date_time = EXCLUDED.session_start_date_time,
        session_start_date = EXCLUDED.session_start_date,
        session_status = EXCLUDED.session_status,
        sibling_discount_active = EXCLUDED.sibling_discount_active,
        support_coach_1 = EXCLUDED.support_coach_1,
        support_coach_2 = EXCLUDED.support_coach_2,
        support_coach_3 = EXCLUDED.support_coach_3,
        support_coach_4 = EXCLUDED.support_coach_4,
        support_coach_5 = EXCLUDED.support_coach_5,
        support_coach_6 = EXCLUDED.support_coach_6,
        third_program_level_restriction = EXCLUDED.third_program_level_restriction,
        total_registrations = EXCLUDED.total_registrations,
        total_space_available = EXCLUDED.total_space_available,
        waitlist_space_available = EXCLUDED.waitlist_space_available,
        waitlist_capacity = EXCLUDED.waitlist_capacity,
        waitlist_counter_new = EXCLUDED.waitlist_counter_new,
        is_deleted = EXCLUDED.is_deleted,
        sf_created_timestamp = EXCLUDED.sf_created_timestamp,
        sf_last_modified_timestamp = EXCLUDED.sf_last_modified_timestamp,
        sf_system_modstamp  = EXCLUDED.sf_system_modstamp,
        dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp
    ;

    INSERT INTO ft_ds_valid.raw_to_valid_execution_log
    SELECT
    MAX(dss_ingestion_timestamp) AS execution_timestamp,
    'sf_listing_session' AS entity
    FROM ft_ds_valid.sf_listing_session
    ;
END;
$$;