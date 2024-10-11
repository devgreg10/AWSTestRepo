CREATE OR REPLACE VIEW ft_ds_refined.curriculum_listing_sessions_view
AS
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
    sf_created_timestamp,
    sf_last_modified_timestamp,
    sf_system_modstamp,
    dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.listing_session
WHERE
    --this is the id representing the Curriculum type
    record_type_id = '01236000000nmeLAAQ'
    --the remainder of fields cannot be NULL since they are soft-required.
    AND base_price IS NOT NULL
    AND sf_created_timestamp IS NOT NULL
    AND max_capacity IS NOT NULL
    AND chapter_name IS NOT NULL
    AND chapter_id IS NOT NULL
    AND record_type_id IS NOT NULL
    AND dss_ingestion_timestamp IS NOT NULL
;