CREATE OR REPLACE VIEW ft_ds_refined.curriculum_listing_sessions_view
AS
SELECT
    listing_session.listing_session_id_18,
    listing_session.additional_trade_name,
    listing_session.age_eligibility_date,
    listing_session.age_restriction,
    listing_session.allow_early_registration,
    listing_session.base_price,
    listing_session.coach_assigned,
    contact_coach.first_name || ' ' || contact_coach.last_name as coach_name, 
    listing_session.curriculum_hours,
    listing_session.days_offered,
    listing_session.event_coordinator,
    listing_session.event_hours,
    listing_session.full_description,
    listing_session.gender_restriction,
    listing_session.lesson_plan,
    listing_session.listing_session_location_address,
    listing_session.program_location_id,
    listing_session.program_location_name,
    listing_session.listing_id,
    listing.listing_name,
    listing_session.max_capacity,
    listing_session.maximum_age,
    listing_session.membership_discount_active,
    listing_session.membership_id,
    listing_session.membership_required,
    listing_session.military_discount_active,
    listing_session.minimum_age,
    listing_session.listing_session_name,
    listing_session.number_of_classes,
    listing_session.parent_communication_french,
    listing_session.parent_communication_spanish,
    listing_session.parent_communication,
    listing_session.chapter_name,
    listing_session.chapter_id,
    listing_session.primary_program_level_restriction,
    listing_session.priority,
    listing_session.private_event,
    listing_session.program_coordinator,
    listing_session.program_level,
    listing_session.program_sub_level,
    listing_session.program_type,
    listing_session.publish_end_date_time,
    listing_session.publish_start_date_time,
    listing_session.record_type_id,
    listing_session.register_end_date_time,
    listing_session.register_start_date_time,
    listing_session.season,
    listing_session.secondary_program_level_restriction,
    listing_session.session_end_date_time,
    listing_session.session_id,
    listing_session.session_start_date_time,
    listing_session.session_start_date,
    listing_session.session_status,
    listing_session.sibling_discount_active,
    listing_session.support_coach_1,
    listing_session.support_coach_2,
    listing_session.support_coach_3,
    listing_session.support_coach_4,
    listing_session.support_coach_5,
    listing_session.support_coach_6,
    listing_session.third_program_level_restriction,
    listing_session.total_registrations,
    listing_session.total_space_available,
    listing_session.waitlist_space_available,
    listing_session.waitlist_capacity,
    listing_session.waitlist_counter_new,
    listing_session.sf_created_timestamp,
    listing_session.sf_last_modified_timestamp,
    listing_session.sf_system_modstamp,
    listing_session.dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.listing_session listing_session
left join ft_ds_refined.contact contact_coach
	on listing_session.coach_assigned = contact_coach.contact_id_18
left join ft_ds_refined.listing listing 
	on listing_session.listing_id = listing.listing_id_18
WHERE
    --this is the id representing the Curriculum type
    listing_session.record_type_id = '01236000000nmeLAAQ'
    --the remainder of fields cannot be NULL since they are soft-required.
    AND listing_session.base_price IS NOT NULL
    AND listing_session.sf_created_timestamp IS NOT NULL
    AND listing_session.max_capacity IS NOT NULL
    AND listing_session.chapter_name IS NOT NULL
    AND listing_session.chapter_id IS NOT NULL
    AND listing_session.record_type_id IS NOT NULL
    AND listing_session.dss_ingestion_timestamp IS NOT NULL
;