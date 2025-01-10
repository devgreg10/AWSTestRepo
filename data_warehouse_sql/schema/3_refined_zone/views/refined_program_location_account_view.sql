CREATE OR REPLACE VIEW ft_ds_refined.program_location_account_view
AS
SELECT
    program_location.account_id,
    program_location.account_name,
    parent_account_ids.account_name AS parent_account_name,
    program_location.parent_account_id,
    additional_trade_name_ids.account_name AS additional_trade_name,
    program_location.additional_trade_name_id,
    program_location.signed_facility_use_agreement,
    --record_type_ids.account_record_type_name,
    --this record type object is not imported to DSS yet
    program_location.account_record_type_id,
    program_location.territory,
    program_location.billing_street,
    program_location.billing_city,
    program_location.billing_state,
    program_location.billing_postal_code,
    program_location.billing_country,
    program_location.shipping_street,
    program_location.shipping_city,
    program_location.shipping_state,
    program_location.shipping_postal_code,
    program_location.shipping_country,
    program_location.number_of_holes,
    program_location.if_other_fill_in_how_many,
    program_location.golf_course_type,
    --NOT IN RAW AS course_features_available_to_first_tee,
    --NOT IN RAW AS if_location_has_simulators,_how_many?,
    program_location.discounts_offered_to_participants,
    program_location.discount_description,
    program_location.chapter_owns_this_facility,
    program_location.dedicated_first_tee_learning_center,
    --NOT IN RAW AS operates_the_facility_through_a_lease,
    program_location.learning_center_description,
    program_location.if_yes_how_long_is_the_lease,
    program_location.lease_holder,
    --NOT IN VALID AS notes_about_the_partnership,
    program_location.is_active
FROM ft_ds_refined.account program_location
LEFT JOIN ft_ds_refined.account parent_account_ids-- parent_account_ids come from accounts of all types, so cannot use one of the more restrictive views
    ON program_location.parent_account_id = parent_account_ids.account_id
LEFT JOIN ft_ds_refined.additional_trade_name_account_view additional_trade_name_ids
    ON program_location.additional_trade_name_id = additional_trade_name_ids.account_id
WHERE
    program_location.account_record_type_id = '01236000000nmeHAAQ'
;