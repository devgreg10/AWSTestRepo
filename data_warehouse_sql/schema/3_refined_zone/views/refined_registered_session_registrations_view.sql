CREATE OR REPLACE VIEW ft_ds_refined.registered_session_registrations_view
AS
SELECT
    session_registration_id_18,
    original_session_charge_amount,
    contact_id_18,
    cost_difference,
    sf_created_by_id,
    sf_created_timestamp,
    discount,
    item_price,
    sf_last_modified_by_id,
    sf_last_modified_timestamp,
    listing_session_id_18,
    membership_registration_id_18,
    session_registration_number,
    old_listing_session_id_18,
    new_session_cost,
    reggie_registration_id,
    session_type,
    status,
    is_transferred,
    waitlist_process,
    original_session_registration_number,
    sf_system_modstamp,
    dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.session_registration
WHERE
    status = 'Registered'
    --the remainder of fields cannot be NULL since they are soft-required.
    AND contact_id_18 IS NOT NULL
    AND sf_created_by_id IS NOT NULL
    AND sf_created_timestamp IS NOT NULL
    AND sf_last_modified_by_id IS NOT NULL
    AND sf_last_modified_timestamp IS NOT NULL
    AND listing_session_id_18 IS NOT NULL
    AND session_registration_number IS NOT NULL
    AND status IS NOT NULL
    AND dss_ingestion_timestamp IS NOT NULL
;