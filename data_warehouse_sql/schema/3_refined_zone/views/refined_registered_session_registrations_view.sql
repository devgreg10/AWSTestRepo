CREATE OR REPLACE VIEW ft_ds_refined.registered_session_registrations_view
AS
SELECT
    session_registration.session_registration_id_18,
    session_registration.original_session_charge_amount,
    session_registration.contact_id_18,
    contact.first_name || ' ' || contact.last_name as contact_name,
    session_registration.cost_difference,
    session_registration.sf_created_by_id,
    session_registration.sf_created_timestamp,
    session_registration.discount,
    session_registration.item_price,
    session_registration.sf_last_modified_by_id,
    session_registration.sf_last_modified_timestamp,
    session_registration.listing_session_id_18,
    listing_session.listing_session_name as listing_session_name,
    session_registration.membership_registration_id_18,
    session_registration.session_registration_number,
    session_registration.old_listing_session_id_18,
    old_listing_session.listing_session_name as old_listing_session_name,
    session_registration.new_session_cost,
    session_registration.reggie_registration_id,
    session_registration.session_type,
    session_registration.status,
    session_registration.is_transferred,
    session_registration.waitlist_process,
    session_registration.original_session_registration_number,
    session_registration.sf_system_modstamp,
    session_registration.dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.session_registration session_registration
left join ft_ds_refined.contact contact
	on session_registration.contact_id_18 = contact.contact_id_18
left join ft_ds_refined.listing_session listing_session
	on session_registration.listing_session_id_18 = listing_session.listing_session_id_18
left join ft_ds_refined.listing_session old_listing_session
	on session_registration.old_listing_session_id_18 = old_listing_session.listing_session_id_18
WHERE
    session_registration.status = 'Registered'
    --the remainder of fields cannot be NULL since they are soft-required.
    AND session_registration.contact_id_18 IS NOT NULL
    AND session_registration.sf_created_by_id IS NOT NULL
    AND session_registration.sf_created_timestamp IS NOT NULL
    AND session_registration.sf_last_modified_by_id IS NOT NULL
    AND session_registration.sf_last_modified_timestamp IS NOT NULL
    AND session_registration.listing_session_id_18 IS NOT NULL
    AND session_registration.session_registration_number IS NOT NULL
    AND session_registration.status IS NOT NULL
    AND session_registration.dss_ingestion_timestamp IS NOT NULL
;