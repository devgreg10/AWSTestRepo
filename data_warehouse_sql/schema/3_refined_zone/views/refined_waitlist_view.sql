CREATE OR REPLACE VIEW ft_ds_refined.waitlist_view
AS
SELECT
	waitlist_id,
    waitlist_name,
    chapter_id,
    chapter_name,
    contact,
    listing_session_location_name,
    listing_session_name,
    --membership_end_date,
    --membership_start_date,
    --membership_price,
    --membership_required,
    parent_contact,
    status,
    waitlist_participant_order,
    waitlist_unique_key,
    status_is_in_process_or_selected,
    is_deleted,
    sf_created_timestamp,
    sf_last_modified_timestamp,
    sf_system_modstamp,
    listing_session,
    dss_ingestion_timestamp
FROM ft_ds_refined.waitlist;