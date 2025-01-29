drop view if exists ft_ds_refined.listings_view;

CREATE OR REPLACE VIEW ft_ds_refined.listings_view
AS
SELECT
    listing.listing_id_18,
    listing.account_id,
    account.account_name,
    listing.start_date,
    listing.end_date,
    listing.hosted_by,
    account_hosted_by.account_name as hosted_by_name,
    listing.listing_location_address,
    listing.listing_name,
    listing.presented_by,
    account_presented_by.account_name as presented_by_name,
    listing.publish_start_date,
    listing.publish_end_date,
    listing.record_type_id,
    listing.sf_created_timestamp,
    listing.sf_last_modified_timestamp,
    listing.sf_system_modstamp,
    listing.dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.listing listing
left join ft_ds_refined.account account
	on listing.account_id = account.account_id
left join ft_ds_refined.account account_hosted_by
	on listing.hosted_by = account_hosted_by.account_id
left join ft_ds_refined.account account_presented_by
	on listing.presented_by = account_presented_by.account_id
WHERE
    --the remainder of fields cannot be NULL since they are soft-required.
    listing.account_id IS NOT NULL
    AND listing.sf_created_timestamp IS NOT NULL
    AND listing.end_date IS NOT NULL
    AND listing.hosted_by IS NOT NULL
    AND listing.listing_name IS NOT NULL
    AND listing.presented_by IS NOT NULL
    AND listing.publish_start_date IS NOT NULL
    AND listing.publish_end_date IS NOT NULL
    AND listing.record_type_id IS NOT NULL
    AND listing.start_date IS NOT NULL
    AND listing.dss_ingestion_timestamp IS NOT NULL
;