CREATE OR REPLACE VIEW ft_ds_refined.listings_view
AS
SELECT
    listing_id_18,
    account_id,
    start_date,
    end_date,
    hosted_by,
    listing_location_address,
    listing_name,
    presented_by,
    publish_start_date,
    publish_end_date,
    record_type_id,
    sf_created_timestamp,
    sf_last_modified_timestamp,
    sf_system_modstamp,
    dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.listing
WHERE
    --the remainder of fields cannot be NULL since they are soft-required.
    account_id IS NOT NULL
    AND sf_created_timestamp IS NOT NULL
    AND end_date IS NOT NULL
    AND hosted_by IS NOT NULL
    AND listing_name IS NOT NULL
    AND presented_by IS NOT NULL
    AND publish_start_date IS NOT NULL
    AND publish_end_date IS NOT NULL
    AND record_type_id IS NOT NULL
    AND start_date IS NOT NULL
    AND dss_ingestion_timestamp IS NOT NULL
;