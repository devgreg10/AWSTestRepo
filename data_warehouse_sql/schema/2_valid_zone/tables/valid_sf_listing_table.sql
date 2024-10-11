CREATE TABLE IF NOT EXISTS ft_ds_valid.sf_listing (
    PRIMARY KEY (listing_id_18),
    listing_id_18 CHAR(18),
    account_id CHAR(18),
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    hosted_by CHAR(18),
    listing_location_address TEXT,
    listing_name VARCHAR(80),
    presented_by CHAR(18),
    publish_start_date TIMESTAMPTZ,
    publish_end_date TIMESTAMPTZ,
    is_deleted BOOLEAN,
    record_type_id VARCHAR(100), --picklist
    sf_created_timestamp TIMESTAMPTZ,
    sf_last_modified_timestamp TIMESTAMPTZ,
    sf_system_modstamp TIMESTAMPTZ,
    dss_ingestion_timestamp TIMESTAMPTZ
);