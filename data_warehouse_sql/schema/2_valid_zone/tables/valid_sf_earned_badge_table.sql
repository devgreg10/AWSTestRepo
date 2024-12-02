CREATE TABLE IF NOT EXISTS ft_ds_valid.sf_earned_badge (
    PRIMARY KEY (earned_badge_id),
    earned_badge_id CHAR(18),
    is_deleted BOOLEAN,
    sf_created_timestamp TIMESTAMPTZ,
    sf_created_by_id CHAR(18),
    sf_last_modified_timestamp TIMESTAMPTZ,
    sf_last_modified_by_id CHAR(18),
    sf_system_modstamp TIMESTAMPTZ,
    badge_id CHAR(18),
    badge_type TEXT, --picklist
    category TEXT, --picklist
    class_id CHAR(18),
    contact_id CHAR(18),
    date_earned TIMESTAMPTZ,
    listing_session_id CHAR(18),
    is_pending_aws_callout BOOLEAN,
    points NUMERIC(18,0),
    source_system TEXT, --picklist
    dss_ingestion_timestamp TIMESTAMPTZ
);