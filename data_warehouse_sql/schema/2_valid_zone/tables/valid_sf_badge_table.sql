CREATE TABLE IF NOT EXISTS ft_ds_valid.sf_badge (
    PRIMARY KEY (badge_id),
    badge_id CHAR(18),
    owner_id CHAR(18),
    is_deleted BOOLEAN,
    badge_name VARCHAR(80),
    sf_created_timestamp TIMESTAMPTZ,
    sf_created_by_id CHAR(18),
    sf_last_modified_timestamp TIMESTAMPTZ,
    sf_last_modified_by_id CHAR(18),
    sf_system_modstamp TIMESTAMPTZ,
    sf_last_viewed_date TIMESTAMPTZ,
    sf_last_referenced_date TIMESTAMPTZ,
    description TEXT,
    category TEXT, --picklist
    badge_type TEXT, --picklist
    is_active BOOLEAN,
    points NUMERIC(18,0),
    sort_order NUMERIC(3,0),
    external_badge_id VARCHAR(20),
    age_group TEXT, --multi-picklist
    dss_ingestion_timestamp TIMESTAMPTZ
);