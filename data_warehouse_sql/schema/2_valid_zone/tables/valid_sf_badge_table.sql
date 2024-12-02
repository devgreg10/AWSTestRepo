CREATE TABLE IF NOT EXISTS ft_ds_valid.sf_badge (
    PRIMARY KEY (badge_id),
    badge_id CHAR(18),
    ownerid CHAR(18),
    isdeleted BOOLEAN,
    name VARCHAR(80),
    sf_created_date TIMESTAMPTZ,
    sf_last_modified_date TIMESTAMPTZ,
    last_modified_by_id CHAR(18),
    sf_system_modstamp TIMESTAMPTZ,
    sf_last_viewed_date TIMESTAMPTZ,
    sf_last_referenced_date TIMESTAMPTZ,
    description TEXT,
    category TEXT, --picklist
    badge_type TEXT, --picklist
    is_active BOOLEAN,
    points NUMERIC(18,0),
    sort_order NUMERIC(3,0),
    badge_id VARCHAR(20),
    age_group, --multi-picklist
    dss_ingestion_timestamp TIMESTAMPTZ
);