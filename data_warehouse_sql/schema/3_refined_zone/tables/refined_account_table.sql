CREATE TABLE IF NOT EXISTS ft_ds_refined.account (
    PRIMARY KEY (account_id),
    account_id CHAR(18),
    account_inactive_date TIMESTAMPTZ,
    account_name TEXT,
    account_number VARCHAR(40),
    account_record_type_id CHAR(18),
    is_active BOOLEAN,
    additional_trade_name_id CHAR(18),
    additional_trade_name_chapter_affiliation_id CHAR(18),
    billing_street TEXT,
    billing_city TEXT,
    billing_state TEXT,
    billing_postal_code TEXT,
    billing_country TEXT,
    board_chair CHAR(18),
    chapter_affiliation_id CHAR(18),
    chapter_country TEXT, --picklist
    chapter_membership_price NUMERIC(16, 2),
    chapter_owns_this_facility TEXT, -- picklist
    chapter_standing TEXT, --picklist
    contract_effective_date TIMESTAMPTZ,
    contract_expiration_date TIMESTAMPTZ,
    contract_status TEXT, --picklist
    current_coach_retention_percentage NUMERIC(18, 0),
    customer_id CHAR(18),
    date_joined TIMESTAMPTZ,
    dcr TEXT, --picklist
    dedicated_first_tee_learning_center TEXT, --picklist
    description TEXT,
    discounts_offered_to_participants TEXT, --picklist
    ein VARCHAR(9),
    number_of_employees NUMERIC(8, 0),
    enrollment NUMERIC(18, 0),
    ordered_equipment BOOLEAN,
    executive_director CHAR(18),
    financial_aid_active BOOLEAN,
    former_legal_entity TEXT,
    former_trade_names TEXT,
    ft_app_pilot BOOLEAN,
    golf_course_type TEXT, --picklist
    governance_structure TEXT, --picklist
    graduate BOOLEAN,
    home_school BOOLEAN,
    number_of_holes TEXT, --picklist
    if_other_fill_in_how_many VARCHAR(80),
    if_yes_how_long_is_the_lease VARCHAR(80),
    inactive_date TIMESTAMPTZ,
    insurance_expiration_date TIMESTAMPTZ,
    insurance_expires TIMESTAMPTZ,
    international_chapter BOOLEAN,
    kindergarten BOOLEAN,
    sf_last_modified_by_id CHAR(18),
    legacy_id VARCHAR(125),
    legal_entity_name VARCHAR(100),
    location_type TEXT, --picklist
    market_size TEXT, --picklist
    mdr_pid VARCHAR(25),
    membership_active BOOLEAN,
    membership_discount_amount NUMERIC(16, 2),
    membership_discount_percentage NUMERIC(16, 2),
    membership_end_date TIMESTAMPTZ,
    membership_offered BOOLEAN,
    membership_start_date TIMESTAMPTZ,
    military_discount_amount NUMERIC(16, 2),
    military_discount_percentage NUMERIC(16, 2),
    nces_id VARCHAR(12),
    new_parent_registration BOOLEAN,
    organization_city TEXT,
    organization_state TEXT, --picklist
    owner_id CHAR(18),
    ownership TEXT, --picklist
    parent_account CHAR(18),
    parent_account_id CHAR(18),
    partner_account BOOLEAN,
    partner_org_demographics_id CHAR(18),
    partner_program_type TEXT, --picklist
    partners_in_market TEXT, --multi picklist
    payments_accepted_in_person BOOLEAN,
    peer_group TEXT, --picklist
    performance_record_type TEXT,
    phone TEXT,
    discount_description TEXT,
    learning_center_description TEXT,
    pre_school BOOLEAN,
    primary_contact_id CHAR(18),
    primary_contact_email TEXT,
    primary_contact_email_address TEXT,
    program_director_id CHAR(18),
    program_location_key TEXT,
    program_location_type TEXT, --picklist
    reggie_account_id VARCHAR(30),
    reggie_id VARCHAR(30),
    reggie_location_id VARCHAR(100),
    reggie_name TEXT,
    region TEXT, --picklist
    service_area TEXT,
    shipping_street TEXT,
    shipping_city TEXT,
    shipping_state TEXT,
    shipping_postal_code TEXT,
    shipping_country TEXT,
    sibling_discount_amount NUMERIC(16, 2),
    sibling_discount_percentage NUMERIC(16, 2),
    signed_facility_use_agreement BOOLEAN,
    status TEXT, --picklist
    territory_partner_account TEXT,
    territory TEXT, --picklist
    time_zone TEXT, --picklist
    title_i BOOLEAN,
    type TEXT, --picklist
    lease_holder TEXT,
    youth_population INTEGER,
    ys_report_chapter_affiliation TEXT,
    is_deleted BOOLEAN,
    sf_created_timestamp TIMESTAMPTZ,
    sf_last_modified_timestamp TIMESTAMPTZ,
    sf_system_modstamp TIMESTAMPTZ,
    dss_ingestion_timestamp TIMESTAMPTZ
);