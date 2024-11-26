CREATE TABLE IF NOT EXISTS ft_ds_raw.validation_errors_sf_account (
    account_id TEXT,
    account_inactive_date TEXT,
    account_name TEXT,
    account_number TEXT,
    account_record_type_id TEXT,
    is_active TEXT,
    additional_trade_name_id TEXT,
    additional_trade_name_chapter_affiliation_id TEXT,
    billing_street TEXT,
    billing_city TEXT,
    billing_state TEXT,
    billing_postal_code TEXT,
    billing_country TEXT,
    board_chair TEXT,
    chapter_affiliation_id TEXT,
    chapter_country TEXT, --picklist but likely very long
    chapter_membership_price TEXT,
    chapter_owns_this_facility TEXT, -- picklist
    chapter_standing TEXT, --picklist
    contract_effective_date TEXT,
    contract_expiration_date__c TEXT,
    contract_status TEXT, --picklist
    current_coach_retention_percentage TEXT,
    customer_id TEXT,
    date_joined TEXT,
    dcr TEXT, --picklist
    dedicated_first_tee_learning_center TEXT, --picklist
    description TEXT,
    discounts_offered_to_participants TEXT, --picklist
    ein TEXT,
    number_of_employees TEXT,
    enrollment TEXT,
    ordered_equipment TEXT,
    executive_director TEXT,
    financial_aid_active TEXT,
    former_legal_entity TEXT,
    former_trade_names TEXT,
    ft_app_pilot TEXT,
    golf_course_type TEXT, --picklist
    governance_structure TEXT, --picklist
    graduate TEXT,
    home_school TEXT,
    number_of_holes TEXT, --picklist
    if_other_fill_in_how_many TEXT,
    if_yes_how_long_is_the_lease TEXT,
    inactive_date TEXT,
    insurance_expiration_date TEXT,
    insurance_expires TEXT,
    international_chapter TEXT,
    kindergarten TEXT,
    sf_last_modified_by_id TEXT,
    legacy_id TEXT,
    legal_entity_name TEXT,
    location_type TEXT, --picklist
    market_size TEXT, --picklist
    mdr_pid TEXT,
    membership_active TEXT,
    membership_discount_amount TEXT,
    membership_discount_percentage TEXT,
    membership_end_date TEXT,
    membership_offered TEXT,
    membership_start_date TEXT,
    military_discount_amount TEXT,
    military_discount_percentage TEXT,
    nces_id TEXT,
    new_parent_registration TEXT,
    organization_city TEXT,
    organization_state TEXT, --picklist
    owner_id TEXT,
    ownership TEXT, --picklist
    parent_account TEXT,
    parent_account_id TEXT,
    partner_account TEXT,
    partner_org_demographics_id TEXT,
    partner_program_type TEXT, --picklist
    partners_in_market TEXT, --multi picklist
    payments_accepted_in_person TEXT,
    peer_group TEXT, --picklist
    performance_record_type TEXT,
    phone TEXT,
    discount_description TEXT,
    learning_center_description TEXT,
    pre_school TEXT,
    primary_contact_id TEXT,
    primary_contact_email TEXT,
    primary_contact_email_address TEXT,
    program_director_id TEXT,
    program_location_key TEXT,
    program_location_type TEXT, --picklist
    reggie_account_id TEXT,
    reggie_id TEXT,
    reggie_location_id TEXT,
    reggie_name TEXT,
    region TEXT, --picklist
    service_area TEXT,
    shipping_street TEXT,
    shipping_city TEXT,
    shipping_state TEXT,
    shipping_postal_code TEXT,
    shipping_country TEXT,
    sibling_discount_amount TEXT,
    sibling_discount_percentage TEXT,
    signed_facility_use_agreement TEXT,
    status TEXT, --picklist
    territory_partner_account TEXT,
    territory TEXT, --picklist
    time_zone TEXT, --picklist
    title_i TEXT,
    type TEXT, --picklist
    lease_holder TEXT,
    youth_population TEXT,
    ys_report_chapter_affiliation TEXT,
    is_deleted TEXT,
    sf_created_timestamp TEXT,
    sf_last_modified_timestamp TEXT,
    sf_system_modstamp TEXT,
    dss_ingestion_timestamp TIMESTAMPTZ,
    required_fields_validated BOOLEAN,
    optional_fields_validated BOOLEAN,
    fixed_in_source BOOLEAN
);