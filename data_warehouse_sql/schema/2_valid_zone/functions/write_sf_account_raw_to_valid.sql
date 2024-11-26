CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_account_raw_to_valid (load_threshold_timestamp TIMESTAMPTZ DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_account_raw_to_valid;

    CREATE TEMP TABLE IF NOT EXISTS temp_sf_account_raw_to_valid AS
    SELECT
        Account_ID__c AS account_id,
        Account_Inactive_Date__c AS account_inactive_date,
        Name AS account_name,
        AccountNumber AS account_number,
        RecordTypeId AS account_record_type_id,
        Active__c AS is_active,
        Additional_Trade_Name__c AS additional_trade_name_id,
        Additional_Trade_NameChap_Affiliation__c AS additional_trade_name_chapter_affiliation_id,
        billingstreet AS billing_street,
        billingcity AS billing_city,
        billingstate AS billing_state,
        billingpostalcode AS billing_postal_code,
        billingcountry AS billing_country,
        Board_Chair__c AS board_chair,
        Chapter_Affiliation__c AS chapter_affiliation_id,
        Chapter_Country__c AS chapter_country, --picklist but likely very long
        Chapter_Membership_Price__c AS chapter_membership_price,
        Chapter_owns_this_facility__c AS chapter_owns_this_facility, -- picklist
        Chapter_Standing__c AS chapter_standing, --picklist
        Contract_Effective_Date__c AS contract_effective_date,
        Contract_Expiration_Date__c AS contract_expiration_date,
        Contract_Status__c AS contract_status, --picklist
        Current_Coach_Retention_Percentage__c AS current_coach_retention_percentage,
        Customer_ID__c AS customer_id,
        Date_Joined__c AS date_joined,
        DCR__c AS dcr, --picklist
        dedicated_First_Tee_Learning_Center__c AS dedicated_first_tee_learning_center, --picklist
        Description AS description,
        Discounts_offered_to_Participants__c AS discounts_offered_to_participants, --picklist
        EIN__c AS ein,
        NumberOfEmployees AS number_of_employees,
        Enrollment__c AS enrollment,
        Equipment__c AS ordered_equipment,
        Executive_Director__c AS executive_director,
        Financial_Aid_Active__c AS financial_aid_active,
        Former_Legal_Entity__c AS former_legal_entity,
        Former_Trade_Names__c AS former_trade_names,
        FT_App_Pilot__c AS ft_app_pilot,
        Golf_Course_Type__c AS golf_course_type, --picklist
        Governance_Structure__c AS governance_structure, --picklist
        Graduate__c AS graduate,
        Home_School__c AS home_school,
        How_many_holes__c AS number_of_holes, --picklist
        If_other_fill_in_how_many__c AS if_other_fill_in_how_many,
        If_yes_how_long_is_the_lease__c AS if_yes_how_long_is_the_lease,
        Inactive_Date__c AS inactive_date,
        Insurance_Expiration_Date__c AS insurance_expiration_date,
        Insurance_Expires__c AS insurance_expires,
        International_Chapter__c AS international_chapter,
        Kindergarten__c AS kindergarten,
        LastModifiedById AS sf_last_modified_by_id,
        Legacy_ID__c AS legacy_id,
        Legal_Entity_Name__c AS legal_entity_name,
        Location_Type__c AS location_type, --picklist
        Market_Size__c AS market_size, --picklist
        MDR_PID__c AS mdr_pid,
        Membership_Active__c AS membership_active,
        Membership_Discount_Amount__c AS membership_discount_amount,
        Membership_Discount_Percentage__c AS membership_discount_percentage,
        Membership_End_Date__c AS membership_end_date,
        Membership_Offered__c AS membership_offered,
        Membership_Start_Date__c AS membership_start_date,
        Military_Discount_Amount__c AS military_discount_amount,
        Military_Discount_Percentage__c AS military_discount_percentage,
        NCES_ID__c AS nces_id,
        New_Parent_Registration__C AS new_parent_registration,
        Organization_City__c AS organization_city,
        Organization_State__c AS organization_state, --picklist
        Owner_ID__c AS owner_id,
        Ownership AS ownership, --picklist
        ParentId AS parent_account,
        Parent_Account_ID_18__c AS parent_account_id,
        IsPartner AS partner_account,
        Partner_Org_Demographics__c AS partner_org_demographics_id,
        Partner_Program_Type__c AS partner_program_type, --picklist
        In_Market_Partners__c AS partners_in_market, --multi picklist
        Payments_Accepted_In_Person__c AS payments_accepted_in_person,
        Peer_Group__c AS peer_group, --picklist
        Performance_Record_Type__c AS performance_record_type,
        Phone AS phone,
        Please_describe_discount__c AS discount_description,
        Please_describe_the_learning_center__c AS learning_center_description,
        Pre_School__c AS pre_school,
        Primary_Contact__c AS primary_contact_id,
        Primary_Contact_Email__c AS primary_contact_email,
        Primary_Contact_email_address__c AS primary_contact_email_address,
        Program_Director__c AS program_director_id,
        Program_Location_Key__c AS program_location_key,
        Program_Location_Type__c AS program_location_type, --picklist
        Reggie_Account_Id__c AS reggie_account_id,
        Reggie_Id__c AS reggie_id,
        Reggie_Location_Id__c AS reggie_location_id,
        Reggie_Name__c AS reggie_name,
        Region__c AS region, --picklist
        Service_Area__c AS service_area,
        shippingstreet AS shipping_street,
        shippingcity AS shipping_city,
        shippingstate AS shipping_state,
        shippingpostalcode AS shipping_postal_code,
        shippingcountry AS shipping_country,
        Sibling_Discount_Amount__c AS sibling_discount_amount,
        Sibling_Discount_Percentage__c AS sibling_discount_percentage,
        HO_Contracted_Location__c AS signed_facility_use_agreement,
        Status__c AS status, --picklist
        Territory_Partner_Acct__c AS territory_partner_account,
        Territory__c AS territory, --picklist
        Time_Zone__c AS time_zone, --picklist
        Title_I__c AS title_i,
        Type AS type, --picklist
        Who_is_the_lease_with__c AS lease_holder,
        Youth_population__c AS youth_population,
        YS_Report_Chapter_Affiliation__c AS ys_report_chapter_affiliation,
        isdeleted AS is_deleted,
        createddate AS sf_created_timestamp,
        lastmodifieddate AS sf_last_modified_timestamp,
        systemmodstamp AS sf_system_modstamp,
        dss_ingestion_timestamp,
        TRUE AS required_fields_validated,
        TRUE AS optional_fields_validated
    FROM ft_ds_raw.sf_account
    WHERE
        dss_ingestion_timestamp >
        CASE
            WHEN load_threshold_timestamp IS NULL THEN (SELECT MAX(execution_timestamp) from ft_ds_valid.raw_to_valid_execution_log WHERE entity = 'sf_account')
            ELSE load_threshold_timestamp
        END
    ;

    --this statement updates the required_fields_validated flag
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    --it is structured this way because if the required fields do not meet data quality, then they are not passed to valid and therefore do not need to be transformed further. Therefore, only the required_fields_validated flag needs to be updated.
    UPDATE temp_sf_account_raw_to_valid
    SET
    required_fields_validated = FALSE
    WHERE
    --account_id
        account_id IS NULL
        OR LENGTH(account_id) <> 18
        OR account_id = ''
    --account_name
        OR account_name IS NULL
        OR account_name = ''
    --record_type_id
        OR account_record_type_id IS NULL
        OR LENGTH(account_record_type_id) <> 18
        OR account_record_type_id = ''
    --sf_system_modstamp
        OR sf_system_modstamp IS NULL
        OR NOT (SELECT ft_ds_admin.is_coercable_to_timestamptz(sf_system_modstamp))
    ;

    --these statements update the optional_fields_validated flag and swap invalid values to NULL
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    -- there are not validations for TEXT fields since those do not have constraints for data type validations
    -- account_inactive_date
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    account_inactive_date = NULL
    WHERE
        account_inactive_date IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(account_inactive_date)
        OR account_inactive_date = ''
    ;

    -- account_number
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    account_number = NULL
    WHERE
        account_number IS NULL
        OR LENGTH(account_number) > 40
        OR account_number = ''
    ;

    -- is_active
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_active = NULL
    WHERE
        is_active IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(is_active)
        OR is_active = ''
    ;

    -- additional_trade_name_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    additional_trade_name_id = NULL
    WHERE
        additional_trade_name_id IS NULL
        OR LENGTH(additional_trade_name_id) <> 18
        OR additional_trade_name_id = ''
    ;

    -- additional_trade_name_chapter_affiliation_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    additional_trade_name_chapter_affiliation_id = NULL
    WHERE
        additional_trade_name_chapter_affiliation_id IS NULL
        OR LENGTH(additional_trade_name_chapter_affiliation_id) <> 18
        OR additional_trade_name_chapter_affiliation_id = ''
    ;

    -- board_chair
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    board_chair = NULL
    WHERE
        board_chair IS NULL
        OR LENGTH(board_chair) <> 18
        OR board_chair = ''
    ;

    -- chapter_affiliation_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_affiliation_id = NULL
    WHERE
        chapter_affiliation_id IS NULL
        OR LENGTH(chapter_affiliation_id) <> 18
        OR chapter_affiliation_id = ''
    ;

    -- chapter_country
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_country = NULL
    WHERE
        chapter_country IS NULL
        OR chapter_country NOT IN ('United States', 'Canada')
        OR chapter_country = ''
    ;

    -- chapter_membership_price
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_membership_price = NULL
    WHERE
        chapter_membership_price IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(chapter_membership_price)
        OR chapter_membership_price = ''
    ;

    -- chapter_owns_this_facility
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_owns_this_facility = NULL
    WHERE
        chapter_owns_this_facility IS NULL
        OR chapter_owns_this_facility NOT IN ('Yes', 'No')
        OR chapter_owns_this_facility = ''
    ;

    -- chapter_standing
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_standing = NULL
    WHERE
        chapter_standing IS NULL
        OR chapter_standing NOT IN ('Good Standing', 'Not in Good Standing')
        OR chapter_standing = ''
    ;

    -- contract_effective_date
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    contract_effective_date = NULL
    WHERE
        contract_effective_date IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(contract_effective_date)
        OR contract_effective_date = ''
    ;

    -- contract_expiration_date
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    contract_expiration_date = NULL
    WHERE
        contract_expiration_date IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(contract_expiration_date)
        OR contract_expiration_date = ''
    ;
    
    -- contract_status
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    contract_status = NULL
    WHERE
        contract_status IS NULL
        OR contract_status NOT IN ('LOI', 'Contracted')
        OR contract_status = ''
    ;

    -- current_coach_retention_percentage
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    current_coach_retention_percentage = NULL
    WHERE
        current_coach_retention_percentage IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(current_coach_retention_percentage)
        OR current_coach_retention_percentage = ''
    ;

    -- customer_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    customer_id = NULL
    WHERE
        customer_id IS NULL
        OR LENGTH(customer_id) <> 18
        OR customer_id = ''
    ;

    -- date_joined
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    date_joined = NULL
    WHERE
        date_joined IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(date_joined)
        OR date_joined = ''
    ;

    -- dcr
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    dcr = NULL
    WHERE
        dcr IS NULL
        OR dcr NOT IN ('Joshua McDade','Karlton Creech','Rob Neal','Susan Rasmus','Teal Thron','Tiffani English','Tom Lawrence','TBD', 'Julie Jansa', 'Mike Blackwell')
        OR dcr = ''
    ;

    -- dedicated_first_tee_learning_center
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    dedicated_first_tee_learning_center = NULL
    WHERE
        dedicated_first_tee_learning_center IS NULL
        OR dedicated_first_tee_learning_center NOT IN ('Yes', 'No')
        OR dedicated_first_tee_learning_center = ''
    ;

    -- discounts_offered_to_participants
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    discounts_offered_to_participants = NULL
    WHERE
        discounts_offered_to_participants IS NULL
        OR discounts_offered_to_participants NOT IN ('Golf Course Access','Driving Range','Both')
        OR discounts_offered_to_participants = ''
    ;

    -- ein
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    ein = NULL
    WHERE
        ein IS NULL
        OR LENGTH(ein) > 9
        OR ein = ''
    ;

    -- number_of_employees
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    number_of_employees = NULL
    WHERE
        number_of_employees IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(number_of_employees)
        OR number_of_employees = ''
    ;

    -- enrollment
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    enrollment = NULL
    WHERE
        enrollment IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(enrollment)
        OR enrollment = ''
    ;

    -- ordered_equipment
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    ordered_equipment = NULL
    WHERE
        ordered_equipment IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(ordered_equipment)
        OR ordered_equipment = ''
    ;

    -- executive_director
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    executive_director = NULL
    WHERE
        executive_director IS NULL
        OR LENGTH(executive_director) <> 18
        OR executive_director = ''
    ;

    -- financial_aid_active
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    financial_aid_active = NULL
    WHERE
        financial_aid_active IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(financial_aid_active)
        OR financial_aid_active = ''
    ;

    -- ft_app_pilot
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    ft_app_pilot = NULL
    WHERE
        ft_app_pilot IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(ft_app_pilot)
        OR ft_app_pilot = ''
    ;

    -- golf_course_type
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    golf_course_type = NULL
    WHERE
        golf_course_type IS NULL
        OR golf_course_type NOT IN ('Regulation Course','Short Course (primarily par 3s)','Hybrid Course')
        OR golf_course_type = ''
    ;

    -- governance_structure
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    governance_structure = NULL
    WHERE
        governance_structure IS NULL
        OR governance_structure NOT IN ('Government | Chapter-Operated','Government | Facility-Operated','Multi-Purpose | Chapter-Operated','Multi-Purpose | Facility-Operated','Single-Purpose | Chapter-Operated','Single-Purpose | Facility-Operated')
        OR governance_structure = ''
    ;

    -- graduate
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    graduate = NULL
    WHERE
        graduate IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(graduate)
        OR graduate = ''
    ;

    -- home_school
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    home_school = NULL
    WHERE
        home_school IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(home_school)
        OR home_school = ''
    ;

    -- number_of_holes
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    number_of_holes = NULL
    WHERE
        number_of_holes IS NULL
        OR number_of_holes NOT IN ('Alternative Golf Facility','3 Holes','6 Holes','9 Holes','12 Holes','15 Holes','18 Holes','27 Holes','36 Holes','Other')
        OR number_of_holes = ''
    ;

    -- if_other_fill_in_how_many
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    if_other_fill_in_how_many = NULL
    WHERE
        if_other_fill_in_how_many IS NULL
        OR LENGTH(if_other_fill_in_how_many) > 80
        OR if_other_fill_in_how_many = ''
    ;

    -- if_yes_how_long_is_the_lease
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    if_yes_how_long_is_the_lease = NULL
    WHERE
        if_yes_how_long_is_the_lease IS NULL
        OR LENGTH(if_yes_how_long_is_the_lease) > 80
        OR if_yes_how_long_is_the_lease = ''
    ;

    -- inactive_date
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    inactive_date = NULL
    WHERE
        inactive_date IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(inactive_date)
        OR inactive_date = ''
    ;

    -- insurance_expiration_date
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    insurance_expiration_date = NULL
    WHERE
        insurance_expiration_date IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(insurance_expiration_date)
        OR insurance_expiration_date = ''
    ;

    -- insurance_expires
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    insurance_expires = NULL
    WHERE
        insurance_expires IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(insurance_expires)
        OR insurance_expires = ''
    ;

    -- international_chapter
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    international_chapter = NULL
    WHERE
        international_chapter IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(international_chapter)
        OR international_chapter = ''
    ;

    -- kindergarten
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    kindergarten = NULL
    WHERE
        kindergarten IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(kindergarten)
        OR kindergarten = ''
    ;

    -- sf_last_modified_by_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_by_id = NULL
    WHERE
        sf_last_modified_by_id IS NULL
        OR LENGTH(sf_last_modified_by_id) <> 18
        OR sf_last_modified_by_id = ''
    ;

    -- legacy_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    legacy_id = NULL
    WHERE
        legacy_id IS NULL
        OR LENGTH(legacy_id) > 125
        OR legacy_id = ''
    ;

    -- legal_entity_name
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    legal_entity_name = NULL
    WHERE
        legal_entity_name IS NULL
        OR LENGTH(legal_entity_name) > 100
        OR legal_entity_name = ''
    ;

    -- location_type
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    location_type = NULL
    WHERE
        location_type IS NULL
        OR location_type NOT IN ('Elementary School','Middle School','K-8 School','High School','K-12 School','School District','Church','YMCA','Boys & Girls Club','Parks & Rec','Golf Course','Other')
        OR location_type = ''
    ;

    -- market_size
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    market_size = NULL
    WHERE
        market_size IS NULL
        OR market_size NOT IN ('Larger Population','Smaller Population')
        OR market_size = ''
    ;

    -- mdr_pid
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    mdr_pid = NULL
    WHERE
        mdr_pid IS NULL
        OR LENGTH(mdr_pid) > 25
        OR mdr_pid = ''
    ;

    -- membership_active
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    membership_active = NULL
    WHERE
        membership_active IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(membership_active)
        OR membership_active = ''
    ;

    -- membership_discount_amount
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    membership_discount_amount = NULL
    WHERE
        membership_discount_amount IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(membership_discount_amount)
        OR membership_discount_amount = ''
    ;

    -- membership_discount_percentage
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    membership_discount_percentage = NULL
    WHERE
        membership_discount_percentage IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(membership_discount_percentage)
        OR membership_discount_percentage = ''
    ;

    -- membership_end_date
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    membership_end_date = NULL
    WHERE
        membership_end_date IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(membership_end_date)
        OR membership_end_date = ''
    ;

    -- membership_offered
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    membership_offered = NULL
    WHERE
        membership_offered IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(membership_offered)
        OR membership_offered = ''
    ;

    -- membership_start_date
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    membership_start_date = NULL
    WHERE
        membership_start_date IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(membership_start_date)
        OR membership_start_date = ''
    ;

    -- military_discount_amount
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    military_discount_amount = NULL
    WHERE
        military_discount_amount IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(military_discount_amount)
        OR military_discount_amount = ''
    ;

    -- military_discount_percentage
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    military_discount_percentage = NULL
    WHERE
        military_discount_percentage IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(military_discount_percentage)
        OR military_discount_percentage = ''
    ;

    -- nces_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    nces_id = NULL
    WHERE
        nces_id IS NULL
        OR LENGTH(nces_id) > 12
        OR nces_id = ''
    ;

    -- new_parent_registration
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    new_parent_registration = NULL
    WHERE
        new_parent_registration IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(new_parent_registration)
        OR new_parent_registration = ''
    ;

    -- owner_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    owner_id = NULL
    WHERE
        owner_id IS NULL
        OR LENGTH(owner_id) <> 18
        OR owner_id = ''
    ;

    -- ownership
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    ownership = NULL
    WHERE
        ownership IS NULL
        OR ownership NOT IN ('Public','Private','Subsidiary','Other')
        OR ownership = ''
    ;

    -- parent_account
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    parent_account = NULL
    WHERE
        parent_account IS NULL
        OR LENGTH(parent_account) <> 18
        OR parent_account = ''
    ;

    -- parent_account_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    parent_account_id = NULL
    WHERE
        parent_account_id IS NULL
        OR LENGTH(parent_account_id) <> 18
        OR parent_account_id = ''
    ;

    -- partner_account
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    partner_account = NULL
    WHERE
        partner_account IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(partner_account)
        OR partner_account = ''
    ;

    -- partner_org_demographics_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    partner_org_demographics_id = NULL
    WHERE
        partner_org_demographics_id IS NULL
        OR LENGTH(partner_org_demographics_id) <> 18
        OR partner_org_demographics_id = ''
    ;

    -- partner_program_type
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    partner_program_type = NULL
    WHERE
        partner_program_type IS NULL
        OR partner_program_type NOT IN ('School Program','Community Program','Chapter Led','School Program & Community Program')
        OR partner_program_type = ''
    ;

    -- partners_in_market
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    partners_in_market = NULL
    WHERE
        partners_in_market IS NULL
        OR partner_program_type NOT IN ('TopGolf','PGA TOUR Superstore;TopGolf', 'PGA TOUR Superstore')
        OR partners_in_market = ''
    ;

    -- payments_accepted_in_person
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    payments_accepted_in_person = NULL
    WHERE
        payments_accepted_in_person IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(payments_accepted_in_person)
        OR payments_accepted_in_person = ''
    ;

    -- peer_group
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    peer_group = NULL
    WHERE
        peer_group IS NULL
        OR peer_group NOT IN ('1','2','3','4','5','6')
        OR peer_group = ''
    ;

    -- pre_school
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    pre_school = NULL
    WHERE
        pre_school IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(pre_school)
        OR pre_school = ''
    ;

    -- primary_contact_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    primary_contact_id = NULL
    WHERE
        primary_contact_id IS NULL
        OR LENGTH(primary_contact_id) <> 18
        OR primary_contact_id = ''
    ;

    -- program_director_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    program_director_id = NULL
    WHERE
        program_director_id IS NULL
        OR LENGTH(program_director_id) <> 18
        OR program_director_id = ''
    ;

    -- program_location_type
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    program_location_type = NULL
    WHERE
        program_location_type IS NULL
        OR program_location_type NOT IN ('Chapter-Operated','Facility-Operated')
        OR program_location_type = ''
    ;

    -- reggie_account_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    reggie_account_id = NULL
    WHERE
        reggie_account_id IS NULL
        OR LENGTH(reggie_account_id) > 30
        OR reggie_account_id = ''
    ;

    -- reggie_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    reggie_id = NULL
    WHERE
        reggie_id IS NULL
        OR LENGTH(reggie_id) > 30
        OR reggie_id = ''
    ;

    -- reggie_location_id
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    reggie_location_id = NULL
    WHERE
        reggie_location_id IS NULL
        OR LENGTH(reggie_location_id) > 100
        OR reggie_location_id = ''
    ;

    -- region
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    region = NULL
    WHERE
        region IS NULL
        OR region not in ('Central Atlantic','Central Plains','Great Lakes','International','Mountain','Northeast','Pacific','River','Southeast')
        OR region = ''
    ;

    -- sibling_discount_amount
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sibling_discount_amount = NULL
    WHERE
        sibling_discount_amount IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(sibling_discount_amount)
        OR sibling_discount_amount = ''
    ;

    -- sibling_discount_percentage
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sibling_discount_percentage = NULL
    WHERE
        sibling_discount_percentage IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(sibling_discount_percentage)
        OR sibling_discount_percentage = ''
    ;

    -- signed_facility_use_agreement
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    signed_facility_use_agreement = NULL
    WHERE
        signed_facility_use_agreement IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(signed_facility_use_agreement)
        OR signed_facility_use_agreement = ''
    ;

    -- status
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    status = NULL
    WHERE
        status IS NULL
        OR status not in ('Pending','In Progress','Active','Withdrawn','Inactive')
        OR status = ''
    ;

    -- territory
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    territory = NULL
    WHERE
        territory IS NULL
        OR territory not in ('Eastern','International - Eastern','International - Western','Western')
        OR territory = ''
    ;

    -- time_zone
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    time_zone = NULL
    WHERE
        time_zone IS NULL
        OR time_zone not in ('Close','(GMT-03:00) Atlantic Daylight Time (America/Halifax)','(GMT-04:00) Atlantic Standard Time (America/Puerto_Rico)','(GMT-04:00) Eastern Daylight Time (America/New_York)','(GMT-05:00) Eastern Standard Time (America/Panama)','(GMT-05:00) Central Daylight Time (America/Chicago)','(GMT-05:00) Central Daylight Time (America/Mexico_City)','(GMT-06:00) Central Standard Time (America/El_Salvador)','(GMT-06:00) Mountain Daylight Time (America/Denver)','(GMT-07:00) Mountain Standard Time (America/Phoenix)','(GMT-07:00) Pacific Daylight Time (America/Los_Angeles)','(GMT-08:00) Alaska Daylight Time (America/Anchorage)','(GMT-9:00) Hawaii-Aleutian Standard Time (America/Adak)','(GMT-10:00) Hawaii-Aleutian Standard Time (Pacific/Honolulu)','(GMT+01:00) Western European Summer Time (Europe/Lisbon)','(GMT+09:00) Korea Standard Time (Asia/Seoul)','(GMT+09:00) Japan Standard Time (Asia/Tokyo)','(GMT+11:00) Australian Eastern Daylight Time (Australia/Sydney)')
        OR time_zone = ''
    ;

    -- title_i
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    title_i = NULL
    WHERE
        title_i IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(title_i)
        OR title_i = ''
    ;

    -- type
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    type = NULL
    WHERE
        type IS NULL
        OR type not in ('Prospect','Customer - Direct','Customer - Channel','Channel Partner / Reseller','Installation Partner','Technology Partner','Other','Individual','Household','Corporate','Government','Nonprofit','Foundation','Drive','School','Military Base')
        OR type = ''
    ;

    -- youth_population
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    youth_population = NULL
    WHERE
        youth_population IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(youth_population)
        OR youth_population = ''
    ;

    -- is_deleted
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_deleted = NULL
    WHERE
        is_deleted IS NULL
        OR NOT ft_ds_admin.is_coercable_to_boolean(is_deleted)
        OR is_deleted = ''
    ;

    -- sf_created_timestamp
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_created_timestamp = NULL
    WHERE
        sf_created_timestamp IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(sf_created_timestamp)
        OR sf_created_timestamp = ''
    ;

    -- sf_last_modified_timestamp
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_timestamp = NULL
    WHERE
        sf_last_modified_timestamp IS NULL
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(sf_last_modified_timestamp)
        OR sf_last_modified_timestamp = ''
    ;

    -- dss_ingestion_timestamp
    UPDATE temp_sf_account_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    dss_ingestion_timestamp = NULL
    WHERE
        -- dont include the coerced_to_timestamp() check because this field is already a timestamp
        dss_ingestion_timestamp IS NULL
    ;

    --copy all records where required_fields_validated = FALSE OR optional_fields_validated = FALSE to a permanent errored table
    --this can be reported on later to let data source owners fix the data upstream, where it will be re-ingested and fixed throughout all zones of the data warehouse
    --the fixed_in_source field can be updated when a data owner fixes the record in the source.
    INSERT INTO ft_ds_raw.validation_errors_sf_account
    SELECT
        account_id,
        account_inactive_date,
        account_name,
        account_number,
        account_record_type_id,
        is_active,
        additional_trade_name_id,
        additional_trade_name_chapter_affiliation_id,
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country,
        board_chair,
        chapter_affiliation_id,
        chapter_country,
        chapter_membership_price,
        chapter_owns_this_facility,
        chapter_standing,
        contract_effective_date,
        contract_expiration_date,
        contract_status,
        current_coach_retention_percentage,
        customer_id,
        date_joined,
        dcr,
        dedicated_first_tee_learning_center,
        description,
        discounts_offered_to_participants,
        ein,
        number_of_employees,
        enrollment,
        ordered_equipment,
        executive_director,
        financial_aid_active,
        former_legal_entity,
        former_trade_names,
        ft_app_pilot,
        golf_course_type,
        governance_structure,
        graduate,
        home_school,
        number_of_holes,
        if_other_fill_in_how_many,
        if_yes_how_long_is_the_lease,
        inactive_date,
        insurance_expiration_date,
        insurance_expires,
        international_chapter,
        kindergarten,
        sf_last_modified_by_id,
        legacy_id,
        legal_entity_name,
        location_type,
        market_size,
        mdr_pid,
        membership_active,
        membership_discount_amount,
        membership_discount_percentage,
        membership_end_date,
        membership_offered,
        membership_start_date,
        military_discount_amount,
        military_discount_percentage,
        nces_id,
        new_parent_registration,
        organization_city,
        organization_state,
        owner_id,
        ownership,
        parent_account,
        parent_account_id,
        partner_account,
        partner_org_demographics_id,
        partner_program_type,
        partners_in_market,
        payments_accepted_in_person,
        peer_group,
        performance_record_type,
        phone,
        discount_description,
        learning_center_description,
        pre_school,
        primary_contact_id,
        primary_contact_email,
        primary_contact_email_address,
        program_director_id,
        program_location_key,
        program_location_type,
        reggie_account_id,
        reggie_id,
        reggie_location_id,
        reggie_name,
        region,
        service_area,
        shipping_street,
        shipping_city,
        shipping_state,
        shipping_postal_code,
        shipping_country,
        sibling_discount_amount,
        sibling_discount_percentage,
        signed_facility_use_agreement,
        status,
        territory_partner_account,
        territory,
        time_zone,
        title_i,
        type,
        lease_holder,
        youth_population,
        ys_report_chapter_affiliation,
        is_deleted,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp,
        dss_ingestion_timestamp,
        required_fields_validated,
        optional_fields_validated,
        FALSE AS fixed_in_source
    FROM temp_sf_account_raw_to_valid
    WHERE
        required_fields_validated = FALSE
        OR optional_fields_validated = FALSE
    ;

    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_account_raw_to_valid_validated;

    --now that we've flagged the valid data and set optional invalid fields to NULL, we can cast the values to their valid types
    CREATE TABLE IF NOT EXISTS temp_sf_account_raw_to_valid_validated AS 
    SELECT
        account_id,
        CAST(account_inactive_date AS TIMESTAMPTZ) AS account_inactive_date,
        account_name,
        account_number,
        account_record_type_id,
        CAST(is_active AS BOOLEAN) AS is_active,
        additional_trade_name_id,
        additional_trade_name_chapter_affiliation_id,
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country,
        board_chair,
        chapter_affiliation_id,
        chapter_country,
        CAST(chapter_membership_price AS NUMERIC) AS chapter_membership_price,
        chapter_owns_this_facility,
        chapter_standing,
        CAST(contract_effective_date AS TIMESTAMPTZ) AS contract_effective_date,
        CAST(contract_expiration_date AS TIMESTAMPTZ) AS contract_expiration_date,
        contract_status,
        CAST(current_coach_retention_percentage AS NUMERIC) AS current_coach_retention_percentage,
        customer_id,
        CAST(date_joined AS TIMESTAMPTZ) AS date_joined,
        dcr,
        dedicated_first_tee_learning_center,
        description,
        discounts_offered_to_participants,
        ein,
        CAST(number_of_employees AS NUMERIC) AS number_of_employees,
        CAST(enrollment AS NUMERIC) AS enrollment,
        CAST(ordered_equipment AS BOOLEAN) AS ordered_equipment,
        executive_director,
        CAST(financial_aid_active AS BOOLEAN) AS financial_aid_active,
        former_legal_entity,
        former_trade_names,
        CAST(ft_app_pilot AS BOOLEAN) AS ft_app_pilot,
        golf_course_type,
        governance_structure,
        CAST(graduate AS BOOLEAN) AS graduate,
        CAST(home_school AS BOOLEAN) AS home_school,
        number_of_holes,
        if_other_fill_in_how_many,
        if_yes_how_long_is_the_lease,
        CAST(inactive_date AS TIMESTAMPTZ) AS inactive_date,
        CAST(insurance_expiration_date AS TIMESTAMPTZ) AS insurance_expiration_date,
        CAST(insurance_expires AS TIMESTAMPTZ) AS insurance_expires,
        CAST(international_chapter AS BOOLEAN) AS international_chapter,
        CAST(kindergarten AS BOOLEAN) AS kindergarten,
        sf_last_modified_by_id,
        legacy_id,
        legal_entity_name,
        location_type,
        market_size,
        mdr_pid,
        CAST(membership_active AS BOOLEAN) AS membership_active,
        CAST(membership_discount_amount AS NUMERIC) AS membership_discount_amount,
        CAST(membership_discount_percentage AS NUMERIC) AS membership_discount_percentage,
        CAST(membership_end_date AS TIMESTAMPTZ) AS membership_end_date,
        CAST(membership_offered AS BOOLEAN) AS membership_offered,
        CAST(membership_start_date AS TIMESTAMPTZ) AS membership_start_date,
        CAST(military_discount_amount AS NUMERIC) AS military_discount_amount,
        CAST(military_discount_percentage AS NUMERIC) AS military_discount_percentage,
        nces_id,
        CAST(new_parent_registration AS BOOLEAN) AS new_parent_registration,
        organization_city,
        organization_state,
        owner_id,
        ownership,
        parent_account,
        parent_account_id,
        CAST(partner_account AS BOOLEAN) AS partner_account,
        partner_org_demographics_id,
        partner_program_type,
        partners_in_market,
        CAST(payments_accepted_in_person AS BOOLEAN) AS payments_accepted_in_person,
        peer_group,
        performance_record_type,
        phone,
        discount_description,
        learning_center_description,
        CAST(pre_school AS BOOLEAN) AS pre_school,
        primary_contact_id,
        primary_contact_email,
        primary_contact_email_address,
        program_director_id,
        program_location_key,
        program_location_type,
        reggie_account_id,
        reggie_id,
        reggie_location_id,
        reggie_name,
        region,
        service_area,
        shipping_street,
        shipping_city,
        shipping_state,
        shipping_postal_code,
        shipping_country,
        CAST(sibling_discount_amount AS NUMERIC) AS sibling_discount_amount,
        CAST(sibling_discount_percentage AS NUMERIC) AS sibling_discount_percentage,
        CAST(signed_facility_use_agreement AS BOOLEAN) AS signed_facility_use_agreement,
        status,
        territory_partner_account,
        territory,
        time_zone,
        CAST(title_i AS BOOLEAN) AS title_i,
        type,
        lease_holder,
        CAST(youth_population AS INTEGER) AS youth_population,
        ys_report_chapter_affiliation,
        CAST(is_deleted AS BOOLEAN) AS is_deleted,
        CAST(sf_created_timestamp AS TIMESTAMPTZ) AS sf_created_timestamp,
        CAST(sf_last_modified_timestamp AS TIMESTAMPTZ) AS sf_last_modified_timestamp,
        CAST(sf_system_modstamp AS TIMESTAMPTZ) AS sf_system_modstamp,
        dss_ingestion_timestamp
    FROM temp_sf_account_raw_to_valid
    WHERE
        required_fields_validated = TRUE
    ;

    --this query gets the population correct for transitioning from raw to valid. It only includes records:
    -- that were inserted into the raw zone since the last raw->valid run
    -- that are unique to each unique ID value (no dups)
    INSERT INTO ft_ds_valid.sf_account
    SELECT
        all_values_but_dss_ingestion.*,
        dss_ingestion.dss_ingestion_timestamp
    FROM
    (SELECT
        account_id,
        account_inactive_date,
        account_name,
        account_number,
        account_record_type_id,
        is_active,
        additional_trade_name_id,
        additional_trade_name_chapter_affiliation_id,
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country,
        board_chair,
        chapter_affiliation_id,
        chapter_country,
        chapter_membership_price,
        chapter_owns_this_facility,
        chapter_standing,
        contract_effective_date,
        contract_expiration_date,
        contract_status,
        current_coach_retention_percentage,
        customer_id,
        date_joined,
        dcr,
        dedicated_first_tee_learning_center,
        description,
        discounts_offered_to_participants,
        ein,
        number_of_employees,
        enrollment,
        ordered_equipment,
        executive_director,
        financial_aid_active,
        former_legal_entity,
        former_trade_names,
        ft_app_pilot,
        golf_course_type,
        governance_structure,
        graduate,
        home_school,
        number_of_holes,
        if_other_fill_in_how_many,
        if_yes_how_long_is_the_lease,
        inactive_date,
        insurance_expiration_date,
        insurance_expires,
        international_chapter,
        kindergarten,
        sf_last_modified_by_id,
        legacy_id,
        legal_entity_name,
        location_type,
        market_size,
        mdr_pid,
        membership_active,
        membership_discount_amount,
        membership_discount_percentage,
        membership_end_date,
        membership_offered,
        membership_start_date,
        military_discount_amount,
        military_discount_percentage,
        nces_id,
        new_parent_registration,
        organization_city,
        organization_state,
        owner_id,
        ownership,
        parent_account,
        parent_account_id,
        partner_account,
        partner_org_demographics_id,
        partner_program_type,
        partners_in_market,
        payments_accepted_in_person,
        peer_group,
        performance_record_type,
        phone,
        discount_description,
        learning_center_description,
        pre_school,
        primary_contact_id,
        primary_contact_email,
        primary_contact_email_address,
        program_director_id,
        program_location_key,
        program_location_type,
        reggie_account_id,
        reggie_id,
        reggie_location_id,
        reggie_name,
        region,
        service_area,
        shipping_street,
        shipping_city,
        shipping_state,
        shipping_postal_code,
        shipping_country,
        sibling_discount_amount,
        sibling_discount_percentage,
        signed_facility_use_agreement,
        status,
        territory_partner_account,
        territory,
        time_zone,
        title_i,
        type,
        lease_holder,
        youth_population,
        ys_report_chapter_affiliation,
        is_deleted,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp
    FROM temp_sf_account_raw_to_valid_validated
    GROUP BY
        --this group by clause exists to eliminate duplicates since multiple records with the same Id and system_modstamp can exist
        --it is every field going into the valid table except dss_ingestion_timestamp
        account_id,
        account_inactive_date,
        account_name,
        account_number,
        account_record_type_id,
        is_active,
        additional_trade_name_id,
        additional_trade_name_chapter_affiliation_id,
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country,
        board_chair,
        chapter_affiliation_id,
        chapter_country,
        chapter_membership_price,
        chapter_owns_this_facility,
        chapter_standing,
        contract_effective_date,
        contract_expiration_date,
        contract_status,
        current_coach_retention_percentage,
        customer_id,
        date_joined,
        dcr,
        dedicated_first_tee_learning_center,
        description,
        discounts_offered_to_participants,
        ein,
        number_of_employees,
        enrollment,
        ordered_equipment,
        executive_director,
        financial_aid_active,
        former_legal_entity,
        former_trade_names,
        ft_app_pilot,
        golf_course_type,
        governance_structure,
        graduate,
        home_school,
        number_of_holes,
        if_other_fill_in_how_many,
        if_yes_how_long_is_the_lease,
        inactive_date,
        insurance_expiration_date,
        insurance_expires,
        international_chapter,
        kindergarten,
        sf_last_modified_by_id,
        legacy_id,
        legal_entity_name,
        location_type,
        market_size,
        mdr_pid,
        membership_active,
        membership_discount_amount,
        membership_discount_percentage,
        membership_end_date,
        membership_offered,
        membership_start_date,
        military_discount_amount,
        military_discount_percentage,
        nces_id,
        new_parent_registration,
        organization_city,
        organization_state,
        owner_id,
        ownership,
        parent_account,
        parent_account_id,
        partner_account,
        partner_org_demographics_id,
        partner_program_type,
        partners_in_market,
        payments_accepted_in_person,
        peer_group,
        performance_record_type,
        phone,
        discount_description,
        learning_center_description,
        pre_school,
        primary_contact_id,
        primary_contact_email,
        primary_contact_email_address,
        program_director_id,
        program_location_key,
        program_location_type,
        reggie_account_id,
        reggie_id,
        reggie_location_id,
        reggie_name,
        region,
        service_area,
        shipping_street,
        shipping_city,
        shipping_state,
        shipping_postal_code,
        shipping_country,
        sibling_discount_amount,
        sibling_discount_percentage,
        signed_facility_use_agreement,
        status,
        territory_partner_account,
        territory,
        time_zone,
        title_i,
        type,
        lease_holder,
        youth_population,
        ys_report_chapter_affiliation,
        is_deleted,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp
    ) all_values_but_dss_ingestion
    JOIN
    (SELECT
        account_id,
        MAX(sf_system_modstamp) as max_date
    FROM temp_sf_account_raw_to_valid_validated
    GROUP BY
        account_id
    ) max_dates
    ON all_values_but_dss_ingestion.account_id = max_dates.account_id
    AND all_values_but_dss_ingestion.sf_system_modstamp = max_dates.max_date
    JOIN
    (SELECT
        account_id,
        MAX(dss_ingestion_timestamp) as dss_ingestion_timestamp
    FROM temp_sf_account_raw_to_valid_validated
    GROUP BY
        account_id
    )dss_ingestion
    ON all_values_but_dss_ingestion.account_id = dss_ingestion.account_id
    ON CONFLICT (account_id) DO UPDATE SET
        account_id = EXCLUDED.account_id,
        account_inactive_date = EXCLUDED.account_inactive_date,
        account_name = EXCLUDED.account_name,
        account_number = EXCLUDED.account_number,
        account_record_type_id = EXCLUDED.account_record_type_id,
        is_active = EXCLUDED.is_active,
        additional_trade_name_id = EXCLUDED.additional_trade_name_id,
        additional_trade_name_chapter_affiliation_id = EXCLUDED.additional_trade_name_chapter_affiliation_id,
        billing_street = EXCLUDED.billing_street,
        billing_city = EXCLUDED.billing_city,
        billing_state = EXCLUDED.billing_state,
        billing_postal_code = EXCLUDED.billing_postal_code,
        billing_country = EXCLUDED.billing_country,
        board_chair = EXCLUDED.board_chair,
        chapter_affiliation_id = EXCLUDED.chapter_affiliation_id,
        chapter_country = EXCLUDED.chapter_country,
        chapter_membership_price = EXCLUDED.chapter_membership_price,
        chapter_owns_this_facility = EXCLUDED.chapter_owns_this_facility,
        chapter_standing = EXCLUDED.chapter_standing,
        contract_effective_date = EXCLUDED.contract_effective_date,
        contract_expiration_date = EXCLUDED.contract_expiration_date,
        contract_status = EXCLUDED.contract_status,
        current_coach_retention_percentage = EXCLUDED.current_coach_retention_percentage,
        customer_id = EXCLUDED.customer_id,
        date_joined = EXCLUDED.date_joined,
        dcr = EXCLUDED.dcr,
        dedicated_first_tee_learning_center = EXCLUDED.dedicated_first_tee_learning_center,
        description = EXCLUDED.description,
        discounts_offered_to_participants = EXCLUDED.discounts_offered_to_participants,
        ein = EXCLUDED.ein,
        number_of_employees = EXCLUDED.number_of_employees,
        enrollment = EXCLUDED.enrollment,
        ordered_equipment = EXCLUDED.ordered_equipment,
        executive_director = EXCLUDED.executive_director,
        financial_aid_active = EXCLUDED.financial_aid_active,
        former_legal_entity = EXCLUDED.former_legal_entity,
        former_trade_names = EXCLUDED.former_trade_names,
        ft_app_pilot = EXCLUDED.ft_app_pilot,
        golf_course_type = EXCLUDED.golf_course_type,
        governance_structure = EXCLUDED.governance_structure,
        graduate = EXCLUDED.graduate,
        home_school = EXCLUDED.home_school,
        number_of_holes = EXCLUDED.number_of_holes,
        if_other_fill_in_how_many = EXCLUDED.if_other_fill_in_how_many,
        if_yes_how_long_is_the_lease = EXCLUDED.if_yes_how_long_is_the_lease,
        inactive_date = EXCLUDED.inactive_date,
        insurance_expiration_date = EXCLUDED.insurance_expiration_date,
        insurance_expires = EXCLUDED.insurance_expires,
        international_chapter = EXCLUDED.international_chapter,
        kindergarten = EXCLUDED.kindergarten,
        sf_last_modified_by_id = EXCLUDED.sf_last_modified_by_id,
        legacy_id = EXCLUDED.legacy_id,
        legal_entity_name = EXCLUDED.legal_entity_name,
        location_type = EXCLUDED.location_type,
        market_size = EXCLUDED.market_size,
        mdr_pid = EXCLUDED.mdr_pid,
        membership_active = EXCLUDED.membership_active,
        membership_discount_amount = EXCLUDED.membership_discount_amount,
        membership_discount_percentage = EXCLUDED.membership_discount_percentage,
        membership_end_date = EXCLUDED.membership_end_date,
        membership_offered = EXCLUDED.membership_offered,
        membership_start_date = EXCLUDED.membership_start_date,
        military_discount_amount = EXCLUDED.military_discount_amount,
        military_discount_percentage = EXCLUDED.military_discount_percentage,
        nces_id = EXCLUDED.nces_id,
        new_parent_registration = EXCLUDED.new_parent_registration,
        organization_city = EXCLUDED.organization_city,
        organization_state = EXCLUDED.organization_state,
        owner_id = EXCLUDED.owner_id,
        ownership = EXCLUDED.ownership,
        parent_account = EXCLUDED.parent_account,
        parent_account_id = EXCLUDED.parent_account_id,
        partner_account = EXCLUDED.partner_account,
        partner_org_demographics_id = EXCLUDED.partner_org_demographics_id,
        partner_program_type = EXCLUDED.partner_program_type,
        partners_in_market = EXCLUDED.partners_in_market,
        payments_accepted_in_person = EXCLUDED.payments_accepted_in_person,
        peer_group = EXCLUDED.peer_group,
        performance_record_type = EXCLUDED.performance_record_type,
        phone = EXCLUDED.phone,
        discount_description = EXCLUDED.discount_description,
        learning_center_description = EXCLUDED.learning_center_description,
        pre_school = EXCLUDED.pre_school,
        primary_contact_id = EXCLUDED.primary_contact_id,
        primary_contact_email = EXCLUDED.primary_contact_email,
        primary_contact_email_address = EXCLUDED.primary_contact_email_address,
        program_director_id = EXCLUDED.program_director_id,
        program_location_key = EXCLUDED.program_location_key,
        program_location_type = EXCLUDED.program_location_type,
        reggie_account_id = EXCLUDED.reggie_account_id,
        reggie_id = EXCLUDED.reggie_id,
        reggie_location_id = EXCLUDED.reggie_location_id,
        reggie_name = EXCLUDED.reggie_name,
        region = EXCLUDED.region,
        service_area = EXCLUDED.service_area,
        shipping_street = EXCLUDED.shipping_street,
        shipping_city = EXCLUDED.shipping_city,
        shipping_state = EXCLUDED.shipping_state,
        shipping_postal_code = EXCLUDED.shipping_postal_code,
        shipping_country = EXCLUDED.shipping_country,
        sibling_discount_amount = EXCLUDED.sibling_discount_amount,
        sibling_discount_percentage = EXCLUDED.sibling_discount_percentage,
        signed_facility_use_agreement = EXCLUDED.signed_facility_use_agreement,
        status = EXCLUDED.status,
        territory_partner_account = EXCLUDED.territory_partner_account,
        territory = EXCLUDED.territory,
        time_zone = EXCLUDED.time_zone,
        title_i = EXCLUDED.title_i,
        type = EXCLUDED.type,
        lease_holder = EXCLUDED.lease_holder,
        youth_population = EXCLUDED.youth_population,
        ys_report_chapter_affiliation = EXCLUDED.ys_report_chapter_affiliation,
        is_deleted = EXCLUDED.is_deleted,
        sf_created_timestamp = EXCLUDED.sf_created_timestamp,
        sf_last_modified_timestamp = EXCLUDED.sf_last_modified_timestamp,
        sf_system_modstamp = EXCLUDED.sf_system_modstamp
    ;

    INSERT INTO ft_ds_valid.raw_to_valid_execution_log
    SELECT
    MAX(dss_ingestion_timestamp) AS execution_timestamp,
    'sf_account' AS entity
    FROM ft_ds_valid.sf_account
    ;
END;
$$;