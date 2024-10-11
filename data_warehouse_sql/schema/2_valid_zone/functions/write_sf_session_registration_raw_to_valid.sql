CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_session_registration_raw_to_valid (load_threshold_timestamp TIMESTAMPTZ DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_session_registration_raw_to_valid;

    CREATE TEMP TABLE IF NOT EXISTS temp_sf_session_registration_raw_to_valid (
        --still import as all text because we want to be able to analyze it for if it will make it to valid based on dtype, and we can assume it will fit in text because that's how it already exists in raw
        session_registration_id_18 TEXT,
        original_session_charge_amount TEXT,
        contact_id_18 TEXT,
        cost_difference TEXT,
        sf_created_by_id TEXT,
        sf_created_timestamp TEXT,
        discount TEXT,
        item_price TEXT,
        sf_last_modified_by_id TEXT,
        sf_last_modified_timestamp TEXT,
        listing_session_id_18 TEXT,
        membership_registration_id_18 TEXT,
        session_registration_number TEXT,
        old_listing_session_id_18 TEXT,
        new_session_cost TEXT,
        reggie_registration_id TEXT,
        session_type TEXT,
        status TEXT,
        is_transferred TEXT,
        waitlist_process TEXT,
        original_session_registration_number TEXT,
        is_deleted TEXT,
        sf_system_modstamp TEXT,
        --dss_ingestion_timestamp can be assumed to still be timestamp because that's how it exists in raw
        dss_ingestion_timestamp TIMESTAMPTZ,
        required_fields_validated BOOLEAN,
        optional_fields_validated BOOLEAN
    );

    --this statement places all of the most recently uploaded records into the temp table
    INSERT INTO temp_sf_session_registration_raw_to_valid
    SELECT
        id AS session_registration_id_18,
        charge_amount__c AS original_session_charge_amount,
        contact__c AS contact_id_18,
        cost_difference__c AS cost_difference,
        createdbyid AS sf_created_by_id,
        createddate AS sf_created_timestamp,
        discount__c AS discount,
        item_price__c AS item_price,
        lastmodifiedbyid AS sf_last_modified_by_id,
        lastmodifieddate AS sf_last_modified_timestamp,
        listing_session__c AS listing_session_id_18,
        membership_registration__c AS membership_registration_id_18,
        name AS session_registration_number,
        new_listing_session__c AS old_listing_session_id_18,
        new_session_cost__c AS new_session_cost,
        reggie_registration_id__c AS reggie_registration_id,
        sessiontype_replicated__c AS session_type,
        status__c AS status,
        transferred__c AS is_transferred,
        waitlist_process__c AS waitlist_process,
        current_session_registration_number__c AS original_session_registration_number,
        isdeleted AS is_deleted,
        systemmodstamp AS sf_system_modstamp,
        dss_ingestion_timestamp,
        TRUE AS required_fields_validated,
        TRUE AS optional_fields_validated
    FROM ft_ds_raw.sf_session_registration
    WHERE
        dss_ingestion_timestamp >
        CASE
            WHEN load_threshold_timestamp IS NULL THEN (SELECT MAX(execution_timestamp) from ft_ds_valid.raw_to_valid_execution_log WHERE entity = 'sf_session_registration')
            ELSE load_threshold_timestamp
        END
    ;

    --this statement updates the required_fields_validated flag
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    --it is structured this way because if the required fields do not meet data quality, then they are not passed to valid and therefore do not need to be transformed further. Therefore, only the required_fields_validated flag needs to be updated.
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    required_fields_validated = FALSE
    WHERE
    --contact_id_18
        session_registration_id_18 IS NULL
        OR LENGTH(session_registration_id_18) <> 18
        OR session_registration_id_18 = ''
    --sf_system_modstamp
        OR sf_system_modstamp IS NULL
        OR NOT (SELECT ft_ds_admin.is_coercable_to_timestamptz(sf_system_modstamp))
    ;

    --these statements update the optional_fields_validated flag and swap invalid values to NULL
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    -- original_session_charge_amount
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    original_session_charge_amount = NULL
    WHERE
        original_session_charge_amount IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(original_session_charge_amount))
        OR original_session_charge_amount = ''
    ;
    -- contact_id_18
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    contact_id_18 = NULL
    WHERE
        contact_id_18 IS NULL
        OR LENGTH(contact_id_18) <> 18
        OR contact_id_18 = ''
    ;
    -- cost_difference
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    cost_difference = NULL
    WHERE
        cost_difference IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(cost_difference))
        OR cost_difference = ''
    ; 
    -- sf_created_by_id
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_created_by_id = NULL
    WHERE
        sf_created_by_id IS NULL
        OR LENGTH(sf_created_by_id) <> 18
        OR sf_created_by_id = ''
    ;
    -- sf_created_timestamp
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_created_timestamp = NULL
    WHERE
        sf_created_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_created_timestamp))
        OR sf_created_timestamp = ''
    ;
    -- discount
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    discount = NULL
    WHERE
        discount IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(discount))
        OR discount = ''
    ;
    -- item_price
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    item_price = NULL
    WHERE
        item_price IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(item_price))
        OR item_price = ''
    ;
    -- sf_last_modified_by_id
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_by_id = NULL
    WHERE
        sf_last_modified_by_id IS NULL
        OR LENGTH(sf_last_modified_by_id) <> 18
        OR sf_last_modified_by_id = ''
    ;
    -- sf_last_modified_timestamp
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_timestamp = NULL
    WHERE
        sf_last_modified_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_last_modified_timestamp))
        OR sf_last_modified_timestamp = ''
    ;
    -- listing_session_id_18
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    listing_session_id_18 = NULL
    WHERE
        listing_session_id_18 IS NULL
        OR LENGTH(listing_session_id_18) <> 18
        OR listing_session_id_18 = ''
    ;
    -- membership_registration_id_18
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    membership_registration_id_18 = NULL
    WHERE
        membership_registration_id_18 IS NULL
        OR LENGTH(membership_registration_id_18) <> 18
        OR membership_registration_id_18 = ''
    ;
    -- session_registration_number
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    session_registration_number = NULL
    WHERE
        session_registration_number IS NULL
        OR session_registration_number = ''
    ;
    -- old_listing_session_id_18
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    old_listing_session_id_18 = NULL
    WHERE
        old_listing_session_id_18 IS NULL
        OR LENGTH(old_listing_session_id_18) <> 18
        OR old_listing_session_id_18 = ''
    ;
    -- new_session_cost
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    new_session_cost = NULL
    WHERE
        new_session_cost IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(new_session_cost))
        OR new_session_cost = ''
    ;
    -- reggie_registration_id
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    reggie_registration_id = NULL
    WHERE
        reggie_registration_id IS NULL
        OR reggie_registration_id = ''
    ;
    -- session_type
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    session_type = NULL
    WHERE
        session_type IS NULL
        OR session_type = ''
    ;
    -- status
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    status = NULL
    WHERE
        status IS NULL
        OR status NOT IN ('In Process','On Waitlist','Waitlist Pending','Waitlist Cancelled','Financial Aid Applied','Financial Aid Pending','Registered','Cancelled','Abandoned','No Cart Item','Not Registered','Refund Issued')
        OR status = ''
    ;
    -- is_transferred
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_transferred = NULL
    WHERE
        is_transferred IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(is_transferred))
        OR is_transferred = ''
    ;
    -- waitlist_process
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    waitlist_process = NULL
    WHERE
        waitlist_process IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(waitlist_process))
        OR waitlist_process = ''
    ;
    -- original_session_registration_number
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    original_session_registration_number = NULL
    WHERE
        original_session_registration_number IS NULL
        OR LENGTH(original_session_registration_number) <> 18
        OR original_session_registration_number = ''
    ;
    -- is_deleted
    UPDATE temp_sf_session_registration_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_deleted = NULL
    WHERE
        is_deleted IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(is_deleted))
        OR is_deleted = ''
    ;
    -- dss_ingestion_timestamp
    UPDATE temp_sf_session_registration_raw_to_valid
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
    INSERT INTO ft_ds_raw.validation_errors_sf_session_registration
    SELECT
        session_registration_id_18,
        original_session_charge_amount,
        contact_id_18,
        cost_difference,
        sf_created_by_id,
        sf_created_timestamp,
        discount,
        item_price,
        sf_last_modified_by_id,
        sf_last_modified_timestamp,
        listing_session_id_18,
        membership_registration_id_18,
        session_registration_number,
        old_listing_session_id_18,
        new_session_cost,
        reggie_registration_id,
        session_type,
        status,
        is_transferred,
        waitlist_process,
        original_session_registration_number,
        is_deleted,
        sf_system_modstamp,
        dss_ingestion_timestamp,
        required_fields_validated,
        optional_fields_validated,
        FALSE AS fixed_in_source
    FROM temp_sf_session_registration_raw_to_valid
    WHERE
        required_fields_validated = FALSE
        OR optional_fields_validated = FALSE
    ;

    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_session_registration_raw_to_valid_validated;

    --now that we've flagged the valid data and set optional invalid fields to NULL, we can cast the values to their valid types
    CREATE TABLE IF NOT EXISTS temp_sf_session_registration_raw_to_valid_validated (
        session_registration_id_18 CHAR(18),
        original_session_charge_amount NUMERIC(16,2),
        contact_id_18 CHAR(18),
        cost_difference NUMERIC(16,2),
        sf_created_by_id CHAR(18),
        sf_created_timestamp TIMESTAMPTZ,
        discount NUMERIC(18,0),
        item_price NUMERIC(18,0),
        sf_last_modified_by_id CHAR(18),
        sf_last_modified_timestamp TIMESTAMPTZ,
        listing_session_id_18 CHAR(18),
        membership_registration_id_18 CHAR(18),
        session_registration_number VARCHAR(20),
        old_listing_session_id_18 CHAR(18),
        new_session_cost NUMERIC(16,2),
        reggie_registration_id VARCHAR(100),
        session_type VARCHAR(100),
        status VARCHAR(100), --picklist
        is_transferred BOOLEAN,
        waitlist_process BOOLEAN,
        original_session_registration_number CHAR(18),
        is_deleted BOOLEAN,
        sf_system_modstamp TIMESTAMPTZ,
        dss_ingestion_timestamp TIMESTAMPTZ
    );

    --this statement then cleans the data to get the data types correct
    INSERT INTO temp_sf_session_registration_raw_to_valid_validated
    SELECT
        session_registration_id_18,
        CAST(original_session_charge_amount AS NUMERIC) AS original_session_charge_amount,
        contact_id_18,
        CAST(cost_difference AS NUMERIC) AS cost_difference,
        sf_created_by_id,
        CAST(sf_created_timestamp AS TIMESTAMPTZ) AS sf_created_timestamp,
        CAST(discount AS NUMERIC) AS discount,
        CAST(item_price AS NUMERIC) AS item_price,
        sf_last_modified_by_id,
        CAST(sf_last_modified_timestamp AS TIMESTAMPTZ) AS sf_last_modified_timestamp,
        listing_session_id_18,
        membership_registration_id_18,
        session_registration_number,
        old_listing_session_id_18,
        CAST(new_session_cost AS NUMERIC) AS new_session_cost,
        reggie_registration_id,
        session_type,
        status,
        CAST(is_transferred AS BOOLEAN) AS is_transferred,
        CAST(waitlist_process AS BOOLEAN) AS waitlist_process,
        original_session_registration_number,
        CAST(is_deleted AS BOOLEAN) AS is_deleted,
        CAST(sf_system_modstamp AS TIMESTAMPTZ) AS sf_system_modstamp,
        dss_ingestion_timestamp
    FROM temp_sf_session_registration_raw_to_valid
    WHERE
        required_fields_validated = TRUE
    ;

    --this query gets the population correct for transitioning from raw to valid. It only includes records:
    -- that were inserted into the raw zone since the last raw->valid run
    -- that dont belong to testing chapters
    -- that are unique to each unique ID value (no dups)
    INSERT INTO ft_ds_valid.sf_session_registration
    SELECT
        all_values_but_dss_ingestion.*,
        dss_ingestion.dss_ingestion_timestamp
    FROM
    (SELECT
        session_registration_id_18,
        original_session_charge_amount,
        contact_id_18,
        cost_difference,
        sf_created_by_id,
        sf_created_timestamp,
        discount,
        item_price,
        sf_last_modified_by_id,
        sf_last_modified_timestamp,
        listing_session_id_18,
        membership_registration_id_18,
        session_registration_number,
        old_listing_session_id_18,
        new_session_cost,
        reggie_registration_id,
        session_type,
        status,
        is_transferred,
        waitlist_process,
        original_session_registration_number,
        is_deleted,
        sf_system_modstamp
    FROM temp_sf_session_registration_raw_to_valid_validated
    GROUP BY
        --this group by clause exists to eliminate duplicates since multiple records with the same Id and system_modstamp can exist
        --it is every field going into the valid table except dss_ingestion_timestamp
        session_registration_id_18,
        original_session_charge_amount,
        contact_id_18,
        cost_difference,
        sf_created_by_id,
        sf_created_timestamp,
        discount,
        item_price,
        sf_last_modified_by_id,
        sf_last_modified_timestamp,
        listing_session_id_18,
        membership_registration_id_18,
        session_registration_number,
        old_listing_session_id_18,
        new_session_cost,
        reggie_registration_id,
        session_type,
        status,
        is_transferred,
        waitlist_process,
        original_session_registration_number,
        is_deleted,
        sf_system_modstamp
    ) all_values_but_dss_ingestion
    JOIN
    (SELECT
        session_registration_id_18,
        MAX(sf_system_modstamp) as max_date
    FROM temp_sf_session_registration_raw_to_valid_validated
    GROUP BY
        session_registration_id_18
    ) max_dates
    ON all_values_but_dss_ingestion.session_registration_id_18 = max_dates.session_registration_id_18
    AND all_values_but_dss_ingestion.sf_system_modstamp = max_dates.max_date
    JOIN
    (SELECT
        session_registration_id_18,
        MAX(dss_ingestion_timestamp) as dss_ingestion_timestamp
    FROM temp_sf_session_registration_raw_to_valid_validated
    GROUP BY
        session_registration_id_18
    )dss_ingestion
    ON all_values_but_dss_ingestion.session_registration_id_18 = dss_ingestion.session_registration_id_18
    ON CONFLICT (session_registration_id_18) DO UPDATE SET
        original_session_charge_amount = EXCLUDED.original_session_charge_amount,
        contact_id_18 = EXCLUDED.contact_id_18,
        cost_difference = EXCLUDED.cost_difference,
        sf_created_by_id = EXCLUDED.sf_created_by_id,
        sf_created_timestamp = EXCLUDED.sf_created_timestamp,
        discount = EXCLUDED.discount,
        item_price = EXCLUDED.item_price,
        sf_last_modified_by_id = EXCLUDED.sf_last_modified_by_id,
        sf_last_modified_timestamp = EXCLUDED.sf_last_modified_timestamp,
        listing_session_id_18 = EXCLUDED.listing_session_id_18,
        membership_registration_id_18 = EXCLUDED.membership_registration_id_18,
        session_registration_number = EXCLUDED.session_registration_number,
        old_listing_session_id_18 = EXCLUDED.old_listing_session_id_18,
        new_session_cost = EXCLUDED.new_session_cost,
        reggie_registration_id = EXCLUDED.reggie_registration_id,
        session_type = EXCLUDED.session_type,
        status = EXCLUDED.status,
        is_transferred = EXCLUDED.is_transferred,
        waitlist_process = EXCLUDED.waitlist_process,
        original_session_registration_number = EXCLUDED.original_session_registration_number,
        is_deleted = EXCLUDED.is_deleted,
        sf_system_modstamp = EXCLUDED.sf_system_modstamp,
        dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp
    ;

    INSERT INTO ft_ds_valid.raw_to_valid_execution_log
    SELECT
    MAX(dss_ingestion_timestamp) AS execution_timestamp,
    'sf_session_registration' AS entity
    FROM ft_ds_valid.sf_session_registration
    ;
END;
$$;