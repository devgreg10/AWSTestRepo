CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_listing_raw_to_valid (load_threshold_timestamp TIMESTAMPTZ DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_listing_raw_to_valid;

    CREATE TEMP TABLE IF NOT EXISTS temp_sf_listing_raw_to_valid (
        --still import as all text because we want to be able to analyze it for if it will make it to valid based on dtype, and we can assume it will fit in text because that's how it already exists in raw
        listing_id_18 TEXT,
        account_id TEXT,
        start_date TEXT,
        end_date TEXT,
        hosted_by TEXT,
        listing_location_address TEXT,
        listing_name TEXT,
        presented_by TEXT,
        publish_start_date TEXT,
        publish_end_date TEXT,
        is_deleted TEXT,
        record_type_id TEXT, --picklist
        sf_created_timestamp TEXT,
        sf_last_modified_timestamp TEXT,
        sf_system_modstamp TEXT,
        --dss_ingestion_timestamp can be assumed to still be timestamp because that's how it exists in raw
        dss_ingestion_timestamp TIMESTAMPTZ,
        required_fields_validated BOOLEAN,
        optional_fields_validated BOOLEAN
    );

    --this statement places all of the most recently uploaded records into the temp table
    INSERT INTO temp_sf_listing_raw_to_valid
    SELECT
        id AS listing_id_18,
        account__c AS account_id,
        start_date__c AS start_date,
        end_date__c AS end_date,
        hosted_by__c AS hosted_by,
        listing_location_address__c AS listing_location_address,
        name AS listing_name,
        presented_by__c AS presented_by,
        publish_start_date__c AS publish_start_date,
        publish_end_date__c AS publish_end_date,
        isdeleted AS is_deleted,
        recordtypeid AS record_type_id,
        createddate AS sf_created_timestamp,
        lastmodifieddate AS sf_last_modified_timestamp,
        systemmodstamp AS sf_system_modstamp,
        dss_ingestion_timestamp,
        TRUE AS required_fields_validated,
        TRUE AS optional_fields_validated
    FROM ft_ds_raw.sf_listing
    WHERE
        dss_ingestion_timestamp >
        CASE
            WHEN load_threshold_timestamp IS NULL THEN (SELECT MAX(execution_timestamp) from ft_ds_valid.raw_to_valid_execution_log WHERE entity = 'sf_listing')
            ELSE load_threshold_timestamp
        END
    ;

    --this statement updates the required_fields_validated flag
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    --it is structured this way because if the required fields do not meet data quality, then they are not passed to valid and therefore do not need to be transformed further. Therefore, only the required_fields_validated flag needs to be updated.
    UPDATE temp_sf_listing_raw_to_valid
    SET
    required_fields_validated = FALSE
    WHERE
    --contact_id_18
        listing_id_18 IS NULL
        OR LENGTH(listing_id_18) <> 18
        OR listing_id_18 = ''
    --sf_system_modstamp
        OR sf_system_modstamp IS NULL
        OR NOT (SELECT ft_ds_admin.is_coercable_to_timestamptz(sf_system_modstamp))
    ;

    --these statements update the optional_fields_validated flag and swap invalid values to NULL
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    -- account_id
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    account_id = NULL
    WHERE
        account_id IS NULL
        OR LENGTH(account_id) <> 18
        OR account_id = ''
    ;
    -- start_date
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    start_date = NULL
    WHERE
        start_date IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(start_date))
        OR start_date = ''
    ; 
    -- end_date
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    end_date = NULL
    WHERE
        end_date IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(end_date))
        OR end_date = ''
    ; 
    -- hosted_by
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    hosted_by = NULL
    WHERE
        hosted_by IS NULL
        OR LENGTH(hosted_by) <> 18
        OR hosted_by = ''
    ;
    -- listing_location_address
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    listing_location_address = NULL
    WHERE
        listing_location_address IS NULL
        OR listing_location_address = ''
    ;
    -- listing_name
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    listing_name = NULL
    WHERE
        listing_name IS NULL
        OR listing_name = ''
    ;
    -- presented_by
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    presented_by = NULL
    WHERE
        presented_by IS NULL
        OR LENGTH(presented_by) <> 18
        OR presented_by = ''
    ;
    -- publish_start_date
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    publish_start_date = NULL
    WHERE
        publish_start_date IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(publish_start_date))
        OR publish_start_date = ''
    ;
    -- publish_end_date
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    publish_end_date = NULL
    WHERE
        publish_end_date IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(publish_end_date))
        OR publish_end_date = ''
    ;
    -- is_deleted
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_deleted = NULL
    WHERE
        is_deleted IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(is_deleted))
        OR is_deleted = ''
    ;
    -- record_type_id
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    record_type_id = NULL
    WHERE
        record_type_id IS NULL
        OR record_type_id NOT IN ('01236000000nmeNAAQ', '01236000000nmeOAAQ')
        OR record_type_id = ''
    ;
    -- sf_created_timestamp
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_created_timestamp = NULL
    WHERE
        sf_created_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_created_timestamp))
        OR sf_created_timestamp = ''
    ;
    -- sf_last_modified_timestamp
    UPDATE temp_sf_listing_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_timestamp = NULL
    WHERE
        sf_last_modified_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_last_modified_timestamp))
        OR sf_last_modified_timestamp = ''
    ;
    -- dss_ingestion_timestamp
    UPDATE temp_sf_listing_raw_to_valid
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
    INSERT INTO ft_ds_raw.validation_errors_sf_listing
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
        is_deleted,
        record_type_id,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp,
        dss_ingestion_timestamp,
        required_fields_validated,
        optional_fields_validated,
        FALSE AS fixed_in_source
    FROM temp_sf_listing_raw_to_valid
    WHERE
        required_fields_validated = FALSE
        OR optional_fields_validated = FALSE
    ;

    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_listing_raw_to_valid_validated;

    --now that we've flagged the valid data and set optional invalid fields to NULL, we can cast the values to their valid types
    CREATE TABLE IF NOT EXISTS temp_sf_listing_raw_to_valid_validated (
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

    --this statement then cleans the data to get the data types correct
    INSERT INTO temp_sf_listing_raw_to_valid_validated
    SELECT
        listing_id_18,
        account_id,
        CAST(start_date AS TIMESTAMPTZ) AS start_date,
        CAST(end_date AS TIMESTAMPTZ) AS end_date,
        hosted_by,
        listing_location_address,
        listing_name,
        presented_by,
        CAST(publish_start_date AS TIMESTAMPTZ) AS publish_start_date,
        CAST(publish_end_date AS TIMESTAMPTZ) AS publish_end_date,
        CAST(is_deleted AS BOOLEAN) AS is_deleted,
        record_type_id,
        CAST(sf_created_timestamp AS TIMESTAMPTZ) AS sf_created_timestamp,
        CAST(sf_last_modified_timestamp AS TIMESTAMPTZ) AS sf_last_modified_timestamp,
        CAST(sf_system_modstamp AS TIMESTAMPTZ) AS sf_system_modstamp,
        dss_ingestion_timestamp
    FROM temp_sf_listing_raw_to_valid
    WHERE
        required_fields_validated = TRUE
    ;

    --this query gets the population correct for transitioning from raw to valid. It only includes records:
    -- that were inserted into the raw zone since the last raw->valid run
    -- that dont belong to testing chapters
    -- that are unique to each unique ID value (no dups)
    INSERT INTO ft_ds_valid.sf_listing
    SELECT
        all_values_but_dss_ingestion.*,
        dss_ingestion.dss_ingestion_timestamp
    FROM
    (SELECT
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
        is_deleted,
        record_type_id,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp
    FROM temp_sf_listing_raw_to_valid_validated
    GROUP BY
        --this group by clause exists to eliminate duplicates since multiple records with the same Id and system_modstamp can exist
        --it is every field going into the valid table except dss_ingestion_timestamp
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
        is_deleted,
        record_type_id,
        sf_created_timestamp,
        sf_last_modified_timestamp,
        sf_system_modstamp
    ) all_values_but_dss_ingestion
    JOIN
    (SELECT
        listing_id_18,
        MAX(sf_system_modstamp) as max_date
    FROM temp_sf_listing_raw_to_valid_validated
    GROUP BY
        listing_id_18
    ) max_dates
    ON all_values_but_dss_ingestion.listing_id_18 = max_dates.listing_id_18
    AND all_values_but_dss_ingestion.sf_system_modstamp = max_dates.max_date
    JOIN
    (SELECT
        listing_id_18,
        MAX(dss_ingestion_timestamp) as dss_ingestion_timestamp
    FROM temp_sf_listing_raw_to_valid_validated
    GROUP BY
        listing_id_18
    )dss_ingestion
    ON all_values_but_dss_ingestion.listing_id_18 = dss_ingestion.listing_id_18
    ON CONFLICT (listing_id_18) DO UPDATE SET
        account_id = EXCLUDED.account_id,
        start_date = EXCLUDED.start_date,
        end_date = EXCLUDED.end_date,
        hosted_by = EXCLUDED.hosted_by,
        listing_location_address = EXCLUDED.listing_location_address,
        listing_name = EXCLUDED.listing_name,
        presented_by = EXCLUDED.presented_by,
        publish_start_date = EXCLUDED.publish_start_date,
        publish_end_date = EXCLUDED.publish_end_date,
        is_deleted = EXCLUDED.is_deleted,
        record_type_id = EXCLUDED.record_type_id,
        sf_created_timestamp = EXCLUDED.sf_created_timestamp,
        sf_last_modified_timestamp = EXCLUDED.sf_last_modified_timestamp,
        sf_system_modstamp = EXCLUDED.sf_system_modstamp,
        dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp
    ;

    INSERT INTO ft_ds_valid.raw_to_valid_execution_log
    SELECT
    MAX(dss_ingestion_timestamp) AS execution_timestamp,
    'sf_listing' AS entity
    FROM ft_ds_valid.sf_listing
    ;
END;
$$;