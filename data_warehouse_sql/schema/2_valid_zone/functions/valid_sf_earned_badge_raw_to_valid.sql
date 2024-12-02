CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_earned_badge_raw_to_valid (load_threshold_timestamp TIMESTAMPTZ DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_earned_badge_raw_to_valid;

    CREATE TEMP TABLE IF NOT EXISTS temp_sf_earned_badge_raw_to_valid AS
    SELECT
        id AS earned_badge_id,
        isdeleted AS is_deleted,
        createddate AS sf_created_timestamp,
        createdbyid AS sf_created_by_id,
        lastmodifieddate AS sf_last_modified_timestamp,
        lastmodifiedbyid AS sf_last_modified_by_id,
        systemmodstamp AS sf_system_modstamp,
        badge__c AS badge_id,
        badge_type__c AS badge_type,
        category__c AS category,
        class__c AS class_id,
        contact__c AS contact_id,
        date_earned__c AS date_earned,
        listing_session__c AS listing_session_id,
        pending_aws_callout__c AS is_pending_aws_callout,
        points__c AS points,
        source_system__c AS source_system,
        dss_ingestion_timestamp,
        TRUE AS required_fields_validated,
        TRUE AS optional_fields_validated
    FROM ft_ds_raw.sf_earned_badge
    WHERE
        dss_ingestion_timestamp >
        CASE
            WHEN load_threshold_timestamp IS NULL THEN (SELECT MAX(execution_timestamp) from ft_ds_valid.raw_to_valid_execution_log WHERE entity = 'sf_earned_badge')
            ELSE load_threshold_timestamp
        END
    ;

    --this statement updates the required_fields_validated flag
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    --it is structured this way because if the required fields do not meet data quality, then they are not passed to valid and therefore do not need to be transformed further. Therefore, only the required_fields_validated flag needs to be updated.
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    required_fields_validated = FALSE
    WHERE
    --earned_badge_id
        earned_badge_id IS NULL
        OR LENGTH(earned_badge_id) <> 18
        OR earned_badge_id = ''
    --sf_system_modstamp
        OR sf_system_modstamp IS NULL
        OR NOT (SELECT ft_ds_admin.is_coercable_to_timestamptz(sf_system_modstamp))
    --badge_id
        OR badge_id IS NULL
        OR LENGTH(badge_id) <> 18
        OR badge_id = ''
    --contact_id
        OR contact_id IS NULL
        OR LENGTH(contact_id) <> 18
        OR contact_id = ''
    ;

    --these statements update the optional_fields_validated flag and swap invalid values to NULL
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    -- is_deleted
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_deleted = NULL
    WHERE
        is_deleted IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(is_deleted))
        OR is_deleted = ''
    ;
    -- sf_created_timestamp
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_created_timestamp = NULL
    WHERE
        sf_created_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_created_timestamp))
        OR sf_created_timestamp = ''
    ;
    -- sf_created_by_id
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_created_by_id = NULL
    WHERE
        sf_created_by_id IS NULL
        OR LENGTH(sf_created_by_id) <> 18
        OR sf_created_by_id = ''
    ;
    -- sf_last_modified_timestamp
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_timestamp = NULL
    WHERE
        sf_last_modified_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_last_modified_timestamp))
        OR sf_last_modified_timestamp = ''
    ;
    -- sf_last_modified_by_id
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_by_id = NULL
    WHERE
        sf_last_modified_by_id IS NULL
        OR LENGTH(sf_last_modified_by_id) <> 18
        OR sf_last_modified_by_id = ''
    ;
    -- badge_type
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    badge_type = NULL
    WHERE
        badge_type IS NULL
        OR badge_type NOT IN ('Sticker','Attendance','Digital Learning')
        OR badge_type = ''
    ;
    -- category
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    category = NULL
    WHERE
        category IS NULL
        OR category NOT IN ('Golf Skills','Key Commitments','Life Skills')
        OR category = ''
    ;
    -- class_id
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    class_id = NULL
    WHERE
        class_id IS NULL
        OR LENGTH(class_id) <> 18
        OR class_id = ''
    ;
    -- contact_id
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    contact_id = NULL
    WHERE
        contact_id IS NULL
        OR LENGTH(contact_id) <> 18
        OR contact_id = ''
    ;
    -- date_earned
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    date_earned = NULL
    WHERE
        date_earned IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(date_earned))
        OR date_earned = ''
    ;
    -- listing_session_id
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    listing_session_id = NULL
    WHERE
        listing_session_id IS NULL
        OR LENGTH(listing_session_id) <> 18
        OR listing_session_id = ''
    ;
    -- is_pending_aws_callout
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_pending_aws_callout = NULL
    WHERE
        is_pending_aws_callout IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(is_pending_aws_callout))
        OR is_pending_aws_callout = ''
    ;
    -- points
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    points = NULL
    WHERE
        points IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(points))
        OR points = ''
    ;
    -- source_system
    UPDATE temp_sf_earned_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    source_system = NULL
    WHERE
        source_system IS NULL
        OR source_system NOT IN ('Salesforce','Docebo')
        OR source_system = ''
    ;
    -- dss_ingestion_timestamp
    UPDATE temp_sf_earned_badge_raw_to_valid
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
    INSERT INTO ft_ds_raw.validation_errors_sf_earned_badge
    SELECT
        earned_badge_id,
        is_deleted,
        sf_created_timestamp,
        sf_created_by_id,
        sf_last_modified_timestamp,
        sf_last_modified_by_id,
        sf_system_modstamp,
        badge_id,
        badge_type,
        category,
        class_id,
        contact_id,
        date_earned,
        listing_session_id,
        is_pending_aws_callout,
        points,
        source_system,
        dss_ingestion_timestamp,
        required_fields_validated,
        optional_fields_validated,
        FALSE AS fixed_in_source
    FROM temp_sf_earned_badge_raw_to_valid
    WHERE
        required_fields_validated = FALSE
        OR optional_fields_validated = FALSE
    ;

    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_earned_badge_raw_to_valid_validated;

    --now that we've flagged the valid data and set optional invalid fields to NULL, we can cast the values to their valid types
    CREATE TABLE IF NOT EXISTS temp_sf_earned_badge_raw_to_valid_validated (
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

    --this statement then cleans the data to get the data types correct
    INSERT INTO temp_sf_earned_badge_raw_to_valid_validated
    SELECT
        earned_badge_id,
        CAST(is_deleted AS BOOLEAN) AS is_deleted,
        CAST(sf_created_timestamp AS TIMESTAMPTZ) AS sf_created_timestamp,
        sf_created_by_id,
        CAST(sf_last_modified_timestamp AS TIMESTAMPTZ) AS sf_last_modified_timestamp,
        sf_last_modified_by_id,
        CAST(sf_system_modstamp AS TIMESTAMPTZ) AS sf_system_modstamp,
        badge_id,
        badge_type,
        category,
        class_id,
        contact_id,
        CAST(date_earned AS TIMESTAMPTZ) AS date_earned,
        listing_session_id,
        CAST(is_pending_aws_callout AS BOOLEAN) AS is_pending_aws_callout,
        CAST(points AS NUMERIC) AS points,
        source_system,
        dss_ingestion_timestamp
    FROM temp_sf_earned_badge_raw_to_valid
    WHERE
        required_fields_validated = TRUE
    ;

    --this query gets the population correct for transitioning from raw to valid. It only includes records:
    -- that were inserted into the raw zone since the last raw->valid run
    -- that dont belong to testing chapters
    -- that are unique to each unique ID value (no dups)
    INSERT INTO ft_ds_valid.sf_earned_badge
    SELECT
        all_values_but_dss_ingestion.*,
        dss_ingestion.dss_ingestion_timestamp
    FROM
    (SELECT
        earned_badge_id,
        is_deleted,
        sf_created_timestamp,
        sf_created_by_id,
        sf_last_modified_timestamp,
        sf_last_modified_by_id,
        sf_system_modstamp,
        badge_id,
        badge_type,
        category,
        class_id,
        contact_id,
        date_earned,
        listing_session_id,
        is_pending_aws_callout,
        points,
        source_system
    FROM temp_sf_earned_badge_raw_to_valid_validated
    GROUP BY
        --this group by clause exists to eliminate duplicates since multiple records with the same Id and system_modstamp can exist
        --it is every field going into the valid table except dss_ingestion_timestamp
        earned_badge_id,
        is_deleted,
        sf_created_timestamp,
        sf_created_by_id,
        sf_last_modified_timestamp,
        sf_last_modified_by_id,
        sf_system_modstamp,
        badge_id,
        badge_type,
        category,
        class_id,
        contact_id,
        date_earned,
        listing_session_id,
        is_pending_aws_callout,
        points,
        source_system
    ) all_values_but_dss_ingestion
    JOIN
    (SELECT
        earned_badge_id,
        MAX(sf_system_modstamp) as max_date
    FROM temp_sf_earned_badge_raw_to_valid_validated
    GROUP BY
        earned_badge_id
    ) max_dates
    ON all_values_but_dss_ingestion.earned_badge_id = max_dates.earned_badge_id
    AND all_values_but_dss_ingestion.sf_system_modstamp = max_dates.max_date
    JOIN
    (SELECT
        earned_badge_id,
        MAX(dss_ingestion_timestamp) as dss_ingestion_timestamp
    FROM temp_sf_earned_badge_raw_to_valid_validated
    GROUP BY
        earned_badge_id
    )dss_ingestion
    ON all_values_but_dss_ingestion.earned_badge_id = dss_ingestion.earned_badge_id
    ON CONFLICT (earned_badge_id) DO UPDATE SET
        is_deleted = EXCLUDED.is_deleted,
        sf_created_timestamp = EXCLUDED.sf_created_timestamp,
        sf_created_by_id = EXCLUDED.sf_created_by_id,
        sf_last_modified_timestamp = EXCLUDED.sf_last_modified_timestamp,
        sf_last_modified_by_id = EXCLUDED.sf_last_modified_by_id,
        sf_system_modstamp = EXCLUDED.sf_system_modstamp,
        badge_id = EXCLUDED.badge_id,
        badge_type = EXCLUDED.badge_type,
        category = EXCLUDED.category,
        class_id = EXCLUDED.class_id,
        contact_id = EXCLUDED.contact_id,
        date_earned = EXCLUDED.date_earned,
        listing_session_id = EXCLUDED.listing_session_id,
        is_pending_aws_callout = EXCLUDED.is_pending_aws_callout,
        points = EXCLUDED.points,
        source_system = EXCLUDED.source_system,
        dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp
    ;

    INSERT INTO ft_ds_valid.raw_to_valid_execution_log
    SELECT
    MAX(dss_ingestion_timestamp) AS execution_timestamp,
    'sf_earned_badge' AS entity
    FROM ft_ds_valid.sf_earned_badge
    ;
END;
$$;