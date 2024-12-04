CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_badge_raw_to_valid (load_threshold_timestamp TIMESTAMPTZ DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_badge_raw_to_valid;

    CREATE TEMP TABLE IF NOT EXISTS temp_sf_badge_raw_to_valid AS
    SELECT
        id AS badge_id,
        ownerid AS owner_id,
        isdeleted AS is_deleted,
        name AS badge_name,
        createddate AS sf_created_timestamp,
        createdbyid AS sf_created_by_id,
        lastmodifieddate AS sf_last_modified_timestamp,
        lastmodifiedbyid AS sf_last_modified_by_id,
        systemmodstamp AS sf_system_modstamp,
        description__c AS description,
        category__c AS category,
        badge_type__c AS badge_type,
        is_active__c AS is_active,
        points__c AS points,
        sort_order__c AS sort_order,
        badge_id__c AS external_badge_id,
        age_group__c AS age_group,
        dss_ingestion_timestamp,
        TRUE AS required_fields_validated,
        TRUE AS optional_fields_validated
    FROM ft_ds_raw.sf_badge
    WHERE
        dss_ingestion_timestamp >
        CASE
            WHEN load_threshold_timestamp IS NULL THEN (SELECT MAX(execution_timestamp) from ft_ds_valid.raw_to_valid_execution_log WHERE entity = 'sf_badge')
            ELSE load_threshold_timestamp
        END
    ;

    --this statement updates the required_fields_validated flag
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    --it is structured this way because if the required fields do not meet data quality, then they are not passed to valid and therefore do not need to be transformed further. Therefore, only the required_fields_validated flag needs to be updated.
    UPDATE temp_sf_badge_raw_to_valid
    SET
    required_fields_validated = FALSE
    WHERE
    -- badge_id
        badge_id IS NULL
        OR LENGTH(badge_id) <> 18
        OR badge_id = ''
    --sf_system_modstamp
        OR sf_system_modstamp IS NULL
        OR NOT (SELECT ft_ds_admin.is_coercable_to_timestamptz(sf_system_modstamp))
    ;

    --these statements update the optional_fields_validated flag and swap invalid values to NULL
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    -- owner_id
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    owner_id = NULL
    WHERE
        owner_id IS NULL
        OR LENGTH(owner_id) <> 18
        OR owner_id = ''
    ;
    -- is_deleted
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_deleted = NULL
    WHERE
        is_deleted IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(is_deleted))
        OR is_deleted = ''
    ;
    -- badge_name
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    badge_name = NULL
    WHERE
        badge_name IS NULL
        OR LENGTH(badge_name) > 80
        OR badge_name = ''
    ;
    -- sf_created_timestamp
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_created_timestamp = NULL
    WHERE
        sf_created_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_created_timestamp))
        OR sf_created_timestamp = ''
    ;
    -- sf_created_by_id
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_created_by_id = NULL
    WHERE
        sf_created_by_id IS NULL
        OR LENGTH(sf_created_by_id) <> 18
        OR sf_created_by_id = ''
    ;
    -- sf_last_modified_timestamp
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_timestamp = NULL
    WHERE
        sf_last_modified_timestamp IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_timestamptz(sf_last_modified_timestamp))
        OR sf_last_modified_timestamp = ''
    ;
    -- sf_last_modified_by_id
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sf_last_modified_by_id = NULL
    WHERE
        sf_last_modified_by_id IS NULL
        OR LENGTH(sf_last_modified_by_id) <> 18
        OR sf_last_modified_by_id = ''
    ;
    -- description
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    description = NULL
    WHERE
        description IS NULL
        OR description = ''
    ;
    -- category
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    category = NULL
    WHERE
        category IS NULL
        OR category NOT IN ('Golf Skills','Key Commitments','Life Skills')
        OR category = ''
    ;
    -- badge_type
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    badge_type = NULL
    WHERE
        badge_type IS NULL
        OR badge_type NOT IN ('Sticker','Attendance','Digital Learning')
        OR badge_type = ''
    ;
    -- is_active
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    is_active = NULL
    WHERE
        is_active IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_boolean(is_active))
        OR is_active = ''
    ;
    -- points
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    points = NULL
    WHERE
        points IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(points))
        OR points = ''
    ;
    -- sort_order
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    sort_order = NULL
    WHERE
        sort_order IS NULL
        OR NOT (ft_ds_admin.is_coercable_to_numeric(sort_order))
        OR sort_order = ''
    ;
    -- external_badge_id
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    external_badge_id = NULL
    WHERE
        external_badge_id IS NULL
        OR LENGTH(external_badge_id) > 20
        OR external_badge_id = ''
    ;
    -- age_group
    UPDATE temp_sf_badge_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    age_group = NULL
    WHERE
        age_group IS NULL
        OR age_group = ''
    ;
    -- dss_ingestion_timestamp
    UPDATE temp_sf_badge_raw_to_valid
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
    INSERT INTO ft_ds_raw.validation_errors_sf_badge
    SELECT
        badge_id,
        owner_id,
        is_deleted,
        badge_name,
        sf_created_timestamp,
        sf_created_by_id,
        sf_last_modified_timestamp,
        sf_last_modified_by_id,
        sf_system_modstamp,
        description,
        category,
        badge_type,
        is_active,
        points,
        sort_order,
        external_badge_id,
        age_group,
        dss_ingestion_timestamp,
        required_fields_validated,
        optional_fields_validated,
        FALSE AS fixed_in_source
    FROM temp_sf_badge_raw_to_valid
    WHERE
        required_fields_validated = FALSE
        OR optional_fields_validated = FALSE
    ;

    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    DROP TABLE IF EXISTS temp_sf_badge_raw_to_valid_validated;

    --now that we've flagged the valid data and set optional invalid fields to NULL, we can cast the values to their valid types
    CREATE TABLE IF NOT EXISTS temp_sf_badge_raw_to_valid_validated (
        badge_id CHAR(18),
        owner_id CHAR(18),
        is_deleted BOOLEAN,
        badge_name VARCHAR(80),
        sf_created_timestamp TIMESTAMPTZ,
        sf_created_by_id CHAR(18),
        sf_last_modified_timestamp TIMESTAMPTZ,
        sf_last_modified_by_id CHAR(18),
        sf_system_modstamp TIMESTAMPTZ,
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

    --this statement then cleans the data to get the data types correct
    INSERT INTO temp_sf_badge_raw_to_valid_validated
    SELECT
        badge_id,
        owner_id,
        CAST(is_deleted AS BOOLEAN) AS is_deleted,
        badge_name,
        CAST(sf_created_timestamp AS TIMESTAMPTZ) AS sf_created_timestamp,
        sf_created_by_id,
        CAST(sf_last_modified_timestamp AS TIMESTAMPTZ) AS sf_last_modified_timestamp,
        sf_last_modified_by_id,
        CAST(sf_system_modstamp AS TIMESTAMPTZ) AS sf_system_modstamp,
        description,
        category,
        badge_type,
        CAST(is_active AS BOOLEAN) AS is_active,
        CAST(points AS NUMERIC) AS points,
        CAST(sort_order AS NUMERIC) AS sort_order,
        external_badge_id,
        age_group,
        dss_ingestion_timestamp
    FROM temp_sf_badge_raw_to_valid
    WHERE
        required_fields_validated = TRUE
    ;

    --this query gets the population correct for transitioning from raw to valid. It only includes records:
    -- that were inserted into the raw zone since the last raw->valid run
    -- that dont belong to testing chapters
    -- that are unique to each unique ID value (no dups)
    INSERT INTO ft_ds_valid.sf_badge
    SELECT
        all_values_but_dss_ingestion.*,
        dss_ingestion.dss_ingestion_timestamp
    FROM
    (SELECT
        badge_id,
        owner_id,
        is_deleted,
        badge_name,
        sf_created_timestamp,
        sf_created_by_id,
        sf_last_modified_timestamp,
        sf_last_modified_by_id,
        sf_system_modstamp,
        description,
        category,
        badge_type,
        is_active,
        points,
        sort_order,
        external_badge_id,
        age_group
    FROM temp_sf_badge_raw_to_valid_validated
    GROUP BY
        --this group by clause exists to eliminate duplicates since multiple records with the same Id and system_modstamp can exist
        --it is every field going into the valid table except dss_ingestion_timestamp
        badge_id,
        owner_id,
        is_deleted,
        badge_name,
        sf_created_timestamp,
        sf_created_by_id,
        sf_last_modified_timestamp,
        sf_last_modified_by_id,
        sf_system_modstamp,
        description,
        category,
        badge_type,
        is_active,
        points,
        sort_order,
        external_badge_id,
        age_group
    ) all_values_but_dss_ingestion
    JOIN
    (SELECT
        badge_id,
        MAX(sf_system_modstamp) as max_date
    FROM temp_sf_badge_raw_to_valid_validated
    GROUP BY
        badge_id
    ) max_dates
    ON all_values_but_dss_ingestion.badge_id = max_dates.badge_id
    AND all_values_but_dss_ingestion.sf_system_modstamp = max_dates.max_date
    JOIN
    (SELECT
        badge_id,
        MAX(dss_ingestion_timestamp) as dss_ingestion_timestamp
    FROM temp_sf_badge_raw_to_valid_validated
    GROUP BY
        badge_id
    )dss_ingestion
    ON all_values_but_dss_ingestion.badge_id = dss_ingestion.badge_id
    ON CONFLICT (badge_id) DO UPDATE SET
        owner_id = EXCLUDED.owner_id,
        is_deleted = EXCLUDED.is_deleted,
        badge_name = EXCLUDED.badge_name,
        sf_created_timestamp = EXCLUDED.sf_created_timestamp,
        sf_created_by_id = EXCLUDED.sf_created_by_id,
        sf_last_modified_timestamp = EXCLUDED.sf_last_modified_timestamp,
        sf_last_modified_by_id = EXCLUDED.sf_last_modified_by_id,
        sf_system_modstamp = EXCLUDED.sf_system_modstamp,
        description = EXCLUDED.description,
        category = EXCLUDED.category,
        badge_type = EXCLUDED.badge_type,
        is_active = EXCLUDED.is_active,
        points = EXCLUDED.points,
        sort_order = EXCLUDED.sort_order,
        external_badge_id = EXCLUDED.external_badge_id,
        age_group = EXCLUDED.age_group,
        dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp
    ;

    INSERT INTO ft_ds_valid.raw_to_valid_execution_log
    SELECT
    MAX(dss_ingestion_timestamp) AS execution_timestamp,
    'sf_badge' AS entity
    FROM ft_ds_valid.sf_badge
    ;
END;
$$;