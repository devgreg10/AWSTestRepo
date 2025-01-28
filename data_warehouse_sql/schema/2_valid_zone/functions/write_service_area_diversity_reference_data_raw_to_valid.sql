CREATE OR REPLACE FUNCTION ft_ds_admin.write_service_area_diversity_reference_data_raw_to_valid ()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    --we drop instead of truncate in case we make a field list changes and the temp table is still loaded in the database
    DROP TABLE IF EXISTS temp_service_area_diversity_reference_data_raw_to_valid;

    CREATE TEMP TABLE IF NOT EXISTS temp_service_area_diversity_reference_data_raw_to_valid AS
    SELECT
        chapter_info.account_id AS chapter_id,
        'First Tee ' || "Chapter" AS chapter_name,
        regexp_replace("Asian", ',', '', 'g') AS asian_count,
        regexp_replace("Black or African-American", ',', '', 'g') AS black_or_african_american_count,
        regexp_replace("Latino or Hispanic", ',', '', 'g') AS latino_or_hispanic_count,
        regexp_replace("Multi-Racial", ',', '', 'g') AS multi_racial_count,
        regexp_replace("Native American or Native Alaskan", ',', '', 'g') AS native_american_or_native_alaskan_count,
        regexp_replace("Pacific Islander", ',', '', 'g') AS pacific_islander_count,
        regexp_replace("White or Caucasian", ',', '', 'g') AS white_or_caucasian_count,
        regexp_replace("Youth Population", ',', '', 'g') AS youth_population_count,
        regexp_replace("Market Diversity", '%', '', 'g') AS market_diversity_percentage,
        "Upload date" AS upload_date,
        TRUE AS required_fields_validated,
        TRUE AS optional_fields_validated
    FROM ft_ds_raw.service_area_diversity_reference_data ref_data
    LEFT JOIN ft_ds_refined.chapter_account_view chapter_info
        ON chapter_info.account_name = 'First Tee ' || ref_data."Chapter"
    WHERE chapter_info.account_id IS NOT NULL
    ;

    --this statement updates the required_fields_validated flag
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    --it is structured this way because if the required fields do not meet data quality, then they are not passed to valid and therefore do not need to be transformed further. Therefore, only the required_fields_validated flag needs to be updated.
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    required_fields_validated = FALSE
    WHERE
    --chapter_name
        chapter_name IS NULL
        OR chapter_name = ''
    --upload_date
        OR upload_date IS NULL
        OR upload_date = ''
        OR NOT ft_ds_admin.is_coercable_to_timestamptz(upload_date)
    ;

    --these statements update the optional_fields_validated flag and swap invalid values to NULL
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    -- chapter_id
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_id = NULL
    WHERE
        chapter_id IS NULL
        OR LENGTH(chapter_id) <> 18
        OR chapter_name = ''
    ;
    --asian_count
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    asian_count = NULL
    WHERE
        asian_count IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(asian_count)
        OR asian_count = ''
    ;

    --black_or_african_american_count
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    black_or_african_american_count = NULL
    WHERE
        black_or_african_american_count IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(black_or_african_american_count)
        OR black_or_african_american_count = ''
    ;

    --latino_or_hispanic_count
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    latino_or_hispanic_count = NULL
    WHERE
        latino_or_hispanic_count IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(latino_or_hispanic_count)
        OR latino_or_hispanic_count = ''
    ;

    --multi_racial_count
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    multi_racial_count = NULL
    WHERE
        multi_racial_count IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(multi_racial_count)
        OR multi_racial_count = ''
    ;

    --native_american_or_native_alaskan_count
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    native_american_or_native_alaskan_count = NULL
    WHERE
        native_american_or_native_alaskan_count IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(native_american_or_native_alaskan_count)
        OR native_american_or_native_alaskan_count = ''
    ;

    --pacific_islander_count
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    pacific_islander_count = NULL
    WHERE
        pacific_islander_count IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(pacific_islander_count)
        OR pacific_islander_count = ''
    ;

    --white_or_caucasian_count
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    white_or_caucasian_count = NULL
    WHERE
        white_or_caucasian_count IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(white_or_caucasian_count)
        OR white_or_caucasian_count = ''
    ;
    
    --youth_population_count
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    youth_population_count = NULL
    WHERE
        youth_population_count IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(youth_population_count)
        OR youth_population_count = ''
    ;

    --market_diversity_percentage
    UPDATE temp_service_area_diversity_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    market_diversity_percentage = NULL
    WHERE
        market_diversity_percentage IS NULL
        OR NOT ft_ds_admin.is_coercable_to_numeric(market_diversity_percentage)
        OR market_diversity_percentage = ''
    ;

    --truncating the table in valid since we'd also be including previous versions of the raw data in this load, and we dont want to repeat it in valid
     TRUNCATE ft_ds_valid.service_area_diversity_reference_data;

    --copy all records where required_fields_validated = FALSE OR optional_fields_validated = FALSE to a permanent errored table
    --this can be reported on later to let data source owners fix the data upstream, where it will be re-ingested and fixed throughout all zones of the data warehouse
    --the fixed_in_source field can be updated when a data owner fixes the record in the source.
    INSERT INTO ft_ds_raw.validation_errors_service_area_diversity_reference_data
    SELECT
        chapter_id,
        chapter_name,
        asian_count,
        black_or_african_american_count,
        latino_or_hispanic_count,
        multi_racial_count,
        native_american_or_native_alaskan_count,
        pacific_islander_count,
        white_or_caucasian_count,
        youth_population_count,
        market_diversity_percentage,
        upload_date,
        required_fields_validated,
        optional_fields_validated,
        FALSE AS fixed_in_source
    FROM temp_service_area_diversity_reference_data_raw_to_valid
    WHERE
        required_fields_validated = FALSE
        OR optional_fields_validated = FALSE
        
    ;

    --this statement then cleans the data to get the data types correct
    INSERT INTO ft_ds_valid.service_area_diversity_reference_data
    SELECT
        chapter_id,
        chapter_name,
        CAST(asian_count AS INTEGER) AS asian_count,
        CAST(black_or_african_american_count AS INTEGER) AS black_or_african_american_count,
        CAST(latino_or_hispanic_count AS INTEGER) AS latino_or_hispanic_count,
        CAST(multi_racial_count AS INTEGER) AS multi_racial_count,
        CAST(native_american_or_native_alaskan_count AS INTEGER) AS native_american_or_native_alaskan_count,
        CAST(pacific_islander_count AS INTEGER) AS pacific_islander_count,
        CAST(white_or_caucasian_count AS INTEGER) AS white_or_caucasian_count,
        CAST(youth_population_count AS INTEGER) AS youth_population_count,
        (CAST(market_diversity_percentage AS NUMERIC) / 100.0) AS market_diversity_percentage,
        CAST(upload_date AS TIMESTAMPTZ) as upload_date
    FROM temp_service_area_diversity_reference_data_raw_to_valid
    WHERE
        required_fields_validated = TRUE
    ;
END;
$$;