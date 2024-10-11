CREATE OR REPLACE FUNCTION ft_ds_admin.write_dci_reference_raw_to_valid ()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    --need to get rid of all the data from the temp table before we start in case it still exists from the last run
    --we dont truncate at the end of this process because the temp table still briefly persists after execution, and then we can query it for debugging
    --we drop instead of truncate in case we make a field list changes and the temp table is still loaded in the database
    DROP TABLE IF EXISTS temp_dci_reference_data_raw_to_valid;

    CREATE TEMP TABLE IF NOT EXISTS temp_dci_reference_data_raw_to_valid (
        --still import as all text because we want to be able to analyze it for if it will make it to valid based on dtype, and we can assume it will fit in text because that's how it already exists in raw
        zip_code TEXT,
        chapter_name TEXT,
        chapter_id TEXT,
        dci_score TEXT,
        required_fields_validated BOOLEAN,
        optional_fields_validated BOOLEAN
    );

    INSERT INTO temp_dci_reference_data_raw_to_valid
    SELECT
        zip_code,
        chapter_name,
        chapter_id,
        dci_score,
        TRUE AS required_fields_validated,
        TRUE AS optional_fields_validated
    FROM ft_ds_raw.dci_reference_data
    ;

    --this statement updates the required_fields_validated flag
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    --it is structured this way because if the required fields do not meet data quality, then they are not passed to valid and therefore do not need to be transformed further. Therefore, only the required_fields_validated flag needs to be updated.
    UPDATE temp_dci_reference_data_raw_to_valid
    SET
    required_fields_validated = FALSE
    WHERE
    --zip_code
        zip_code IS NULL
        OR zip_code !~ '^\d{5}(-\d{4})?$'
        OR zip_code = ''
    ;

    --these statements update the optional_fields_validated flag and swap invalid values to NULL
    --they must meet the conditions for coercing into the datatype of the next table, not be empty or NULL, and match the values/format if a picklist or formatted field
    -- chapter_name
    UPDATE temp_dci_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_name = NULL
    WHERE
        chapter_name IS NULL
        --this is a special case, some of the values are 'BLANK' and I'd rather them be NULL
        OR chapter_name = 'BLANK'
        OR chapter_name = ''
    ;
    --chapter_id
    UPDATE temp_dci_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    chapter_id = NULL
    WHERE
        chapter_id IS NULL
        --this is a special case, some of the values are 'BLANK' and I'd rather them be NULL
        OR chapter_id = 'BLANK'
        OR LENGTH(chapter_id) <> 18
        OR chapter_id = ''
    ;
    --dci_score
    UPDATE temp_dci_reference_data_raw_to_valid
    SET
    optional_fields_validated = FALSE,
    dci_score = NULL
    WHERE
        dci_score IS NULL
        OR NOT (SELECT ft_ds_admin.is_coercable_to_numeric(dci_score))
        OR dci_score = ''
    ;

    --copy all records where required_fields_validated = FALSE OR optional_fields_validated = FALSE to a permanent errored table
    --this can be reported on later to let data source owners fix the data upstream, where it will be re-ingested and fixed throughout all zones of the data warehouse
    --the fixed_in_source field can be updated when a data owner fixes the record in the source.
    INSERT INTO ft_ds_raw.validation_errors_dci_reference_data
    SELECT
        zip_code,
        chapter_name,
        chapter_id,
        dci_score,
        required_fields_validated,
        optional_fields_validated,
        FALSE AS fixed_in_source
    FROM temp_dci_reference_data_raw_to_valid
    WHERE
        required_fields_validated = FALSE
        OR optional_fields_validated = FALSE
    ;

    --truncating the table in valid since any edits to this should be a re-upload of a valid csv file, which would overwrite the data
    --if we ever swap the dci_reference_data table to also include versions, then this step will need to be removed
    TRUNCATE ft_ds_valid.dci_reference_data;

    --this statement then cleans the data to get the data types correct
    INSERT INTO ft_ds_valid.dci_reference_data
    SELECT
        zip_code,
        chapter_name,
        chapter_id,
        CAST(dci_score AS NUMERIC) AS dci_score
    FROM temp_dci_reference_data_raw_to_valid
    WHERE
        required_fields_validated = TRUE
    ;
END;
$$;