CREATE SCHEMA IF NOT EXISTS ft_ds_raw;
CREATE SCHEMA IF NOT EXISTS ft_ds_valid;
CREATE SCHEMA IF NOT EXISTS ft_ds_refined;
CREATE SCHEMA IF NOT EXISTS ft_ds_admin;

CREATE OR REPLACE FUNCTION ft_ds_admin.is_coercable_to_timestamptz(candidate_string TEXT)
RETURNS BOOLEAN AS $$
DECLARE timestamp_var TIMESTAMPTZ DEFAULT NULL;
BEGIN
    BEGIN
        timestamp_var := candidate_string::TIMESTAMPTZ;
    EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
    END;
RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ft_ds_admin.is_coercable_to_numeric(candidate_string TEXT)
RETURNS BOOLEAN AS $$
DECLARE numeric_var NUMERIC DEFAULT NULL;
BEGIN
    BEGIN
        numeric_var := candidate_string::NUMERIC;
    EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
    END;
RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ft_ds_admin.is_coercable_to_boolean(candidate_string TEXT)
RETURNS BOOLEAN AS $$
DECLARE numeric_var BOOLEAN DEFAULT NULL;
BEGIN
    BEGIN
        numeric_var := candidate_string::BOOLEAN;
    EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
    END;
RETURN TRUE;
END;
$$ LANGUAGE plpgsql;