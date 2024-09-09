CREATE SCHEMA IF NOT EXISTS ft_ds_raw;
CREATE SCHEMA IF NOT EXISTS ft_ds_valid;
CREATE SCHEMA IF NOT EXISTS ft_ds_refined;
CREATE SCHEMA IF NOT EXISTS ft_ds_admin;

CREATE OR REPLACE FUNCTION ft_ds_admin.update_last_modified_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.dss_last_modified_timestamp = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;