CREATE TRIGGER valid_sf_contact_set_last_modified_timestamp
BEFORE UPDATE ON ft_ds_valid.sf_contact
FOR EACH ROW
EXECUTE FUNCTION ft_ds_admin.update_last_modified_timestamp() --this function is defined in database_setup.sql
;