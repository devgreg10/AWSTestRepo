CREATE TRIGGER refined_sf_contact_set_last_modified_timestamp
BEFORE UPDATE ON ft_ds_refined.sf_contact
FOR EACH ROW
EXECUTE FUNCTION update_last_modified_timestamp() --this function is defined in database_setup.sql
;