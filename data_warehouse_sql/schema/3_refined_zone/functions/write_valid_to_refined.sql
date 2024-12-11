CREATE OR REPLACE FUNCTION ft_ds_admin.write_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM ft_ds_admin.write_contact_valid_to_refined();
    PERFORM ft_ds_admin.write_listing_session_valid_to_refined();
    PERFORM ft_ds_admin.write_session_registration_valid_to_refined();
    PERFORM ft_ds_admin.write_listing_valid_to_refined();
    PERFORM ft_ds_admin.write_badge_valid_to_refined();
END;
$$;