CREATE OR REPLACE VIEW ft_ds_refined.organization_account_view
AS
SELECT
    account_name,
    parent_account,
    primary_contact_id,
    --NOT IN RAW AS county,
    account_record_type_id,
    type,
    reggie_name,
    reggie_id,
    reggie_account_id,
    --NOT IN RAW AS mrm_id,
    is_active,
    billing_street,
    billing_city,
    billing_state,
    billing_postal_code,
    billing_country,
    shipping_street ,
    shipping_city,
    shipping_state,
    shipping_postal_code,
    shipping_country
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.account
WHERE
    account_record_type_id = '01236000000nmeGAAQ'
;