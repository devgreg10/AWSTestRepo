CREATE OR REPLACE VIEW ft_ds_refined.additional_trade_name_account_view
AS
SELECT
    account_name,
    parent_account,
    additional_trade_name_chapter_affiliation_id,
    account_record_type_id,
    territory,
    region,
    is_active,
    account_id,
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
    account_record_type_id = '01236000001HyD0AAK'
;