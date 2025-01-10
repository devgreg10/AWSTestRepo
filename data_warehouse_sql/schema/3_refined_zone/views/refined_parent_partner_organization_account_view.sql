CREATE OR REPLACE VIEW ft_ds_refined.parent_partner_organization_account_view
AS
SELECT
    account_name,
    parent_account,
    partner_program_type,
    location_type,
    --NOT IN VALID AS affiliate_delivery_partner,
    account_record_type_id,
    chapter_affiliation_id,
    additional_trade_name_chapter_affiliation_id,
    territory,
    is_active,
    account_id,
    --NOT IN RAW AS fmp_id,
    mdr_pid,
    ys_report_chapter_affiliation,
    date_joined,
    nces_id,
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
    account_record_type_id = '01236000001M1f6AAC'
;