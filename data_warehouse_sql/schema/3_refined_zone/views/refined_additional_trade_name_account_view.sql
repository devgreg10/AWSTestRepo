CREATE OR REPLACE VIEW ft_ds_refined.additional_trade_name_account_view
AS
SELECT
    additional_trade_name_account.account_name,
    parent_account_ids.account_name AS parent_account_name,
    additional_trade_name_account.parent_account,
    additional_trade_name_chapter_affiliation_ids.account_name AS additional_trade_name_chapter_affiliation_name,
    additional_trade_name_account.additional_trade_name_chapter_affiliation_id,
    additional_trade_name_account.account_record_type_id,
    additional_trade_name_account.territory,
    additional_trade_name_account.region,
    additional_trade_name_account.is_active,
    additional_trade_name_account.account_id,
    additional_trade_name_account.billing_street,
    additional_trade_name_account.billing_city,
    additional_trade_name_account.billing_state,
    additional_trade_name_account.billing_postal_code,
    additional_trade_name_account.billing_country,
    additional_trade_name_account.shipping_street ,
    additional_trade_name_account.shipping_city,
    additional_trade_name_account.shipping_state,
    additional_trade_name_account.shipping_postal_code,
    additional_trade_name_account.shipping_country
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.account additional_trade_name_account
LEFT JOIN ft_ds_refined.account parent_account_ids-- parent_account_ids come from accounts of all types, so cannot use one of the more restrictive views
    ON additional_trade_name_account.parent_account_id = parent_account_ids.account_id
LEFT JOIN ft_ds_refined.account additional_trade_name_chapter_affiliation_ids
    ON additional_trade_name_account.additional_trade_name_chapter_affiliation_id = additional_trade_name_chapter_affiliation_ids.account_id
WHERE
    additional_trade_name_account.account_record_type_id = '01236000001HyD0AAK'
;