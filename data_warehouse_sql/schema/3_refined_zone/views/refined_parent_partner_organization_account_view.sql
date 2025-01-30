CREATE OR REPLACE VIEW ft_ds_refined.parent_partner_organization_account_view
AS
SELECT
    account.account_name,
    account.parent_account,
    account.partner_program_type,
    account.location_type,
    --NOT IN VALID AS affiliate_delivery_partner,
    --record_type_ids.account_record_type_name,
    --this record type object is not imported to DSS yet
    account.account_record_type_id,
    account.chapter_affiliation_id,
    account_chapter.account_name as chapter_affiliation_name,
    account.additional_trade_name_chapter_affiliation_id,
    account_additional.account_name as additional_trade_name_chapter_affiliation_name,
    account.territory,
    account.is_active,
    account.account_id,
    --NOT IN RAW AS fmp_id,
    --MDR information does not seem to be a Salesforce object, so it will not be joined as of now. Upon import of MDR information, supplement this view with information based on the below ID
    account.mdr_pid,
    account.ys_report_chapter_affiliation,
    account.date_joined,
    --nces does not seem to be a Salesforce object, so it will not be joined as of now. Upon import of National Center for Education Statistics data, supplement this view with information based on the below ID
    account.nces_id,
    account.billing_street,
    account.billing_city,
    account.billing_state,
    account.billing_postal_code,
    account.billing_country,
    account.shipping_street ,
    account.shipping_city,
    account.shipping_state,
    account.shipping_postal_code,
    account.shipping_country
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.account account
left join ft_ds_refined.account account_chapter
	on account.chapter_affiliation_id = account_chapter.account_id
left join ft_ds_refined.account account_additional
	on account.additional_trade_name_chapter_affiliation_id = account_additional.account_id
WHERE
    account.account_record_type_id = '01236000001M1f6AAC'
;