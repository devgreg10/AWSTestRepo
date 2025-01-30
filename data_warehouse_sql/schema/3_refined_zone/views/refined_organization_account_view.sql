CREATE OR REPLACE VIEW ft_ds_refined.organization_account_view
AS
SELECT
    account.account_name,
    account.parent_account,
    account_parent_account.account_name as parent_account_name,
    account.primary_contact_id,
    contact.first_name || ' ' || contact.last_name as primary_contact_name,
    --NOT IN RAW AS county,
    --record_type_ids.account_record_type_name,
    --this record type object is not imported to DSS yet
    account.account_record_type_id,
    account.type,
    --reggie information does not seem to be a Salesforce object, so it will not be joined as of now. Upon import of reggie information, supplement this view with information based on the below ID
    account.reggie_name,
    account.reggie_id,
    account.reggie_account_id,
    --NOT IN RAW AS mrm_id,
    account.is_active,
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
left join ft_ds_refined.account account_parent_account
	on account.parent_account = account_parent_account.account_id
left join ft_ds_refined.contact contact
	on account.primary_contact_id = contact.contact_id_18
WHERE
    account.account_record_type_id = '01236000000nmeGAAQ'
;