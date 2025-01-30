CREATE OR REPLACE VIEW ft_ds_refined.household_account_view
AS
SELECT
    account.account_name,
    account.chapter_affiliation_id,
    account_chapter.account_name as chapter_name,
    --NOT IN RAW AS number_of_active_participants,
    --record_type_ids.account_record_type_name,
    --this record type object is not imported to DSS yet
    account.account_record_type_id,
    account.primary_contact_id,
    contact.first_name || ' ' || contact.last_name as primary_contact_name,
    --NOT IN VALID AS secondary_contact,
    account.description,
    account.account_id,
    account.parent_account,
    account_parent_account.account_name as parent_account_name,
    --NOT IN RAW AS fmp_id,
    --reggie information does not seem to be a Salesforce object, so it will not be joined as of now. Upon import of reggie information, supplement this view with information based on the below ID
    account.reggie_id,
    account.reggie_account_id
    --NOT IN RAW AS fmp_chapter_code,
    --NOT IN RAW AS legacy_account_holder_id
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.account account
left join ft_ds_refined.account account_chapter
	on account.chapter_affiliation_id = account_chapter.account_id
left join ft_ds_refined.account account_parent_account
	on account.parent_account = account_parent_account.account_id
left join ft_ds_refined.contact contact
	on account.primary_contact_id = contact.contact_id_18
WHERE
    account.account_record_type_id = '01236000000nmeFAAQ'
;