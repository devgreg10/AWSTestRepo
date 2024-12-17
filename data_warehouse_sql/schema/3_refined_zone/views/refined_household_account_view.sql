CREATE OR REPLACE VIEW ft_ds_refined.household_account_view
AS
SELECT
    account_name,
    chapter_affiliation_id,
    --NOT IN RAW AS number_of_active_participants,
    account_record_type_id,
    primary_contact_id,
    --NOT IN VALID AS secondary_contact,
    description,
    account_id,
    parent_account,
    --NOT IN RAW AS fmp_id,
    reggie_id,
    reggie_account_id
    --NOT IN RAW AS fmp_chapter_code,
    --NOT IN RAW AS legacy_account_holder_id
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.account
WHERE
    account_record_type_id = '01236000000nmeFAAQ'
;