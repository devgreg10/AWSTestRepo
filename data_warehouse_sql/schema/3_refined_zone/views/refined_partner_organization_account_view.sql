CREATE OR REPLACE VIEW ft_ds_refined.partner_organization_account_view
AS
SELECT    
    partner_org.account_id,
    partner_org.account_name,
    parent_account_ids.account_name AS parent_account_name,
    partner_org.parent_account_id,
    partner_org.partner_program_type,
    partner_org.location_type,
    partner_org.title_i,
    partner_org.organization_state,
    partner_org.organization_city,
    --record_type_ids.account_record_type_name,
    --this record type object is not imported to DSS yet
    partner_org.account_record_type_id,
    chapter_ids.account_name AS chapter_affiliation_name,
    partner_org.chapter_affiliation_id,
    partner_org.territory,
    partner_org.enrollment,
    partner_org.pre_school AS services_pre_school,
    partner_org.kindergarten AS services_kindergarten,
    --NOT IN RAW AS services_grade_1,
    --NOT IN RAW AS services_grade_2,
    --NOT IN RAW AS services_grade_3,
    --NOT IN RAW AS services_grade_4,
    --NOT IN RAW AS services_grade_5,
    --NOT IN RAW AS services_grade_6,
    --NOT IN RAW AS services_grade_7,
    --NOT IN RAW AS services_grade_8,
    --NOT IN RAW AS services_grade_9,
    --NOT IN RAW AS services_grade_10,
    --NOT IN RAW AS services_grade_11,
    --NOT IN RAW AS services_grade_12,
    --NOT IN RAW AS services_grade_13,
    partner_org.billing_street,
    partner_org.billing_city,
    partner_org.billing_state,
    partner_org.billing_postal_code,
    partner_org.billing_country,
    partner_org.shipping_street,
    partner_org.shipping_city,
    partner_org.shipping_state,
    partner_org.shipping_postal_code,
    partner_org.shipping_country,
    partner_org.is_active,
    partner_org.date_joined,
    --nces does not seem to be a Salesforce object, so it will not be joined as of now. Upon import of National Center for Education Statistics data, supplement this view with information based on the below ID
    partner_org.nces_id
FROM ft_ds_refined.account partner_org
LEFT JOIN ft_ds_refined.account parent_account_ids-- parent_account_ids come from accounts of all types, so cannot use one of the more restrictive views
    ON partner_org.parent_account_id = parent_account_ids.account_id
LEFT JOIN ft_ds_refined.chapter_account_view chapter_ids
    ON partner_org.chapter_affiliation_id = chapter_ids.account_id
WHERE
    partner_org.account_record_type_id = '01236000001M1f7AAC'
;