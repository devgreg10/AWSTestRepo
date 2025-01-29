CREATE OR REPLACE VIEW ft_ds_refined.chapter_account_view
AS
SELECT
    account.account_name,
    account.legal_entity_name,
    account.board_chair,
    contact_board_chair.first_name || ' ' || contact_board_chair.last_name as board_chair_name,
    account.executive_director,
    contact_executive_director.first_name || ' ' || contact_executive_director.last_name as executive_director_name,
    account.program_director_id,
    contact_program_director.first_name || ' ' || contact_program_director.last_name as program_director_name,
    account.payments_accepted_in_person,
    --NOT IN RAW AS sfdc_go_live_date,
    account.international_chapter,
    account.ft_app_pilot,
    --NOT IN VALID AS award_badges_enabled,
    account.account_record_type_id,
    account.is_active,
    account.contract_status,
    account.contract_effective_date,
    account.contract_expiration_date,
    account.insurance_expiration_date,
    account.dcr,
    account.territory,
    account.peer_group AS peer_group_level,
    account.governance_structure,
    account.service_area,
    account.youth_population,
    account.chapter_standing,
    account.former_trade_names,
    account.former_legal_entity,
    account.partners_in_market,
    --NOT IN RAW AS payment_provider,
    account.billing_street,
    account.billing_city,
    account.billing_state,
    account.billing_postal_code,
    account.billing_country,
    account.shipping_street,
    account.shipping_city,
    account.shipping_state,
    account.shipping_postal_code,
    account.shipping_country,
    --NOT IN RAW AS county,
    account.membership_offered,
    account.membership_active,
    account.chapter_membership_price,
    account.membership_start_date,
    account.membership_end_date,
    account.membership_discount_amount,
    account.membership_discount_percentage,
    account.sibling_discount_amount,
    account.sibling_discount_percentage,
    military_discount_amount,
    military_discount_percentage,
    account.account_inactive_date,
    account.time_zone,
    account.new_parent_registration,
    account.ys_report_chapter_affiliation,
    account.account_id,
    account.reggie_name,
    --NOT IN RAW AS fmp_id,
    --NOT IN RAW AS fmp_chapter_code,
    --NOT IN RAW AS mrm_id,
    --NOT IN RAW AS legacy_account_holder_id,
    --NOT IN RAW AS legacy_participant_user_id,
    account.reggie_location_id,
    account.reggie_account_id,
    account.reggie_id
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.account account
left join ft_ds_refined.contact contact_board_chair
	on account.board_chair = contact_board_chair.contact_id_18
left join ft_ds_refined.contact contact_executive_director
	on account.executive_director = contact_executive_director.contact_id_18
left join ft_ds_refined.contact contact_program_director
	on account.program_director_id = contact_program_director.contact_id_18
WHERE
    account.account_record_type_id = '01236000000nmeEAAQ'
;


