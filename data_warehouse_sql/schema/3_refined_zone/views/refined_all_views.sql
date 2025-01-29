--6
drop view if exists ft_ds_refined.curriculum_listing_sessions_view;
--5
drop view if exists ft_ds_refined.partner_organization_account_view;
--4 (depends on partner_organization_account_view)
drop view if exists ft_ds_refined.chapter_account_view;
--3
drop VIEW if exists ft_ds_refined.active_participants_view;
--2
drop view if exists ft_ds_refined.program_location_account_view;
--1 
drop view if exists ft_ds_refined.additional_trade_name_account_view;

--A 
drop view if exists ft_ds_refined.metric_historical_daily_active_participant_counts_view;
--B
drop view if exists ft_ds_refined.metric_historical_daily_ethnic_diversity_percentage_view;
--C
drop view if exists ft_ds_refined.metric_historical_daily_female_percentage_view;
--D
drop VIEW if exists ft_ds_refined.metric_historical_daily_retention_percentage_view;
--E
drop VIEW if exists ft_ds_refined.metric_historical_daily_teen_retention_percentage_view;
--F
drop VIEW if exists ft_ds_refined.metric_historical_daily_tenure_counts_view;
--G
drop VIEW if exists ft_ds_refined.metric_historical_daily_twelve_plus_retention_percentage_view;
--H
drop view if exists ft_ds_refined.metric_historical_daily_twelve_up_engagement_counts_view;
--I
drop view if exists ft_ds_refined.metric_historical_daily_historical_underserved_areas_counts_view;
--J
drop view if exists ft_ds_refined.household_account_view;
--K
drop view if exists ft_ds_refined.listings_view;
--L
drop view if exists ft_ds_refined.organization_account_view;
--M
drop view if exists ft_ds_refined.parent_partner_organization_account_view;
--N
drop view if exists ft_ds_refined.registered_session_registrations_view;

--N
CREATE OR REPLACE VIEW ft_ds_refined.registered_session_registrations_view
AS
SELECT
    session_registration.session_registration_id_18,
    session_registration.original_session_charge_amount,
    session_registration.contact_id_18,
    contact.first_name || ' ' || contact.last_name as contact_name,
    session_registration.cost_difference,
    session_registration.sf_created_by_id,
    session_registration.sf_created_timestamp,
    session_registration.discount,
    session_registration.item_price,
    session_registration.sf_last_modified_by_id,
    session_registration.sf_last_modified_timestamp,
    session_registration.listing_session_id_18,
    listing_session.listing_session_name as listing_session_name,
    session_registration.membership_registration_id_18,
    session_registration.session_registration_number,
    session_registration.old_listing_session_id_18,
    old_listing_session.listing_session_name as old_listing_session_name,
    session_registration.new_session_cost,
    session_registration.reggie_registration_id,
    session_registration.session_type,
    session_registration.status,
    session_registration.is_transferred,
    session_registration.waitlist_process,
    session_registration.original_session_registration_number,
    session_registration.sf_system_modstamp,
    session_registration.dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.session_registration session_registration
left join ft_ds_refined.contact contact
	on session_registration.contact_id_18 = contact.contact_id_18
left join ft_ds_refined.listing_session listing_session
	on session_registration.listing_session_id_18 = listing_session.listing_session_id_18
left join ft_ds_refined.listing_session old_listing_session
	on session_registration.old_listing_session_id_18 = old_listing_session.listing_session_id_18
WHERE
    session_registration.status = 'Registered'
    --the remainder of fields cannot be NULL since they are soft-required.
    AND session_registration.contact_id_18 IS NOT NULL
    AND session_registration.sf_created_by_id IS NOT NULL
    AND session_registration.sf_created_timestamp IS NOT NULL
    AND session_registration.sf_last_modified_by_id IS NOT NULL
    AND session_registration.sf_last_modified_timestamp IS NOT NULL
    AND session_registration.listing_session_id_18 IS NOT NULL
    AND session_registration.session_registration_number IS NOT NULL
    AND session_registration.status IS NOT NULL
    AND session_registration.dss_ingestion_timestamp IS NOT NULL
;

--M
CREATE OR REPLACE VIEW ft_ds_refined.parent_partner_organization_account_view
AS
SELECT
    account.account_name,
    account.parent_account,
    account.partner_program_type,
    account.location_type,
    --NOT IN VALID AS affiliate_delivery_partner,
    account.account_record_type_id,
    account.chapter_affiliation_id,
    account_chapter.account_name as chapter_affiliation_name,
    account.additional_trade_name_chapter_affiliation_id,
    account_additional.account_name as additional_trade_name_chapter_affiliation_name,
    account.territory,
    account.is_active,
    account.account_id,
    --NOT IN RAW AS fmp_id,
    account.mdr_pid,
    account.ys_report_chapter_affiliation,
    account.date_joined,
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

--L
CREATE OR REPLACE VIEW ft_ds_refined.organization_account_view
AS
SELECT
    account.account_name,
    account.parent_account,
    account_parent_account.account_name as parent_account_name,
    account.primary_contact_id,
    contact.first_name || ' ' || contact.last_name as primary_contact_name,
    --NOT IN RAW AS county,
    account.account_record_type_id,
    account.type,
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

--K
CREATE OR REPLACE VIEW ft_ds_refined.listings_view
AS
SELECT
    listing.listing_id_18,
    listing.account_id,
    account.account_name,
    listing.start_date,
    listing.end_date,
    listing.hosted_by,
    account_hosted_by.account_name as hosted_by_name,
    listing.listing_location_address,
    listing.listing_name,
    listing.presented_by,
    account_presented_by.account_name as presented_by_name,
    listing.publish_start_date,
    listing.publish_end_date,
    listing.record_type_id,
    listing.sf_created_timestamp,
    listing.sf_last_modified_timestamp,
    listing.sf_system_modstamp,
    listing.dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.listing listing
left join ft_ds_refined.account account
	on listing.account_id = account.account_id
left join ft_ds_refined.account account_hosted_by
	on listing.hosted_by = account_hosted_by.account_id
left join ft_ds_refined.account account_presented_by
	on listing.presented_by = account_presented_by.account_id
WHERE
    --the remainder of fields cannot be NULL since they are soft-required.
    listing.account_id IS NOT NULL
    AND listing.sf_created_timestamp IS NOT NULL
    AND listing.end_date IS NOT NULL
    AND listing.hosted_by IS NOT NULL
    AND listing.listing_name IS NOT NULL
    AND listing.presented_by IS NOT NULL
    AND listing.publish_start_date IS NOT NULL
    AND listing.publish_end_date IS NOT NULL
    AND listing.record_type_id IS NOT NULL
    AND listing.start_date IS NOT NULL
    AND listing.dss_ingestion_timestamp IS NOT NULL
;

--J
CREATE OR REPLACE VIEW ft_ds_refined.household_account_view
AS
SELECT
    account.account_name,
    account.chapter_affiliation_id,
    account_chapter.account_name as chapter_name,
    --NOT IN RAW AS number_of_active_participants,
    account.account_record_type_id,
    account.primary_contact_id,
    contact.first_name || ' ' || contact.last_name as primary_contact_name,
    --NOT IN VALID AS secondary_contact,
    account.description,
    account.account_id,
    account.parent_account,
    account_parent_account.account_name as parent_account_name,
    --NOT IN RAW AS fmp_id,
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

--I
CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_historical_underserved_areas_counts_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        account.account_name as chapter_name,
        info.count_in_underserved_areas
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_underserved_areas_counts
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id
    ) dates
    JOIN
        ft_ds_refined.metric_historical_underserved_areas_counts info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
    left join ft_ds_refined.account account 
    	on info.chapter_id = account.account_id
;

--H
CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_twelve_up_engagement_counts_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        account.account_name as chapter_name,
        info.twelve_up_engagement_counts,
        info.twelve_up_total_counts,
        info.twelve_up_engagement_percentage
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_twelve_up_engagement_counts
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id
    ) dates
    JOIN
        ft_ds_refined.metric_historical_twelve_up_engagement_counts info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
    left join ft_ds_refined.account account 
    	on info.chapter_id = account.account_id
;

--G
CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_twelve_plus_retention_percentage_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        account.account_name as chapter_name,
        info.twelve_plus_retention_percentage
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_twelve_plus_retention_percentage
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id
    ) dates
    JOIN
        ft_ds_refined.metric_historical_twelve_plus_retention_percentage info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
    left join ft_ds_refined.account account 
    	on info.chapter_id = account.account_id
;

--F
CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_tenure_counts_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        account.account_name as chapter_name,
        info.one_year_tenure_count,
        info.two_year_tenure_count,
        info.three_year_tenure_count,
        info.four_year_tenure_count,
        info.five_year_tenure_count,
        info.six_plus_year_tenure_count,
        info.total_count
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_tenure_counts_view
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id
    ) dates
    JOIN
        ft_ds_refined.metric_historical_tenure_counts_view info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
    left join ft_ds_refined.account account 
    	on info.chapter_id = account.account_id
;

--E
CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_teen_retention_percentage_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        account.account_name as chapter_name,
        info.teen_retention_percentage
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_teen_retention_percentage
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id
    ) dates
    JOIN
        ft_ds_refined.metric_historical_teen_retention_percentage info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
    left join ft_ds_refined.account account 
    	on info.chapter_id = account.account_id
;

--D
CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_retention_percentage_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        account.account_name as chapter_name,
        info.retention_percentage
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_retention_percentage
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id
    ) dates
    JOIN
        ft_ds_refined.metric_historical_retention_percentage info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
    left join ft_ds_refined.account account 
    	on info.chapter_id = account.account_id
;

--C
CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_female_percentage_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        account.account_name as chapter_name,
        info.female_percentage
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_female_percentage
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id
    ) dates
    JOIN
        ft_ds_refined.metric_historical_female_percentage info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
    left join ft_ds_refined.account account 
    	on info.chapter_id = account.account_id
;

--B
CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_ethnic_diversity_percentage_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        account.account_name as chapter_name,
        info.ethnic_diversity_percentage
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_ethnic_diversity_percentage
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id
    ) dates
    JOIN
        ft_ds_refined.metric_historical_ethnic_diversity_percentage info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
    left join ft_ds_refined.account account 
    	on info.chapter_id = account.account_id
;

--A
CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_active_participant_counts_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        account.account_name as chapter_name,
        info.participant_count
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_active_participant_counts
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id
    ) dates
    JOIN
        ft_ds_refined.metric_historical_active_participant_counts info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
	left join ft_ds_refined.account account 
    	on info.chapter_id = account.account_id

;

--1
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

--2
CREATE OR REPLACE VIEW ft_ds_refined.program_location_account_view
AS SELECT program_location.account_id,
    program_location.account_name,
    parent_account_ids.account_name AS parent_account_name,
    program_location.parent_account_id,
    additional_trade_name_ids.account_name AS additional_trade_name,
    program_location.additional_trade_name_id,
    program_location.signed_facility_use_agreement,
    program_location.account_record_type_id,
    program_location.territory,
    program_location.billing_street,
    program_location.billing_city,
    program_location.billing_state,
    program_location.billing_postal_code,
    program_location.billing_country,
    program_location.shipping_street,
    program_location.shipping_city,
    program_location.shipping_state,
    program_location.shipping_postal_code,
    program_location.shipping_country,
    program_location.number_of_holes,
    program_location.if_other_fill_in_how_many,
    program_location.golf_course_type,
    program_location.discounts_offered_to_participants,
    program_location.discount_description,
    program_location.chapter_owns_this_facility,
    program_location.dedicated_first_tee_learning_center,
    program_location.learning_center_description,
    program_location.if_yes_how_long_is_the_lease,
    program_location.lease_holder,
    program_location.is_active
   FROM ft_ds_refined.account program_location
     LEFT JOIN ft_ds_refined.account parent_account_ids ON program_location.parent_account_id = parent_account_ids.account_id
     LEFT JOIN ft_ds_refined.additional_trade_name_account_view additional_trade_name_ids ON program_location.additional_trade_name_id = additional_trade_name_ids.account_id
  WHERE program_location.account_record_type_id = '01236000000nmeHAAQ'::bpchar;
;

--3
CREATE OR REPLACE VIEW ft_ds_refined.active_participants_view
AS
SELECT
    contact.contact_id_18,
    chapter_affiliation.account_name as chapter_name,
    contact.chapter_id,
    contact.contact_type,
    --contact.age,
    contact.ethnicity,
    contact.gender,
    contact.grade,
    contact.participation_status,
    contact.mailing_zip_postal_code,
    contact.mailing_street,
    contact.mailing_city,
    contact.mailing_state,
    contact.school_name,
    contact.school_name_other,
    contact.first_name,
    contact.last_name,
    contact.birthdate,
    household.account_name as household_name,
    contact.household_id,
    contact.sf_created_timestamp,
    contact.sf_last_modified_timestamp,
    contact.sf_system_modstamp,
    contact.dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.contact contact
LEFT JOIN ft_ds_refined.account chapter_affiliation
    ON contact.chapter_id = chapter_affiliation.account_id
LEFT JOIN ft_ds_refined.account household
    ON contact.household_id = household.account_id
WHERE
    contact.contact_type LIKE '%Participant%'
    AND contact.participation_status = 'Active'
    AND contact.first_name IS NOT NULL
    AND contact.last_name IS NOT NULL
    AND contact.birthdate IS NOT NULL
    --AND contact.age IS NOT NULL
    AND contact.gender IS NOT NULL
    AND contact.ethnicity IS NOT NULL
    AND contact.household_id IS NOT NULL
    AND contact.mailing_zip_postal_code IS NOT NULL
    AND contact.chapter_id IS NOT NULL
    AND contact.dss_ingestion_timestamp IS NOT NULL
;

-- 4
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

--5
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

--6
CREATE OR REPLACE VIEW ft_ds_refined.curriculum_listing_sessions_view
AS
SELECT
    listing_session.listing_session_id_18,
    listing_session.additional_trade_name,
    listing_session.age_eligibility_date,
    listing_session.age_restriction,
    listing_session.allow_early_registration,
    listing_session.base_price,
    listing_session.coach_assigned,
    contact_coach.first_name || ' ' || contact_coach.last_name as coach_name, 
    listing_session.curriculum_hours,
    listing_session.days_offered,
    listing_session.event_coordinator,
    listing_session.event_hours,
    listing_session.full_description,
    listing_session.gender_restriction,
    listing_session.lesson_plan,
    listing_session.listing_session_location_address,
    listing_session.program_location_id,
    listing_session.program_location_name,
    listing_session.listing_id,
    listing.listing_name,
    listing_session.max_capacity,
    listing_session.maximum_age,
    listing_session.membership_discount_active,
    listing_session.membership_id,
    listing_session.membership_required,
    listing_session.military_discount_active,
    listing_session.minimum_age,
    listing_session.listing_session_name,
    listing_session.number_of_classes,
    listing_session.parent_communication_french,
    listing_session.parent_communication_spanish,
    listing_session.parent_communication,
    listing_session.chapter_name,
    listing_session.chapter_id,
    listing_session.primary_program_level_restriction,
    listing_session.priority,
    listing_session.private_event,
    listing_session.program_coordinator,
    listing_session.program_level,
    listing_session.program_sub_level,
    listing_session.program_type,
    listing_session.publish_end_date_time,
    listing_session.publish_start_date_time,
    listing_session.record_type_id,
    listing_session.register_end_date_time,
    listing_session.register_start_date_time,
    listing_session.season,
    listing_session.secondary_program_level_restriction,
    listing_session.session_end_date_time,
    listing_session.session_id,
    listing_session.session_start_date_time,
    listing_session.session_start_date,
    listing_session.session_status,
    listing_session.sibling_discount_active,
    listing_session.support_coach_1,
    listing_session.support_coach_2,
    listing_session.support_coach_3,
    listing_session.support_coach_4,
    listing_session.support_coach_5,
    listing_session.support_coach_6,
    listing_session.third_program_level_restriction,
    listing_session.total_registrations,
    listing_session.total_space_available,
    listing_session.waitlist_space_available,
    listing_session.waitlist_capacity,
    listing_session.waitlist_counter_new,
    listing_session.sf_created_timestamp,
    listing_session.sf_last_modified_timestamp,
    listing_session.sf_system_modstamp,
    listing_session.dss_ingestion_timestamp
-- ommitted the is_deleted field since it is an unanalyzable metadata field.
FROM ft_ds_refined.listing_session listing_session
left join ft_ds_refined.contact contact_coach
	on listing_session.coach_assigned = contact_coach.contact_id_18
left join ft_ds_refined.listing listing 
	on listing_session.listing_id = listing.listing_id_18
WHERE
    --this is the id representing the Curriculum type
    listing_session.record_type_id = '01236000000nmeLAAQ'
    --the remainder of fields cannot be NULL since they are soft-required.
    AND listing_session.base_price IS NOT NULL
    AND listing_session.sf_created_timestamp IS NOT NULL
    AND listing_session.max_capacity IS NOT NULL
    AND listing_session.chapter_name IS NOT NULL
    AND listing_session.chapter_id IS NOT NULL
    AND listing_session.record_type_id IS NOT NULL
    AND listing_session.dss_ingestion_timestamp IS NOT NULL
;
