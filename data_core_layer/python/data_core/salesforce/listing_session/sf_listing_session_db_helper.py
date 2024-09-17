from data_core.salesforce.listing_session.sf_listing_session_db_models import SfListingSessionRawDbModel, SfListingSessionSourceModel, map_listing_session_sources_to_raws
from data_core.util.db_execute_helper import DbExecutorHelper
from psycopg2.extensions import connection
from datetime import datetime

import psycopg2
from typing import List

import logging

class SalesforceListingSessionDbHelper:

    @staticmethod
    def insert_sf_raw_listing_sessions_from_source_listing_sessions(*,
                                                    db_connection: connection,
                                                    source_listing_sessions: List[SfListingSessionSourceModel],
                                                    commit_changes: bool = True):
        
        SalesforceListingSessionDbHelper.insert_sf_raw_listing_sessions(db_connection = db_connection,
                                                         new_raw_listing_sessions=map_listing_session_sources_to_raws(source_listing_sessions))

    @staticmethod
    def insert_sf_raw_listing_sessions(*, 
                              db_connection: connection, 
                              new_raw_listing_sessions: List[SfListingSessionRawDbModel],
                              commit_changes: bool = True):
    
        """
        Inserts a collection of SfListingSessionRawDbModel objects into PostgreSQL.
        If a primary key violation occurs, updates the conflicting record.
        
        Args:
        - listing_sessions: List of SfListingSessionRawDbModel instances.
        - conn: A pre-existing psycopg2 connection object.
        """
        # Use the provided connection to create a cursor
        cursor = db_connection.cursor()

        # Define the upsert SQL statement
        upsert_query = f"""
        INSERT INTO ft_ds_raw.sf_listing_session (
            id, lastmodifieddate, isdeleted, createddate, name, recordtypeid, createdbyid,
            lastmodifiedbyid, systemmodstamp, lastactivitydate, lastvieweddate, lastreferenceddate,
            listing__c, actual_price__c, age_restriction__c, base_price__c, brief_description__c,
            capacity_notification_threshold__c, coach_assigned__c, confirmation_supporting_notes__c,
            curriculum_hours__c, event_coordinator__c, full_description__c, gender_restriction__c,
            listing_session_location_address__c, listing_session_location_name__c, max_capacity__c,
            maximum_age__c, membership_discount_active__c, membership_id__c, membership_required__c,
            military_discount_active__c, minimum_age__c, number_of_classes__c, youth_serving_program_type__c,
            owner__c, participants_reached__c, presented_by__c, primary_image__c, 
            primary_program_level_restriction__c, program_level__c, program_sub_level__c,
            publish_end_date_time__c, publish_start_date_time__c, reggie_eventkey__c, reggie_eventtype__c,
            reggie_event_id__c, register_end_date_time__c, register_start_date_time__c, 
            schedule_price__c, season__c, secondary_program_level_restriction__c, session_end_date_time__c,
            session_id__c, session_start_date_time__c, session_status__c, sibling_discount_active__c,
            support_coach_1__c, support_coach_2__c, threshold_notification_email__c, 
            total_space_available__c, total_registrations__c, website__c, program_coordinator__c,
            listing_session_location__c, presented_by_name__c, region__c, days_offered__c, 
            third_program_level_restriction__c, count_listing_session__c, priority__c, additional_trade_name__c,
            can_be_registered__c, waitlist_counter_new__c, support_coach_3_del__c, support_coach_4_del__c,
            support_coach_5_del__c, support_coach_6_del__c, parent_communication__c, x18_digit_id__c,
            waitlist_space_available__c, waitlist_capacity__c, event_hours__c, session_end_date__c,
            session_end_time__c, session_start_date__c, session_start_time__c, age_eligibility_date__c,
            allow_early_registration__c, direct_session_link__c, private_event__c, 
            parent_communication_french__c, parent_communication_spanish__c, program_type__c, lesson_plan__c, dss_ingestion_timestamp
        ) 
        VALUES (
            %(id)s, %(lastmodifieddate)s, %(isdeleted)s, %(createddate)s, %(name)s, %(recordtypeid)s, 
            %(createdbyid)s, %(lastmodifiedbyid)s, %(systemmodstamp)s, %(lastactivitydate)s, %(lastvieweddate)s, 
            %(lastreferenceddate)s, %(listing__c)s, %(actual_price__c)s, %(age_restriction__c)s, %(base_price__c)s,
            %(brief_description__c)s, %(capacity_notification_threshold__c)s, %(coach_assigned__c)s, 
            %(confirmation_supporting_notes__c)s, %(curriculum_hours__c)s, %(event_coordinator__c)s, 
            %(full_description__c)s, %(gender_restriction__c)s, %(listing_session_location_address__c)s, 
            %(listing_session_location_name__c)s, %(max_capacity__c)s, %(maximum_age__c)s, 
            %(membership_discount_active__c)s, %(membership_id__c)s, %(membership_required__c)s, 
            %(military_discount_active__c)s, %(minimum_age__c)s, %(number_of_classes__c)s, 
            %(youth_serving_program_type__c)s, %(owner__c)s, %(participants_reached__c)s, %(presented_by__c)s, 
            %(primary_image__c)s, %(primary_program_level_restriction__c)s, %(program_level__c)s, 
            %(program_sub_level__c)s, %(publish_end_date_time__c)s, %(publish_start_date_time__c)s, 
            %(reggie_eventkey__c)s, %(reggie_eventtype__c)s, %(reggie_event_id__c)s, %(register_end_date_time__c)s, 
            %(register_start_date_time__c)s, %(schedule_price__c)s, %(season__c)s, 
            %(secondary_program_level_restriction__c)s, %(session_end_date_time__c)s, %(session_id__c)s, 
            %(session_start_date_time__c)s, %(session_status__c)s, %(sibling_discount_active__c)s, 
            %(support_coach_1__c)s, %(support_coach_2__c)s, %(threshold_notification_email__c)s, 
            %(total_space_available__c)s, %(total_registrations__c)s, %(website__c)s, %(program_coordinator__c)s, 
            %(listing_session_location__c)s, %(presented_by_name__c)s, %(region__c)s, %(days_offered__c)s, 
            %(third_program_level_restriction__c)s, %(count_listing_session__c)s, %(priority__c)s, 
            %(additional_trade_name__c)s, %(can_be_registered__c)s, %(waitlist_counter_new__c)s, 
            %(support_coach_3_del__c)s, %(support_coach_4_del__c)s, %(support_coach_5_del__c)s, 
            %(support_coach_6_del__c)s, %(parent_communication__c)s, %(x18_digit_id__c)s, 
            %(waitlist_space_available__c)s, %(waitlist_capacity__c)s, %(event_hours__c)s, 
            %(session_end_date__c)s, %(session_end_time__c)s, %(session_start_date__c)s, 
            %(session_start_time__c)s, %(age_eligibility_date__c)s, %(allow_early_registration__c)s, 
            %(direct_session_link__c)s, %(private_event__c)s, %(parent_communication_french__c)s, 
            %(parent_communication_spanish__c)s, %(program_type__c)s, %(lesson_plan__c)s, '{datetime.now()}'
        )
        ON CONFLICT (id, systemmodstamp) 
        DO UPDATE SET
            lastmodifieddate = EXCLUDED.lastmodifieddate,
            isdeleted = EXCLUDED.isdeleted,
            createddate = EXCLUDED.createddate,
            name = EXCLUDED.name,
            recordtypeid = EXCLUDED.recordtypeid,
            createdbyid = EXCLUDED.createdbyid,
            lastmodifiedbyid = EXCLUDED.lastmodifiedbyid,
            lastactivitydate = EXCLUDED.lastactivitydate,
            lastvieweddate = EXCLUDED.lastvieweddate,
            lastreferenceddate = EXCLUDED.lastreferenceddate,
            listing__c = EXCLUDED.listing__c,
            actual_price__c = EXCLUDED.actual_price__c,
            age_restriction__c = EXCLUDED.age_restriction__c,
            base_price__c = EXCLUDED.base_price__c,
            brief_description__c = EXCLUDED.brief_description__c,
            capacity_notification_threshold__c = EXCLUDED.capacity_notification_threshold__c,
            coach_assigned__c = EXCLUDED.coach_assigned__c,
            confirmation_supporting_notes__c = EXCLUDED.confirmation_supporting_notes__c,
            curriculum_hours__c = EXCLUDED.curriculum_hours__c,
            event_coordinator__c = EXCLUDED.event_coordinator__c,
            full_description__c = EXCLUDED.full_description__c,
            gender_restriction__c = EXCLUDED.gender_restriction__c,
            listing_session_location_address__c = EXCLUDED.listing_session_location_address__c,
            listing_session_location_name__c = EXCLUDED.listing_session_location_name__c,
            max_capacity__c = EXCLUDED.max_capacity__c,
            maximum_age__c = EXCLUDED.maximum_age__c,
            membership_discount_active__c = EXCLUDED.membership_discount_active__c,
            membership_id__c = EXCLUDED.membership_id__c,
            membership_required__c = EXCLUDED.membership_required__c,
            military_discount_active__c = EXCLUDED.military_discount_active__c,
            minimum_age__c = EXCLUDED.minimum_age__c,
            number_of_classes__c = EXCLUDED.number_of_classes__c,
            youth_serving_program_type__c = EXCLUDED.youth_serving_program_type__c,
            owner__c = EXCLUDED.owner__c,
            participants_reached__c = EXCLUDED.participants_reached__c,
            presented_by__c = EXCLUDED.presented_by__c,
            primary_image__c = EXCLUDED.primary_image__c,
            primary_program_level_restriction__c = EXCLUDED.primary_program_level_restriction__c,
            program_level__c = EXCLUDED.program_level__c,
            program_sub_level__c = EXCLUDED.program_sub_level__c,
            publish_end_date_time__c = EXCLUDED.publish_end_date_time__c,
            publish_start_date_time__c = EXCLUDED.publish_start_date_time__c,
            reggie_eventkey__c = EXCLUDED.reggie_eventkey__c,
            reggie_eventtype__c = EXCLUDED.reggie_eventtype__c,
            reggie_event_id__c = EXCLUDED.reggie_event_id__c,
            register_end_date_time__c = EXCLUDED.register_end_date_time__c,
            register_start_date_time__c = EXCLUDED.register_start_date_time__c,
            schedule_price__c = EXCLUDED.schedule_price__c,
            season__c = EXCLUDED.season__c,
            secondary_program_level_restriction__c = EXCLUDED.secondary_program_level_restriction__c,
            session_end_date_time__c = EXCLUDED.session_end_date_time__c,
            session_id__c = EXCLUDED.session_id__c,
            session_start_date_time__c = EXCLUDED.session_start_date_time__c,
            session_status__c = EXCLUDED.session_status__c,
            sibling_discount_active__c = EXCLUDED.sibling_discount_active__c,
            support_coach_1__c = EXCLUDED.support_coach_1__c,
            support_coach_2__c = EXCLUDED.support_coach_2__c,
            threshold_notification_email__c = EXCLUDED.threshold_notification_email__c,
            total_space_available__c = EXCLUDED.total_space_available__c,
            total_registrations__c = EXCLUDED.total_registrations__c,
            website__c = EXCLUDED.website__c,
            program_coordinator__c = EXCLUDED.program_coordinator__c,
            listing_session_location__c = EXCLUDED.listing_session_location__c,
            presented_by_name__c = EXCLUDED.presented_by_name__c,
            region__c = EXCLUDED.region__c,
            days_offered__c = EXCLUDED.days_offered__c,
            third_program_level_restriction__c = EXCLUDED.third_program_level_restriction__c,
            count_listing_session__c = EXCLUDED.count_listing_session__c,
            priority__c = EXCLUDED.priority__c,
            additional_trade_name__c = EXCLUDED.additional_trade_name__c,
            can_be_registered__c = EXCLUDED.can_be_registered__c,
            waitlist_counter_new__c = EXCLUDED.waitlist_counter_new__c,
            support_coach_3_del__c = EXCLUDED.support_coach_3_del__c,
            support_coach_4_del__c = EXCLUDED.support_coach_4_del__c,
            support_coach_5_del__c = EXCLUDED.support_coach_5_del__c,
            support_coach_6_del__c = EXCLUDED.support_coach_6_del__c,
            parent_communication__c = EXCLUDED.parent_communication__c,
            x18_digit_id__c = EXCLUDED.x18_digit_id__c,
            waitlist_space_available__c = EXCLUDED.waitlist_space_available__c,
            waitlist_capacity__c = EXCLUDED.waitlist_capacity__c,
            event_hours__c = EXCLUDED.event_hours__c,
            session_end_date__c = EXCLUDED.session_end_date__c,
            session_end_time__c = EXCLUDED.session_end_time__c,
            session_start_date__c = EXCLUDED.session_start_date__c,
            session_start_time__c = EXCLUDED.session_start_time__c,
            age_eligibility_date__c = EXCLUDED.age_eligibility_date__c,
            allow_early_registration__c = EXCLUDED.allow_early_registration__c,
            direct_session_link__c = EXCLUDED.direct_session_link__c,
            private_event__c = EXCLUDED.private_event__c,
            parent_communication_french__c = EXCLUDED.parent_communication_french__c,
            parent_communication_spanish__c = EXCLUDED.parent_communication_spanish__c,
            program_type__c = EXCLUDED.program_type__c,
            lesson_plan__c = EXCLUDED.lesson_plan__c,
            dss_ingestion_timestamp='{datetime.now}';
        """

        # Prepare the data
        records = [
            {
                "id": listing_session.id,
                "lastmodifieddate": listing_session.lastmodifieddate,
                "isdeleted": listing_session.isdeleted,
                "createddate": listing_session.createddate,
                "name": listing_session.name,
                "recordtypeid": listing_session.recordtypeid,
                "createdbyid": listing_session.createdbyid,
                "lastmodifiedbyid": listing_session.lastmodifiedbyid,
                "systemmodstamp": listing_session.systemmodstamp,
                "lastactivitydate": listing_session.lastactivitydate,
                "lastvieweddate": listing_session.lastvieweddate,
                "lastreferenceddate": listing_session.lastreferenceddate,
                "listing__c": listing_session.listing__c,
                "actual_price__c": listing_session.actual_price__c,
                "age_restriction__c": listing_session.age_restriction__c,
                "base_price__c": listing_session.base_price__c,
                "brief_description__c": listing_session.brief_description__c,
                "capacity_notification_threshold__c": listing_session.capacity_notification_threshold__c,
                "coach_assigned__c": listing_session.coach_assigned__c,
                "confirmation_supporting_notes__c": listing_session.confirmation_supporting_notes__c,
                "curriculum_hours__c": listing_session.curriculum_hours__c,
                "event_coordinator__c": listing_session.event_coordinator__c,
                "full_description__c": listing_session.full_description__c,
                "gender_restriction__c": listing_session.gender_restriction__c,
                "listing_session_location_address__c": listing_session.listing_session_location_address__c,
                "listing_session_location_name__c": listing_session.listing_session_location_name__c,
                "max_capacity__c": listing_session.max_capacity__c,
                "maximum_age__c": listing_session.maximum_age__c,
                "membership_discount_active__c": listing_session.membership_discount_active__c,
                "membership_id__c": listing_session.membership_id__c,
                "membership_required__c": listing_session.membership_required__c,
                "military_discount_active__c": listing_session.military_discount_active__c,
                "minimum_age__c": listing_session.minimum_age__c,
                "number_of_classes__c": listing_session.number_of_classes__c,
                "youth_serving_program_type__c": listing_session.youth_serving_program_type__c,
                "owner__c": listing_session.owner__c,
                "participants_reached__c": listing_session.participants_reached__c,
                "presented_by__c": listing_session.presented_by__c,
                "primary_image__c": listing_session.primary_image__c,
                "primary_program_level_restriction__c": listing_session.primary_program_level_restriction__c,
                "program_level__c": listing_session.program_level__c,
                "program_sub_level__c": listing_session.program_sub_level__c,
                "publish_end_date_time__c": listing_session.publish_end_date_time__c,
                "publish_start_date_time__c": listing_session.publish_start_date_time__c,
                "reggie_eventkey__c": listing_session.reggie_eventkey__c,
                "reggie_eventtype__c": listing_session.reggie_eventtype__c,
                "reggie_event_id__c": listing_session.reggie_event_id__c,
                "register_end_date_time__c": listing_session.register_end_date_time__c,
                "register_start_date_time__c": listing_session.register_start_date_time__c,
                "schedule_price__c": listing_session.schedule_price__c,
                "season__c": listing_session.season__c,
                "secondary_program_level_restriction__c": listing_session.secondary_program_level_restriction__c,
                "session_end_date_time__c": listing_session.session_end_date_time__c,
                "session_id__c": listing_session.session_id__c,
                "session_start_date_time__c": listing_session.session_start_date_time__c,
                "session_status__c": listing_session.session_status__c,
                "sibling_discount_active__c": listing_session.sibling_discount_active__c,
                "support_coach_1__c": listing_session.support_coach_1__c,
                "support_coach_2__c": listing_session.support_coach_2__c,
                "threshold_notification_email__c": listing_session.threshold_notification_email__c,
                "total_space_available__c": listing_session.total_space_available__c,
                "total_registrations__c": listing_session.total_registrations__c,
                "website__c": listing_session.website__c,
                "program_coordinator__c": listing_session.program_coordinator__c,
                "listing_session_location__c": listing_session.listing_session_location__c,
                "presented_by_name__c": listing_session.presented_by_name__c,
                "region__c": listing_session.region__c,
                "days_offered__c": listing_session.days_offered__c,
                "third_program_level_restriction__c": listing_session.third_program_level_restriction__c,
                "count_listing_session__c": listing_session.count_listing_session__c,
                "priority__c": listing_session.priority__c,
                "additional_trade_name__c": listing_session.additional_trade_name__c,
                "can_be_registered__c": listing_session.can_be_registered__c,
                "waitlist_counter_new__c": listing_session.waitlist_counter_new__c,
                "support_coach_3_del__c": listing_session.support_coach_3_del__c,
                "support_coach_4_del__c": listing_session.support_coach_4_del__c,
                "support_coach_5_del__c": listing_session.support_coach_5_del__c,
                "support_coach_6_del__c": listing_session.support_coach_6_del__c,
                "parent_communication__c": listing_session.parent_communication__c,
                "x18_digit_id__c": listing_session.x18_digit_id__c,
                "waitlist_space_available__c": listing_session.waitlist_space_available__c,
                "waitlist_capacity__c": listing_session.waitlist_capacity__c,
                "event_hours__c": listing_session.event_hours__c,
                "session_end_date__c": listing_session.session_end_date__c,
                "session_end_time__c": listing_session.session_end_time__c,
                "session_start_date__c": listing_session.session_start_date__c,
                "session_start_time__c": listing_session.session_start_time__c,
                "age_eligibility_date__c": listing_session.age_eligibility_date__c,
                "allow_early_registration__c": listing_session.allow_early_registration__c,
                "direct_session_link__c": listing_session.direct_session_link__c,
                "private_event__c": listing_session.private_event__c,
                "parent_communication_french__c": listing_session.parent_communication_french__c,
                "parent_communication_spanish__c": listing_session.parent_communication_spanish__c,
                "program_type__c": listing_session.program_type__c,
                "lesson_plan__c": listing_session.lesson_plan__c
            }
            for listing_session in new_raw_listing_sessions
        ]


        # Execute the upsert query for all listing_sessions
        cursor.executemany(upsert_query, records)

        # Commit the transaction
        if commit_changes:
            db_connection.commit()

        # Close the cursor (but not the connection)
        cursor.close()
