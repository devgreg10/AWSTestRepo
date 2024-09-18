from data_core.salesforce.session_registration.sf_session_registration_db_models import SfSessionRegistrationRawDbModel, SfSessionRegistrationSourceModel, map_sf_session_registration_sources_to_raws
from psycopg2.extensions import connection
from datetime import datetime

from typing import List

import logging

class SalesforceSessionRegistrationDbHelper:

    @staticmethod
    def insert_sf_raw_session_registrations_from_source_session_registrations(*,
                                                    db_connection: connection,
                                                    source_session_registrations: List[SfSessionRegistrationSourceModel],
                                                    commit_changes: bool = True):
        
        SalesforceSessionRegistrationDbHelper.insert_sf_raw_session_registrations(db_connection = db_connection,
                                                         new_raw_session_registrations=map_sf_session_registration_sources_to_raws(source_session_registrations))

    @staticmethod
    def insert_sf_raw_session_registrations(*, 
                              db_connection: connection, 
                              new_raw_session_registrations: List[SfSessionRegistrationRawDbModel],
                              commit_changes: bool = True):
    
        """
        Inserts a collection of SfSessionRegistrationRawDbModel objects into PostgreSQL.
        If a primary key violation occurs, updates the conflicting record.
        
        Args:
        - session_registrations: List of SfSessionRegistrationRawDbModel instances.
        - conn: A pre-existing psycopg2 connection object.
        """
        # Use the provided connection to create a cursor
        cursor = db_connection.cursor()

        # Define the upsert SQL statement
        upsert_query = """
        INSERT INTO sf_registration_session (
            id, lastmodifieddate, isdeleted, createddate, name, createdbyid, lastmodifiedbyid,
            systemmodstamp, lastactivitydate, lastvieweddate, lastreferenceddate, listing_session__c,
            contact__c, allergies__c, amount_paid__c, emergency_contact_email__c, emergency_contact_name__c,
            emergency_contact_number__c, financial_aid_applied__c, financial_aid__c, membership_registration__c,
            reevaluate_waitlist__c, reggie_registration_id__c, status__c, trigger_time__c, waitlist_position__c,
            waitlist_process__c, listing_record_type__c, delete_expired_registration__c, expiration_date__c,
            bypass_automation__c, count_session_registration__c, parent_contact__c, listing_session_record_type__c,
            sessiontype_replicated__c, charge_amount__c, cost_difference__c, discount__c, item_price__c,
            new_listing_session__c, new_session_cost__c, transferred__c, current_session_registration_number__c,
            dss_ingestion_timestamp
        ) VALUES (
            %(id)s, %(lastmodifieddate)s, %(isdeleted)s, %(createddate)s, %(name)s, %(createdbyid)s, %(lastmodifiedbyid)s,
            %(systemmodstamp)s, %(lastactivitydate)s, %(lastvieweddate)s, %(lastreferenceddate)s, %(listing_session__c)s,
            %(contact__c)s, %(allergies__c)s, %(amount_paid__c)s, %(emergency_contact_email__c)s, %(emergency_contact_name__c)s,
            %(emergency_contact_number__c)s, %(financial_aid_applied__c)s, %(financial_aid__c)s, %(membership_registration__c)s,
            %(reevaluate_waitlist__c)s, %(reggie_registration_id__c)s, %(status__c)s, %(trigger_time__c)s, %(waitlist_position__c)s,
            %(waitlist_process__c)s, %(listing_record_type__c)s, %(delete_expired_registration__c)s, %(expiration_date__c)s,
            %(bypass_automation__c)s, %(count_session_registration__c)s, %(parent_contact__c)s, %(listing_session_record_type__c)s,
            %(sessiontype_replicated__c)s, %(charge_amount__c)s, %(cost_difference__c)s, %(discount__c)s, %(item_price__c)s,
            %(new_listing_session__c)s, %(new_session_cost__c)s, %(transferred__c)s, %(current_session_registration_number__c)s,
            %(dss_ingestion_timestamp)s
        )
        ON CONFLICT (id, systemmodstamp) 
        DO UPDATE SET
            lastmodifieddate = EXCLUDED.lastmodifieddate,
            isdeleted = EXCLUDED.isdeleted,
            createddate = EXCLUDED.createddate,
            name = EXCLUDED.name,
            createdbyid = EXCLUDED.createdbyid,
            lastmodifiedbyid = EXCLUDED.lastmodifiedbyid,
            lastactivitydate = EXCLUDED.lastactivitydate,
            lastvieweddate = EXCLUDED.lastvieweddate,
            lastreferenceddate = EXCLUDED.lastreferenceddate,
            listing_session__c = EXCLUDED.listing_session__c,
            contact__c = EXCLUDED.contact__c,
            allergies__c = EXCLUDED.allergies__c,
            amount_paid__c = EXCLUDED.amount_paid__c,
            emergency_contact_email__c = EXCLUDED.emergency_contact_email__c,
            emergency_contact_name__c = EXCLUDED.emergency_contact_name__c,
            emergency_contact_number__c = EXCLUDED.emergency_contact_number__c,
            financial_aid_applied__c = EXCLUDED.financial_aid_applied__c,
            financial_aid__c = EXCLUDED.financial_aid__c,
            membership_registration__c = EXCLUDED.membership_registration__c,
            reevaluate_waitlist__c = EXCLUDED.reevaluate_waitlist__c,
            reggie_registration_id__c = EXCLUDED.reggie_registration_id__c,
            status__c = EXCLUDED.status__c,
            trigger_time__c = EXCLUDED.trigger_time__c,
            waitlist_position__c = EXCLUDED.waitlist_position__c,
            waitlist_process__c = EXCLUDED.waitlist_process__c,
            listing_record_type__c = EXCLUDED.listing_record_type__c,
            delete_expired_registration__c = EXCLUDED.delete_expired_registration__c,
            expiration_date__c = EXCLUDED.expiration_date__c,
            bypass_automation__c = EXCLUDED.bypass_automation__c,
            count_session_registration__c = EXCLUDED.count_session_registration__c,
            parent_contact__c = EXCLUDED.parent_contact__c,
            listing_session_record_type__c = EXCLUDED.listing_session_record_type__c,
            sessiontype_replicated__c = EXCLUDED.sessiontype_replicated__c,
            charge_amount__c = EXCLUDED.charge_amount__c,
            cost_difference__c = EXCLUDED.cost_difference__c,
            discount__c = EXCLUDED.discount__c,
            item_price__c = EXCLUDED.item_price__c,
            new_listing_session__c = EXCLUDED.new_listing_session__c,
            new_session_cost__c = EXCLUDED.new_session_cost__c,
            transferred__c = EXCLUDED.transferred__c,
            current_session_registration_number__c = EXCLUDED.current_session_registration_number__c,
            dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp;
        """

        # Prepare the data
        records = [
            {
                "id": session_registration.id,
                "lastmodifieddate": session_registration.lastmodifieddate,
                "isdeleted": session_registration.isdeleted,
                "createddate": session_registration.createddate,
                "name": session_registration.name,
                "createdbyid": session_registration.createdbyid,
                "lastmodifiedbyid": session_registration.lastmodifiedbyid,
                "systemmodstamp": session_registration.systemmodstamp,
                "lastactivitydate": session_registration.lastactivitydate,
                "lastvieweddate": session_registration.lastvieweddate,
                "lastreferenceddate": session_registration.lastreferenceddate,
                "listing_session__c": session_registration.listing_session__c,
                "contact__c": session_registration.contact__c,
                "allergies__c": session_registration.allergies__c,
                "amount_paid__c": session_registration.amount_paid__c,
                "emergency_contact_email__c": session_registration.emergency_contact_email__c,
                "emergency_contact_name__c": session_registration.emergency_contact_name__c,
                "emergency_contact_number__c": session_registration.emergency_contact_number__c,
                "financial_aid_applied__c": session_registration.financial_aid_applied__c,
                "financial_aid__c": session_registration.financial_aid__c,
                "membership_registration__c": session_registration.membership_registration__c,
                "reevaluate_waitlist__c": session_registration.reevaluate_waitlist__c,
                "reggie_registration_id__c": session_registration.reggie_registration_id__c,
                "status__c": session_registration.status__c,
                "trigger_time__c": session_registration.trigger_time__c,
                "waitlist_position__c": session_registration.waitlist_position__c,
                "waitlist_process__c": session_registration.waitlist_process__c,
                "listing_record_type__c": session_registration.listing_record_type__c,
                "delete_expired_registration__c": session_registration.delete_expired_registration__c,
                "expiration_date__c": session_registration.expiration_date__c,
                "bypass_automation__c": session_registration.bypass_automation__c,
                "count_session_registration__c": session_registration.count_session_registration__c,
                "parent_contact__c": session_registration.parent_contact__c,
                "listing_session_record_type__c": session_registration.listing_session_record_type__c,
                "sessiontype_replicated__c": session_registration.sessiontype_replicated__c,
                "charge_amount__c": session_registration.charge_amount__c,
                "cost_difference__c": session_registration.cost_difference__c,
                "discount__c": session_registration.discount__c,
                "item_price__c": session_registration.item_price__c,
                "new_listing_session__c": session_registration.new_listing_session__c,
                "new_session_cost__c": session_registration.new_session_cost__c,
                "transferred__c": session_registration.transferred__c,
                "current_session_registration_number__c": session_registration.current_session_registration_number__c,
                "dss_ingestion_timestamp": datetime.now()  # Capture current timestamp
            }
            for session_registration in new_raw_session_registrations
        ]

        # Execute the upsert query for all session_registrations
        cursor.executemany(upsert_query, records)

        # Commit the transaction
        if commit_changes:
            db_connection.commit()

        # Close the cursor (but not the connection)
        cursor.close()
