from data_core.salesforce.waitlist.sf_waitlist_db_models import SfWaitlistRawDbModel, SfWaitlistSourceModel, map_waitlist_sources_to_raws
from psycopg2.extensions import connection
from datetime import datetime

from typing import List

import logging

class SalesforceWaitlistDbHelper:

    @staticmethod
    def insert_sf_raw_waitlists_from_source_waitlists(*,
                                                    db_connection: connection,
                                                    source_waitlists: List[SfWaitlistSourceModel],
                                                    commit_changes: bool = True):
        
        SalesforceWaitlistDbHelper.insert_sf_raw_waitlists(db_connection = db_connection,
                                                         new_raw_waitlists=map_waitlist_sources_to_raws(source_waitlists))
                                                         

    @staticmethod
    def insert_sf_raw_waitlists(*, 
                              db_connection: connection, 
                              new_raw_waitlists: List[SfWaitlistRawDbModel],
                              commit_changes: bool = True):
    
        """
        Inserts a collection of SfWaitlistRawDbModel objects into PostgreSQL.
        If a primary key violation occurs, updates the conflicting record.
        
        Args:
        - new_raw_waitlists: List of SfWaitlistRawDbModel instances.
        - db_connection: A pre-existing psycopg2 connection object.
        - commit_changes: Whether to commit the changes to the database.
        """
        # Use the provided connection to create a cursor
        cursor = db_connection.cursor()

        # Define the upsert SQL statement
        upsert_query = f"""
        INSERT INTO ft_ds_raw.sf_waitlist (
            id, isdeleted, name, createddate, createdbyid, lastmodifieddate, lastmodifiedbyid,
            systemmodstamp, lastactivitydate, lastvieweddate, lastreferenceddate, listing_session__c,
            chapterid__c, chapter_name__c, contact__c, listing_session_location_address__c,
            listing_session_location_name__c, listing_session_name__c, membership_end_date__c,
            membership_price__c, membership_required__c, membership_start_date__c, parent_contact__c,
            status__c, waitlist_participant_order__c, created_by_email__c, waitlist_unique_key__c,
            status_is_in_process_or_selected__c, dss_ingestion_timestamp
        ) 
        VALUES (
            %(id)s, %(isdeleted)s, %(name)s, %(createddate)s, %(createdbyid)s, %(lastmodifieddate)s,
            %(lastmodifiedbyid)s, %(systemmodstamp)s, %(lastactivitydate)s, %(lastvieweddate)s,
            %(lastreferenceddate)s, %(listing_session__c)s, %(chapterid__c)s, %(chapter_name__c)s,
            %(contact__c)s, %(listing_session_location_address__c)s, %(listing_session_location_name__c)s,
            %(listing_session_name__c)s, %(membership_end_date__c)s, %(membership_price__c)s,
            %(membership_required__c)s, %(membership_start_date__c)s, %(parent_contact__c)s, %(status__c)s,
            %(waitlist_participant_order__c)s, %(created_by_email__c)s, %(waitlist_unique_key__c)s,
            %(status_is_in_process_or_selected__c)s, %(dss_ingestion_timestamp)s
        )
        ON CONFLICT (id, systemmodstamp) 
        DO UPDATE SET
            isdeleted = EXCLUDED.isdeleted,
            name = EXCLUDED.name,
            createddate = EXCLUDED.createddate,
            createdbyid = EXCLUDED.createdbyid,
            lastmodifieddate = EXCLUDED.lastmodifieddate,
            lastmodifiedbyid = EXCLUDED.lastmodifiedbyid,
            lastactivitydate = EXCLUDED.lastactivitydate,
            lastvieweddate = EXCLUDED.lastvieweddate,
            lastreferenceddate = EXCLUDED.lastreferenceddate,
            listing_session__c = EXCLUDED.listing_session__c,
            chapterid__c = EXCLUDED.chapterid__c,
            chapter_name__c = EXCLUDED.chapter_name__c,
            contact__c = EXCLUDED.contact__c,
            listing_session_location_address__c = EXCLUDED.listing_session_location_address__c,
            listing_session_location_name__c = EXCLUDED.listing_session_location_name__c,
            listing_session_name__c = EXCLUDED.listing_session_name__c,
            membership_end_date__c = EXCLUDED.membership_end_date__c,
            membership_price__c = EXCLUDED.membership_price__c,
            membership_required__c = EXCLUDED.membership_required__c,
            membership_start_date__c = EXCLUDED.membership_start_date__c,
            parent_contact__c = EXCLUDED.parent_contact__c,
            status__c = EXCLUDED.status__c,
            waitlist_participant_order__c = EXCLUDED.waitlist_participant_order__c,
            created_by_email__c = EXCLUDED.created_by_email__c,
            waitlist_unique_key__c = EXCLUDED.waitlist_unique_key__c,
            status_is_in_process_or_selected__c = EXCLUDED.status_is_in_process_or_selected__c,
            dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp;
        """

        # Prepare the data
        records = [
            {
                "id": waitlist.id,
                "isdeleted": waitlist.isdeleted,
                "name": waitlist.name,
                "createddate": waitlist.createddate,
                "createdbyid": waitlist.createdbyid,
                "lastmodifieddate": waitlist.lastmodifieddate,
                "lastmodifiedbyid": waitlist.lastmodifiedbyid,
                "systemmodstamp": waitlist.systemmodstamp,
                "lastactivitydate": waitlist.lastactivitydate,
                "lastvieweddate": waitlist.lastvieweddate,
                "lastreferenceddate": waitlist.lastreferenceddate,
                "listing_session__c": waitlist.listing_session__c,
                "chapterid__c": waitlist.chapterid__c,
                "chapter_name__c": waitlist.chapter_name__c,
                "contact__c": waitlist.contact__c,
                "listing_session_location_address__c": waitlist.listing_session_location_address__c,
                "listing_session_location_name__c": waitlist.listing_session_location_name__c,
                "listing_session_name__c": waitlist.listing_session_name__c,
                "membership_end_date__c": waitlist.membership_end_date__c,
                "membership_price__c": waitlist.membership_price__c,
                "membership_required__c": waitlist.membership_required__c,
                "membership_start_date__c": waitlist.membership_start_date__c,
                "parent_contact__c": waitlist.parent_contact__c,
                "status__c": waitlist.status__c,
                "waitlist_participant_order__c": waitlist.waitlist_participant_order__c,
                "created_by_email__c": waitlist.created_by_email__c,
                "waitlist_unique_key__c": waitlist.waitlist_unique_key__c,
                "status_is_in_process_or_selected__c": waitlist.status_is_in_process_or_selected__c,
                "dss_ingestion_timestamp": datetime.now()
            }
            for waitlist in new_raw_waitlists
        ]

        # Execute the upsert query for all waitlists
        cursor.executemany(upsert_query, records)

        # Commit the transaction if requested
        if commit_changes:
            db_connection.commit()

        # Close the cursor (but not the connection)
        cursor.close()