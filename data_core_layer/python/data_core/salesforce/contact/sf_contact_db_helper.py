from data_core.salesforce.contact.sf_contact_db_models import SfContactRawDbModel, SfContactSourceModel, map_sources_to_raws
from data_core.util.db_execute_helper import DbExecutorHelper
from psycopg2.extensions import connection
from datetime import datetime

import psycopg2
from typing import List

import logging

class SalesforceContactDbHelper:

    @staticmethod
    def insert_sf_raw_contacts_from_source_contacts(*,
                                                    db_connection: connection,
                                                    source_contacts: List[SfContactSourceModel],
                                                    commit_changes: bool = True):
        
        SalesforceContactDbHelper.insert_sf_raw_contacts(db_connection = db_connection,
                                                         new_raw_contacts=map_sources_to_raws(source_contacts))

    @staticmethod
    def insert_sf_raw_contacts(*, 
                              db_connection: connection, 
                              new_raw_contacts: List[SfContactRawDbModel],
                              commit_changes: bool = True, 
                              close_db_conn: bool = True):
    
        """
        Inserts a collection of SfContactRawDbModel objects into PostgreSQL.
        If a primary key violation occurs, updates the conflicting record.
        
        Args:
        - contacts: List of SfContactRawDbModel instances.
        - conn: A pre-existing psycopg2 connection object.
        """
        # Use the provided connection to create a cursor
        cursor = db_connection.cursor()

        # Define the upsert SQL statement
        upsert_query = f"""
        INSERT INTO ft_ds_raw.sf_contact_dk_test (
            id, mailingpostalcode, chapter_affiliation__c, chapterid_contact__c, 
            casesafeid__c, contact_type__c, age__c, ethnicity__c, gender__c, grade__c, 
            participation_status__c, isdeleted, lastmodifieddate, createddate, dss_last_modified_timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,{datetime.now()})
        ON CONFLICT (id) 
        DO UPDATE SET 
            mailingpostalcode = EXCLUDED.mailingpostalcode,
            chapter_affiliation__c = EXCLUDED.chapter_affiliation__c,
            chapterid_contact__c = EXCLUDED.chapterid_contact__c,
            casesafeid__c = EXCLUDED.casesafeid__c,
            contact_type__c = EXCLUDED.contact_type__c,
            age__c = EXCLUDED.age__c,
            ethnicity__c = EXCLUDED.ethnicity__c,
            gender__c = EXCLUDED.gender__c,
            grade__c = EXCLUDED.grade__c,
            participation_status__c = EXCLUDED.participation_status__c,
            isdeleted = EXCLUDED.isdeleted,
            lastmodifieddate = EXCLUDED.lastmodifieddate,
            createddate = EXCLUDED.createddate,
            dss_last_modified_timestamp = EXCLUDED.dss_last_modified_timestamp
        """

        # Prepare the data
        records = [
            (
                contact.id, contact.mailingpostalcode, contact.chapter_affiliation__c, 
                contact.chapterid_contact__c, contact.casesafeid__c, contact.contact_type__c, 
                contact.age__c, contact.ethnicity__c, contact.gender__c, contact.grade__c, 
                contact.participation_status__c, contact.isdeleted, contact.lastmodifieddate, 
                contact.createddate, contact.dss_last_modified_timestamp
            )
            for contact in new_raw_contacts
        ]

        # Execute the upsert query for all contacts
        cursor.executemany(upsert_query, records)

        # Commit the transaction
        if commit_changes:
            db_connection.commit()

        # Close the cursor (but not the connection)
        cursor.close()
