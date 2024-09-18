from data_core.salesforce.contact.sf_contact_db_models import SfContactRawDbModel, SfContactSourceModel, map_sf_contact_sources_to_raws
from psycopg2.extensions import connection
from datetime import datetime

from typing import List

import logging

class SalesforceContactDbHelper:

    @staticmethod
    def insert_sf_raw_contacts_from_source_contacts(*,
                                                    db_connection: connection,
                                                    source_contacts: List[SfContactSourceModel],
                                                    commit_changes: bool = True):
        
        SalesforceContactDbHelper.insert_sf_raw_contacts(db_connection = db_connection,
                                                         new_raw_contacts=map_sf_contact_sources_to_raws(source_contacts))

    @staticmethod
    def insert_sf_raw_contacts(*, 
                              db_connection: connection, 
                              new_raw_contacts: List[SfContactRawDbModel],
                              commit_changes: bool = True):
    
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
        upsert_query = """
            INSERT INTO ft_ds_raw.sf_contact (
            id, chapter_affiliation__c, chapterid_contact__c, casesafeid__c, contact_type__c, 
            age__c, ethnicity__c, gender__c, grade__c, participation_status__c, 
            mailingpostalcode, mailingstreet, mailingcity, mailingstate, school_name__c, 
            school_name_other__c, firstname, lastname, birthdate, accountid, 
            lastmodifieddate, isdeleted, createddate, systemmodstamp, dss_ingestion_timestamp
        ) VALUES (
            %(id)s, %(chapter_affiliation__c)s, %(chapterid_contact__c)s, %(casesafeid__c)s, %(contact_type__c)s, 
            %(age__c)s, %(ethnicity__c)s, %(gender__c)s, %(grade__c)s, %(participation_status__c)s, 
            %(mailingpostalcode)s, %(mailingstreet)s, %(mailingcity)s, %(mailingstate)s, %(school_name__c)s, 
            %(school_name_other__c)s, %(firstname)s, %(lastname)s, %(birthdate)s, %(accountid)s, 
            %(lastmodifieddate)s, %(isdeleted)s, %(createddate)s, %(systemmodstamp)s, %(dss_ingestion_timestamp)s
        )
        ON CONFLICT (id, systemmodstamp) 
        DO UPDATE SET
            chapter_affiliation__c = EXCLUDED.chapter_affiliation__c,
            chapterid_contact__c = EXCLUDED.chapterid_contact__c,
            casesafeid__c = EXCLUDED.casesafeid__c,
            contact_type__c = EXCLUDED.contact_type__c,
            age__c = EXCLUDED.age__c,
            ethnicity__c = EXCLUDED.ethnicity__c,
            gender__c = EXCLUDED.gender__c,
            grade__c = EXCLUDED.grade__c,
            participation_status__c = EXCLUDED.participation_status__c,
            mailingpostalcode = EXCLUDED.mailingpostalcode,
            mailingstreet = EXCLUDED.mailingstreet,
            mailingcity = EXCLUDED.mailingcity,
            mailingstate = EXCLUDED.mailingstate,
            school_name__c = EXCLUDED.school_name__c,
            school_name_other__c = EXCLUDED.school_name_other__c,
            firstname = EXCLUDED.firstname,
            lastname = EXCLUDED.lastname,
            birthdate = EXCLUDED.birthdate,
            accountid = EXCLUDED.accountid,
            lastmodifieddate = EXCLUDED.lastmodifieddate,
            isdeleted = EXCLUDED.isdeleted,
            createddate = EXCLUDED.createddate,
            systemmodstamp = EXCLUDED.systemmodstamp,
            dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp;
        """

        # Prepare the data
        records = [
            {
                "id": contact.id,  # Contact ID
                "chapter_affiliation__c": contact.chapter_affiliation__c,  # Chapter Affiliation
                "chapterid_contact__c": contact.chapterid_contact__c,  # Chapter ID for Contact
                "casesafeid__c": contact.casesafeid__c,  # Case Safe ID
                "contact_type__c": contact.contact_type__c,  # Contact Type
                "age__c": contact.age__c,  # Age
                "ethnicity__c": contact.ethnicity__c,  # Ethnicity
                "gender__c": contact.gender__c,  # Gender
                "grade__c": contact.grade__c,  # Grade
                "participation_status__c": contact.participation_status__c,  # Participation Status
                "mailingpostalcode": contact.mailingpostalcode,  # Postal Code
                "mailingstreet": contact.mailingstreet,  # Street Address
                "mailingcity": contact.mailingcity,  # City
                "mailingstate": contact.mailingstate,  # State
                "school_name__c": contact.school_name__c,  # School Name
                "school_name_other__c": contact.school_name_other__c,  # Other School Name
                "firstname": contact.firstname,  # First Name
                "lastname": contact.lastname,  # Last Name
                "birthdate": contact.birthdate,  # Birthdate
                "accountid": contact.accountid,  # Account ID
                "lastmodifieddate": contact.lastmodifieddate,  # Last Modified Date
                "isdeleted": contact.isdeleted,  # Is Deleted
                "createddate": contact.createddate,  # Created Date
                "systemmodstamp": contact.systemmodstamp,  # System Modstamp
                "dss_ingestion_timestamp": datetime.now()  # Ingestion Timestamp
            }
            for contact in new_raw_contacts
        ]

        # Execute the upsert query for all contacts
        cursor.executemany(upsert_query, records)

        # Commit the transaction
        if commit_changes:
            db_connection.commit()

        # Close the cursor (but not the connection)
        cursor.close()
