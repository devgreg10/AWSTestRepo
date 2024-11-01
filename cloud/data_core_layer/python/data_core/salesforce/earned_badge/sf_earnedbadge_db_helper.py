from data_core.salesforce.earned_badge.sf_earnedbadge_db_models import SfEarnedBadgeRawDbModel, SfEarnedBadgeSourceModel, map_earnedbadge_sources_to_raws
from psycopg2.extensions import connection
from datetime import datetime

from typing import List

import logging

class SalesforceEarnedBadgeDbHelper:

    @staticmethod
    def insert_sf_raw_badge_from_source_badge(*,
                                                    db_connection: connection,
                                                    source_badge: List[SfEarnedBadgeSourceModel],
                                                    commit_changes: bool = True):
        
        SalesforceEarnedBadgeDbHelper.insert_sf_raw_badge(db_connection = db_connection,
                                                         new_raw_badge=map_earnedbadge_sources_to_raws(source_badge))

    @staticmethod
    def insert_sf_raw_badge(*, 
                              db_connection: connection, 
                              new_raw_badge: List[SfEarnedBadgeRawDbModel],
                              commit_changes: bool = True):
    
        """
        Inserts a collection of SfEarnedBadgeRawDbModel objects into PostgreSQL.
        If a primary key violation occurs, updates the conflicting record.
        
        Args:
        - earnedbadge: List of SfEarnedBadgeRawDbModel instances.
        - conn: A pre-existing psycopg2 connection object.
        """
        # Use the provided connection to create a cursor
        cursor = db_connection.cursor()

            # Define the upsert SQL statement
        upsert_query = """
        INSERT INTO ft_ds_raw.sf_earnedbadge (
           "Id", "IsDeleted", "Name", "CreatedDate", "CreatedById", "LastModifiedDate", "LastModifiedById",
            "SystemModstamp", "Contact__c", "Badge__c", "Id__c", "Date_Earned__c", "Listing_Session__c",
            "Pending_AWS_Callout__c", "Points__c", "Source_System__c", "dss_ingestion_timestamp"                       
            VALUES (
            %(id)s, %(isdeleted)s, %(name)s, %(createddate)s, %(createdbyid)s, %(lastmodifieddate)s, %(lastmodifiedbyid)s,
            %(systemmodstamp)s, %(contact__c)s, %(badge__c)s, %(id__c)s, %(date_earned__c)s, %(listing_session__c)s,
            %(pending_aws_callout__c)s, %(points__c)s, %(source_system__c)s, %(dss_ingestion_timestamp)s      
            )
        ON CONFLICT (id, systemmodstamp)
        DO UPDATE SET
            lastmodifieddate = EXCLUDED.lastmodifieddate,
            isdeleted = EXCLUDED.isdeleted,
            name = EXCLUDED.name, 
            createddate = EXCLUDED.createddate,
            createdbyid = EXCLUDED.createdbyid,
            lastmodifiedbyid = EXCLUDED.lastmodifiedbyid,
            systemmodstamp = EXCLUDED.systemmodstamp,
            contact__c = EXCLUDED.contact__c, 
            dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp;       
            """

        # Prepare the data
        records = [
            {
                "id": earnedbadge.id,
                "isdeleted": earnedbadge.isdeleted,
                "name": earnedbadge.name,
                "createddate": earnedbadge.createddate,
                "createdbyid": earnedbadge.createdbyid,
                "lastmodifieddate": earnedbadge.lastmodifieddate,
                "lastmodifiedbyid": earnedbadge.lastmodifiedbyid,
                "systemmodstamp": earnedbadge.systemmodstamp,
                "contact__c": earnedbadge.contact__c,
                "badge__c": earnedbadge.badge__c,
                "id__c": earnedbadge.id__c,
                "date_earned__c": earnedbadge.date_earned__c,
                "listing_session__c": earnedbadge.listing_session__c,
                "pending_aws_callout__c": earnedbadge.pending_aws_callout__c,
                "points__c": earnedbadge.points__c,
                "source_system__c": earnedbadge.source_system__c,
                "dss_ingestion_timestamp": datetime.now()  
            }         
                
                for earnedbadge in new_raw_badge
        ]

        # Execute the upsert query for all earnedbadge
        cursor.executemany(upsert_query, records)

        # Commit the transaction
        if commit_changes:
            db_connection.commit()

        # Close the cursor (but not the connection)
        cursor.close()
