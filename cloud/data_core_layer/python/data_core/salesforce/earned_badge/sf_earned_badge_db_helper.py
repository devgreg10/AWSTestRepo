from data_core.salesforce.earned_badge.sf_earned_badge_db_models import SfEarnedBadgeRawDbModel, SfEarnedBadgeSourceModel, map_earned_badge_sources_to_raws
from psycopg2.extensions import connection
from datetime import datetime

from typing import List

import logging

class SalesforceEarnedBadgeDbHelper:
    @staticmethod
    def insert_sf_raw_earned_badge_from_source_earned_badges(*,
                                                    db_connection: connection,
                                                    source_badges: List[SfEarnedBadgeSourceModel],
                                                    commit_changes: bool = True):
        
        SalesforceEarnedBadgeDbHelper.insert_sf_raw_earned_badges(db_connection = db_connection,
                                                                  new_raw_badges=map_earned_badge_sources_to_raws(source_badges))

    @staticmethod
    def insert_sf_raw_earned_badges(*, 
                              db_connection: connection, 
                              new_raw_badges: List[SfEarnedBadgeRawDbModel],
                              commit_changes: bool = True):
    
        """
        Inserts a collection of SfEarnedBadgeRawDbModel objects into PostgreSQL.
        If a primary key violation occurs, updates the conflicting record.
        
        Args:
        - earned_badge: List of SfEarnedBadgeRawDbModel instances.
        - conn: A pre-existing psycopg2 connection object.
        """
        # Use the provided connection to create a cursor
        cursor = db_connection.cursor()

            # Define the upsert SQL statement
        upsert_query = """
            INSERT INTO ft_ds_raw.sf_earned_badge (
                id, isdeleted, name, createddate, createdbyid, lastmodifieddate, lastmodifiedbyid,
                systemmodstamp, contact__c, badge__c, badge_type__c, category__c, class__c, id__c, date_earned__c, listing_session__c, listing_session_old__c,
                pending_aws_callout__c, points__c, source_system__c, dss_ingestion_timestamp)
            VALUES (
            %(id)s, %(isdeleted)s, %(name)s, %(createddate)s, %(createdbyid)s, %(lastmodifieddate)s, %(lastmodifiedbyid)s,
            %(systemmodstamp)s, %(contact__c)s, %(badge__c)s, %(badge_type__c)s, %(category__c)s, %(class__c)s, %(id__c)s, %(date_earned__c)s, %(listing_session__c)s, %(listing_session_old__c)s,
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
            dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp
            badge__c = EXCLUDED.badge__c,
            badge_type__c = EXCLUDED.badge_type__c,
            category__c = EXCLUDED.category__c,
            class__c = EXCLUDED.class__c,
            id__c = EXCLUDED.id__c,
            date_earned__c = EXCLUDED.date_earned__c,
            listing_session__c = EXCLUDED.listing_session__c,
            listing_session_old__c = EXCLUDED.listing_session_old__c,
            pending_aws_callout__c = EXCLUDED.pending_aws_callout__c,
            points__c = EXCLUDED.points__c,
            source_system__c = EXCLUDED.source_system__c,
            dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp
            ;       
            """

        # Prepare the data
        records = [
            {
                "id": earned_badge.id,
                "isdeleted": earned_badge.isdeleted,
                "name": earned_badge.name,
                "createddate": earned_badge.createddate,
                "createdbyid": earned_badge.createdbyid,
                "lastmodifieddate": earned_badge.lastmodifieddate,
                "lastmodifiedbyid": earned_badge.lastmodifiedbyid,
                "systemmodstamp": earned_badge.systemmodstamp,
                "contact__c": earned_badge.contact__c,
                "badge__c": earned_badge.badge__c,
                "badge_type__c": earned_badge.badge_type__c,
                "category__c": earned_badge.category__c,
                "class__c": earned_badge.class__c,
                "id__c": earned_badge.id__c,
                "date_earned__c": earned_badge.date_earned__c,
                "listing_session__c": earned_badge.listing_session__c,
                "listing_session_old__c": earned_badge.listing_session_old__c,
                "pending_aws_callout__c": earned_badge.pending_aws_callout__c,
                "points__c": earned_badge.points__c,
                "source_system__c": earned_badge.source_system__c,
                "dss_ingestion_timestamp": datetime.now()  
            }         
                
                for earned_badge in new_raw_badges
        ]

        # Execute the upsert query for all earned_badge
        cursor.executemany(upsert_query, records)

        # Commit the transaction
        if commit_changes:
            db_connection.commit()

        # Close the cursor (but not the connection)
        cursor.close()
