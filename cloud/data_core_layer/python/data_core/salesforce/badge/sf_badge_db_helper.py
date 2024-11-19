from data_core.salesforce.badge.sf_badge_db_models import SfBadgeRawDbModel, SfBadgeSourceModel, map_badge_sources_to_raws
from psycopg2.extensions import connection
from datetime import datetime

from typing import List

import logging

class SalesforceBadgeDbHelper:

    @staticmethod
    def insert_sf_raw_badge_from_source_badge(*,
                                                    db_connection: connection,
                                                    source_badge: List[SfBadgeSourceModel],
                                                    commit_changes: bool = True):
        
        SalesforceBadgeDbHelper.insert_sf_raw_badge(db_connection = db_connection,
                                                         new_raw_badge=map_badge_sources_to_raws(source_badge))

    @staticmethod
    def insert_sf_raw_badge(*, 
                              db_connection: connection, 
                              new_raw_badge: List[SfBadgeRawDbModel],
                              commit_changes: bool = True):
    
        """
        Inserts a collection of SfBadgeRawDbModel objects into PostgreSQL.
        If a primary key violation occurs, updates the conflicting record.
        
        Args:
        - badge: List of SfBadgeRawDbModel instances.
        - conn: A pre-existing psycopg2 connection object.
        """
        # Use the provided connection to create a cursor
        cursor = db_connection.cursor()

            # Define the upsert SQL statement
        upsert_query = """
        INSERT INTO ft_ds_raw.sf_badge (
            id, lastmodifieddate, isdeleted, createddate, createdbyid, name, 
            lastmodifiedbyid, systemmodstamp, lastvieweddate, lastreferenceddate, 
            description__c, category__c, ownerid, 
            badge_type__c, is_active__c, 
            points__c, sort_order__c, badge_id__c, age_group__c, dss_ingestion_timestamp)            
            VALUES (
            %(id)s, %(lastmodifieddate)s, %(isdeleted)s, %(createddate)s, %(createdbyid)s, %(name)s, 
            %(lastmodifiedbyid)s, %(systemmodstamp)s, %(lastvieweddate)s, %(lastreferenceddate)s,
            %(description__c)s, %(category__c)s, %(ownerid)s, 
            %(badge_type__c)s, %(is_active__c)s,
            %(points__c)s, %(sort_order__c)s, %(badge_id__c)s, %(age_group__c)s, %(dss_ingestion_timestamp)s
        )
        ON CONFLICT (id, systemmodstamp)
        DO UPDATE SET
            lastmodifieddate = EXCLUDED.lastmodifieddate,
            isdeleted = EXCLUDED.isdeleted,
            createddate = EXCLUDED.createddate,
            createdbyid = EXCLUDED.createdbyid,
            name = EXCLUDED.name,
            lastmodifiedbyid = EXCLUDED.lastmodifiedbyid,
            lastvieweddate = EXCLUDED.lastvieweddate,
            lastreferenceddate = EXCLUDED.lastreferenceddate,
            description__c = EXCLUDED.description__c,
            category__c = EXCLUDED.category__c,
            ownerid = EXCLUDED.ownerid,
            badge_type__c = EXCLUDED.badge_type__c,
            is_active__c = EXCLUDED.is_active__c,
            points__c = EXCLUDED.points__c,
            sort_order__c = EXCLUDED.sort_order__c,
            badge_id__c = EXCLUDED.badge_id__c,
            age_group__c = EXCLUDED.age_group__c,
            dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp;
        """

        # Prepare the data
        records = [
            {
                "id": badge.id,
                "lastmodifieddate": badge.lastmodifieddate,
                "isdeleted": badge.isdeleted,
                "createddate": badge.createddate,
                "createdbyid": badge.createdbyid,
                "name": badge.name,
                "lastmodifiedbyid": badge.lastmodifiedbyid,
                "systemmodstamp": badge.systemmodstamp,
                "lastvieweddate": badge.lastvieweddate,
                "lastreferenceddate": badge.lastreferenceddate,
                "description__c": badge.description__c,
                "category__c": badge.category__c,
                "ownerid": badge.ownerid,
                "badge_type__c": badge.badge_type__c,
                "is_active__c": badge.is_active__c,
                "points__c": badge.points__c,
                "sort_order__c": badge.sort_order__c,
                "badge_id__c": badge.badge_id__c,
                "age_group__c": badge.age_group__c,
                "dss_ingestion_timestamp": datetime.now()
            }
            for badge in new_raw_badge
        ]

        # Execute the upsert query for all badge
        cursor.executemany(upsert_query, records)

        # Commit the transaction
        if commit_changes:
            db_connection.commit()

        # Close the cursor (but not the connection)
        cursor.close()
