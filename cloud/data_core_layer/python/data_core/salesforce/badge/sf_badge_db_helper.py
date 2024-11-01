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
            "id", "lastmodifieddate", "isdeleted", "createddate", "name", "recordtypeid", "createdbyid", 
            "lastmodifiedbyid", "systemmodstamp", "lastvieweddate", "lastreferenceddate", 
            "description__c", "category__c", "ownerid", "dss_ingestion_timestamp",
            "badge_type__c", "coaches_app_image_id__c", "coaches_app_image_url__c", "is_active__c", 
            "parent_registration_image_id__c", "parent_registration_image_url__c", 
            "points__c", "sort_order__c", "badge_id__c", "age_group__c", "dss_ingestion_timestamp")            
            VALUES (
            %(id)s, %(lastmodifieddate)s, %(isdeleted)s, %(createddate)s, %(name)s, %(recordtypeid)s, %(createdbyid)s,
            %(lastmodifiedbyid)s, %(systemmodstamp)s, %(lastvieweddate)s, %(lastreferenceddate)s,
            %(description__c)s, %(category__c)s, %(ownerid__c)s, %(dss_ingestion_timestamp)s,
            %(Badge_Type__c)s, %(Coaches_App_Image_Id__c)s, %(Coaches_App_Image_URL__c)s, %(Is_Active__c)s,
            %(Parent_Registration_Image_Id__c)s, %(parent_registration_image_url__c)s,
            %(Points__c)s, %(Sort_Order__c)s, %(badge_id__c)s, %(age_group__c)s, %(dss_ingestion_timestamp)s
        )
        ON CONFLICT (id, systemmodstamp)
        DO UPDATE SET
            lastmodifieddate = EXCLUDED.lastmodifieddate,
            isdeleted = EXCLUDED.isdeleted,
            createddate = EXCLUDED.createddate,
            name = EXCLUDED.name,
            createdbyid = EXCLUDED.createdbyid,
            lastmodifiedbyid = EXCLUDED.lastmodifiedbyid,
            systemmodstamp = EXCLUDED.systemmodstamp,
            lastvieweddate = EXCLUDED.lastvieweddate,
            lastreferenceddate = EXCLUDED.lastreferenceddate,
            description__c = EXCLUDED.description__c,
            category__c = EXCLUDED.category__c,
            ownerid = EXCLUDED.ownerid,
            Badge_Type__c = EXCLUDED.Badge_Type__c,
            Coaches_App_Image_Id__c = EXCLUDED.Coaches_App_Image_Id__c,
            Coaches_App_Image_URL__c = EXCLUDED.Coaches_App_Image_URL__c,
            Is_Active__c = EXCLUDED.Is_Active__c,
            Parent_Registration_Image_Id__c = EXCLUDED.Parent_Registration_Image_Id__c,
            Parent_Registration_Image_URL__c = EXCLUDED.Parent_Registration_Image_URL__c,
            Points__c = EXCLUDED.Points__c,
            Sort_Order__c = EXCLUDED.Sort_Order__c,
            badge_id__c = EXCLUDED.badge_id__c,
            Age_Group__c = EXCLUDED.Age_Group__c,
            dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp;
        """

        # Prepare the data
        records = [
            {
                "id": badge.id,
                "lastmodifieddate": badge.lastmodifieddate,
                "isdeleted": badge.isdeleted,
                "createddate": badge.createddate,
                "name": badge.name,
                "createdbyid": badge.createdbyid,
                "lastmodifiedbyid": badge.lastmodifiedbyid,
                "systemmodstamp": badge.systemmodstamp,
                "lastvieweddate": badge.lastvieweddate,
                "lastreferenceddate": badge.lastreferenceddate,
                "description__c": badge.description,
                "category__c": badge.category__c,
                "ownerid": badge.ownerid,
                "Badge_Type__c": badge.badgetype__c,
                "Coaches_App_Image_Id__c": badge.coachesappimageid__c,
                "Coaches_App_Image_URL__c": badge.coachesappimageurl__c,
                "Is_Active__c": badge.isactive__c,
                "Parent_Registration_Image_Id__c": badge.parentregistrationimageid__c,
                "Parent_Registration_Image_URL__c": badge.parentregistrationimageurl__c,
                "Points__c": badge.points__c,
                "Sort_Order__c": badge.sortorder__c,
                "badge_id__c": badge.badgeid__c,
                "Age_Group__c": badge.age_group__c,
                "dss_ingestion_timestamp": datetime.now()             }
            for badge in new_raw_badge
        ]

        # Execute the upsert query for all badge
        cursor.executemany(upsert_query, records)

        # Commit the transaction
        if commit_changes:
            db_connection.commit()

        # Close the cursor (but not the connection)
        cursor.close()
