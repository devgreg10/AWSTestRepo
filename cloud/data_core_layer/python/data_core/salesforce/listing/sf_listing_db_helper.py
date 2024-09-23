from data_core.salesforce.listing.sf_listing_db_models import SfListingRawDbModel, SfListingSourceModel, map_listing_sources_to_raws
from psycopg2.extensions import connection
from datetime import datetime

from typing import List

import logging

class SalesforceListingDbHelper:

    @staticmethod
    def insert_sf_raw_listings_from_source_listings(*,
                                                    db_connection: connection,
                                                    source_listings: List[SfListingSourceModel],
                                                    commit_changes: bool = True):
        
        SalesforceListingDbHelper.insert_sf_raw_listings(db_connection = db_connection,
                                                         new_raw_listings=map_listing_sources_to_raws(source_listings))

    @staticmethod
    def insert_sf_raw_listings(*, 
                              db_connection: connection, 
                              new_raw_listings: List[SfListingRawDbModel],
                              commit_changes: bool = True):
    
        """
        Inserts a collection of SfListingRawDbModel objects into PostgreSQL.
        If a primary key violation occurs, updates the conflicting record.
        
        Args:
        - listings: List of SfListingRawDbModel instances.
        - conn: A pre-existing psycopg2 connection object.
        """
        # Use the provided connection to create a cursor
        cursor = db_connection.cursor()

            # Define the upsert SQL statement
        upsert_query = """
        INSERT INTO ft_ds_raw.sf_listing (
            id, lastmodifieddate, isdeleted, createddate, name, recordtypeid, createdbyid, lastmodifiedbyid,
            systemmodstamp, lastactivitydate, lastvieweddate, lastreferenceddate, brief_description__c,
            full_description__c, membership_discount_active__c, military_discount_active__c, presented_by__c,
            primary_image__c, sibling_discount_active__c, priority__c, ownerid, account__c, class_id__c,
            class_status__c, end_date__c, event_id__c, event_status__c, external_id__c, hosted_by__c,
            is_public__c, listing_location_address__c, publish_end_date__c, publish_start_date__c,
            return_policy__c, start_date__c, total_coaches__c, test__c, count_listing__c, dss_ingestion_timestamp
        ) VALUES (
            %(id)s, %(lastmodifieddate)s, %(isdeleted)s, %(createddate)s, %(name)s, %(recordtypeid)s, %(createdbyid)s, 
            %(lastmodifiedbyid)s, %(systemmodstamp)s, %(lastactivitydate)s, %(lastvieweddate)s, %(lastreferenceddate)s, 
            %(brief_description__c)s, %(full_description__c)s, %(membership_discount_active__c)s, 
            %(military_discount_active__c)s, %(presented_by__c)s, %(primary_image__c)s, %(sibling_discount_active__c)s, 
            %(priority__c)s, %(ownerid)s, %(account__c)s, %(class_id__c)s, %(class_status__c)s, %(end_date__c)s, 
            %(event_id__c)s, %(event_status__c)s, %(external_id__c)s, %(hosted_by__c)s, %(is_public__c)s, 
            %(listing_location_address__c)s, %(publish_end_date__c)s, %(publish_start_date__c)s, %(return_policy__c)s, 
            %(start_date__c)s, %(total_coaches__c)s, %(test__c)s, %(count_listing__c)s, %(dss_ingestion_timestamp)s
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
            brief_description__c = EXCLUDED.brief_description__c,
            full_description__c = EXCLUDED.full_description__c,
            membership_discount_active__c = EXCLUDED.membership_discount_active__c,
            military_discount_active__c = EXCLUDED.military_discount_active__c,
            presented_by__c = EXCLUDED.presented_by__c,
            primary_image__c = EXCLUDED.primary_image__c,
            sibling_discount_active__c = EXCLUDED.sibling_discount_active__c,
            priority__c = EXCLUDED.priority__c,
            ownerid = EXCLUDED.ownerid,
            account__c = EXCLUDED.account__c,
            class_id__c = EXCLUDED.class_id__c,
            class_status__c = EXCLUDED.class_status__c,
            end_date__c = EXCLUDED.end_date__c,
            event_id__c = EXCLUDED.event_id__c,
            event_status__c = EXCLUDED.event_status__c,
            external_id__c = EXCLUDED.external_id__c,
            hosted_by__c = EXCLUDED.hosted_by__c,
            is_public__c = EXCLUDED.is_public__c,
            listing_location_address__c = EXCLUDED.listing_location_address__c,
            publish_end_date__c = EXCLUDED.publish_end_date__c,
            publish_start_date__c = EXCLUDED.publish_start_date__c,
            return_policy__c = EXCLUDED.return_policy__c,
            start_date__c = EXCLUDED.start_date__c,
            total_coaches__c = EXCLUDED.total_coaches__c,
            test__c = EXCLUDED.test__c,
            count_listing__c = EXCLUDED.count_listing__c,
            dss_ingestion_timestamp = EXCLUDED.dss_ingestion_timestamp;
        """

        # Prepare the data
        records = [
            {
                "id": listing.id,
                "lastmodifieddate": listing.lastmodifieddate,
                "isdeleted": listing.isdeleted,
                "createddate": listing.createddate,
                "name": listing.name,
                "recordtypeid": listing.recordtypeid,
                "createdbyid": listing.createdbyid,
                "lastmodifiedbyid": listing.lastmodifiedbyid,
                "systemmodstamp": listing.systemmodstamp,
                "lastactivitydate": listing.lastactivitydate,
                "lastvieweddate": listing.lastvieweddate,
                "lastreferenceddate": listing.lastreferenceddate,
                "brief_description__c": listing.brief_description__c,
                "full_description__c": listing.full_description__c,
                "membership_discount_active__c": listing.membership_discount_active__c,
                "military_discount_active__c": listing.military_discount_active__c,
                "presented_by__c": listing.presented_by__c,
                "primary_image__c": listing.primary_image__c,
                "sibling_discount_active__c": listing.sibling_discount_active__c,
                "priority__c": listing.priority__c,
                "ownerid": listing.ownerid,
                "account__c": listing.account__c,
                "class_id__c": listing.class_id__c,
                "class_status__c": listing.class_status__c,
                "end_date__c": listing.end_date__c,
                "event_id__c": listing.event_id__c,
                "event_status__c": listing.event_status__c,
                "external_id__c": listing.external_id__c,
                "hosted_by__c": listing.hosted_by__c,
                "is_public__c": listing.is_public__c,
                "listing_location_address__c": listing.listing_location_address__c,
                "publish_end_date__c": listing.publish_end_date__c,
                "publish_start_date__c": listing.publish_start_date__c,
                "return_policy__c": listing.return_policy__c,
                "start_date__c": listing.start_date__c,
                "total_coaches__c": listing.total_coaches__c,
                "test__c": listing.test__c,
                "count_listing__c": listing.count_listing__c,
                "dss_ingestion_timestamp": datetime.now()  # Capture current timestamp
            }
            for listing in new_raw_listings
        ]

        # Execute the upsert query for all listings
        cursor.executemany(upsert_query, records)

        # Commit the transaction
        if commit_changes:
            db_connection.commit()

        # Close the cursor (but not the connection)
        cursor.close()
