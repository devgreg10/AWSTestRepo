from datetime import datetime
from attrs import define
from data_core.util.db_model import DbModel
from typing import List

@define(kw_only=True)
class SfListingSourceModel():
    """
    Base model for Salesforce Listing from S3
    """
    Id: str
    LastModifiedDate: str
    IsDeleted: str
    CreatedDate: str
    Name: str
    RecordTypeId: str
    CreatedById: str
    LastModifiedById: str
    SystemModstamp: str
    LastActivityDate: str
    LastViewedDate: str
    LastReferencedDate: str
    Brief_Description__c: str
    Full_Description__c: str
    Membership_Discount_Active__c: str
    Military_Discount_Active__c: str
    Presented_By__c: str
    Primary_Image__c: str
    Sibling_Discount_Active__c: str
    Priority__c: str
    OwnerId: str
    Account__c: str
    Class_ID__c: str
    Class_Status__c: str
    End_Date__c: str
    Event_ID__c: str
    Event_Status__c: str
    External_ID__c: str
    Hosted_By__c: str
    Is_Public__c: str
    Listing_Location_Address__c: str
    Publish_End_Date__c: str
    Publish_Start_Date__c: str
    Return_Policy__c: str
    Start_Date__c: str
    Total_Coaches__c: str
    test__c: str
    Count_Listing__c: str

@define(kw_only=True)
class SfListingRawDbModel(DbModel):
    """
    Base model for Raw Salesforce listing
    """
    id: str
    lastmodifieddate: str
    isdeleted: str
    createddate: str
    name: str
    recordtypeid: str
    createdbyid: str
    lastmodifiedbyid: str
    systemmodstamp: str
    lastactivitydate: str
    lastvieweddate: str
    lastreferenceddate: str
    brief_description__c: str
    full_description__c: str
    membership_discount_active__c: str
    military_discount_active__c: str
    presented_by__c: str
    primary_image__c: str
    sibling_discount_active__c: str
    priority__c: str
    ownerid: str
    account__c: str
    class_id__c: str
    class_status__c: str
    end_date__c: str
    event_id__c: str
    event_status__c: str
    external_id__c: str
    hosted_by__c: str
    is_public__c: str
    listing_location_address__c: str
    publish_end_date__c: str
    publish_start_date__c: str
    return_policy__c: str
    start_date__c: str
    total_coaches__c: str
    test__c: str
    count_listing__c: str
    dss_ingestion_timestamp: datetime

    # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)



def map_listing_source_to_raw(source: SfListingSourceModel) -> SfListingRawDbModel:
    """
    Maps an instance of SfListingSourceModel to SfListingRawDbModel.
    """
    return SfListingRawDbModel(
        id=source.Id,
        lastmodifieddate=source.LastModifiedDate,
        isdeleted=source.IsDeleted,
        createddate=source.CreatedDate,
        name=source.Name,
        recordtypeid=source.RecordTypeId,
        createdbyid=source.CreatedById,
        lastmodifiedbyid=source.LastModifiedById,
        systemmodstamp=source.SystemModstamp,
        lastactivitydate=source.LastActivityDate,
        lastvieweddate=source.LastViewedDate,
        lastreferenceddate=source.LastReferencedDate,
        brief_description__c=source.Brief_Description__c,
        full_description__c=source.Full_Description__c,
        membership_discount_active__c=source.Membership_Discount_Active__c,
        military_discount_active__c=source.Military_Discount_Active__c,
        presented_by__c=source.Presented_By__c,
        primary_image__c=source.Primary_Image__c,
        sibling_discount_active__c=source.Sibling_Discount_Active__c,
        priority__c=source.Priority__c,
        ownerid=source.OwnerId,
        account__c=source.Account__c,
        class_id__c=source.Class_ID__c,
        class_status__c=source.Class_Status__c,
        end_date__c=source.End_Date__c,
        event_id__c=source.Event_ID__c,
        event_status__c=source.Event_Status__c,
        external_id__c=source.External_ID__c,
        hosted_by__c=source.Hosted_By__c,
        is_public__c=source.Is_Public__c,
        listing_location_address__c=source.Listing_Location_Address__c,
        publish_end_date__c=source.Publish_End_Date__c,
        publish_start_date__c=source.Publish_Start_Date__c,
        return_policy__c=source.Return_Policy__c,
        start_date__c=source.Start_Date__c,
        total_coaches__c=source.Total_Coaches__c,
        test__c=source.test__c,
        count_listing__c=source.Count_Listing__c,
        dss_ingestion_timestamp=datetime.now()  # Capture current timestamp
    )

def map_listing_sources_to_raws(source_list: List[SfListingSourceModel]) -> List[SfListingRawDbModel]:
    """
    Maps a list of SfListingSourceModel objects to a list of SfListingRawDbModel objects.

    Args:
    - source_list: List of SfListingSourceModel instances.

    Returns:
    - List of SfListingRawDbModel instances.
    """
    return [map_listing_source_to_raw(source) for source in source_list]