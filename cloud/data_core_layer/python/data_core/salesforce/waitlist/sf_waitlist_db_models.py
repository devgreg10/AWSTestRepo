from datetime import datetime
from attrs import define
from data_core.util.db_model import DbModel
from typing import List

@define(kw_only=True)
class SfWaitlistSourceModel():
    """
    Base model for Salesforce Waitlist from S3
    """
    Id: str
    IsDeleted: str
    Name: str
    CreatedDate: str
    CreatedById: str
    LastModifiedDate: str
    LastModifiedById: str
    SystemModstamp: str
    LastActivityDate: str
    LastViewedDate: str
    LastReferencedDate: str
    Listing_Session__c: str
    ChapterId__c: str
    Chapter_Name__c: str
    Contact__c: str
    Listing_Session_Location_Address__c: str
    Listing_Session_Location_Name__c: str
    Listing_Session_Name__c: str
    Membership_End_Date__c: str
    Membership_Price__c: str
    Membership_Required__c: str
    Membership_Start_Date__c: str
    Parent_Contact__c: str
    Status__c: str
    Waitlist_Participant_order__c: str
    Created_By_Email__c: str
    Waitlist_Unique_Key__c: str
    Status_Is_In_Process_or_Selected__c: str

@define(kw_only=True)
class SfWaitlistRawDbModel(DbModel):
    """
    Base model for Raw Salesforce Waitlist
    """
    id: str
    isdeleted: str
    name: str
    createddate: str
    createdbyid: str
    lastmodifieddate: str
    lastmodifiedbyid: str
    systemmodstamp: str
    lastactivitydate: str
    lastvieweddate: str
    lastreferenceddate: str
    listing_session__c: str
    chapterid__c: str
    chapter_name__c: str
    contact__c: str
    listing_session_location_address__c: str
    listing_session_location_name__c: str
    listing_session_name__c: str
    membership_end_date__c: str
    membership_price__c: str
    membership_required__c: str
    membership_start_date__c: str
    parent_contact__c: str
    status__c: str
    waitlist_participant_order__c: str
    created_by_email__c: str
    waitlist_unique_key__c: str
    status_is_in_process_or_selected__c: str
    dss_ingestion_timestamp: datetime

    # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)


def map_waitlist_source_to_raw(source: SfWaitlistSourceModel) -> SfWaitlistRawDbModel:
    """
    Maps an instance of SfWaitlistSourceModel to SfWaitlistRawDbModel.
    """
    return SfWaitlistRawDbModel(
        id=source.Id,
        isdeleted=source.IsDeleted,
        name=source.Name,
        createddate=source.CreatedDate,
        createdbyid=source.CreatedById,
        lastmodifieddate=source.LastModifiedDate,
        lastmodifiedbyid=source.LastModifiedById,
        systemmodstamp=source.SystemModstamp,
        lastactivitydate=source.LastActivityDate,
        lastvieweddate=source.LastViewedDate,
        lastreferenceddate=source.LastReferencedDate,
        listing_session__c=source.Listing_Session__c,
        chapterid__c=source.ChapterId__c,
        chapter_name__c=source.Chapter_Name__c,
        contact__c=source.Contact__c,
        listing_session_location_address__c=source.Listing_Session_Location_Address__c,
        listing_session_location_name__c=source.Listing_Session_Location_Name__c,
        listing_session_name__c=source.Listing_Session_Name__c,
        membership_end_date__c=source.Membership_End_Date__c,
        membership_price__c=source.Membership_Price__c,
        membership_required__c=source.Membership_Required__c,
        membership_start_date__c=source.Membership_Start_Date__c,
        parent_contact__c=source.Parent_Contact__c,
        status__c=source.Status__c,
        waitlist_participant_order__c=source.Waitlist_Participant_order__c,
        created_by_email__c=source.Created_By_Email__c,
        waitlist_unique_key__c=source.Waitlist_Unique_Key__c,
        status_is_in_process_or_selected__c=source.Status_Is_In_Process_or_Selected__c,
        dss_ingestion_timestamp=datetime.now()  # Capture current timestamp
    )


def map_waitlist_sources_to_raws(source_list: List[SfWaitlistSourceModel]) -> List[SfWaitlistRawDbModel]:
    """
    Maps a list of SfWaitlistSourceModel objects to a list of SfWaitlistRawDbModel objects.

    Args:
    - source_list: List of SfWaitlistSourceModel instances.

    Returns:
    - List of SfWaitlistRawDbModel instances.
    """
    return [map_waitlist_source_to_raw(source) for source in source_list]
    