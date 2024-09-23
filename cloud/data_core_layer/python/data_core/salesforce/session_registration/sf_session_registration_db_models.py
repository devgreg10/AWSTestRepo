from datetime import datetime
from attrs import define
from data_core.util.db_model import DbModel
from typing import List

@define(kw_only=True)
class SfSessionRegistrationSourceModel():
    """
    Base model for Salesforce SessionRegistration from S3
    """
    Id: str
    LastModifiedDate: str
    IsDeleted: str
    CreatedDate: str
    Name: str
    CreatedById: str
    LastModifiedById: str
    SystemModstamp: str
    LastActivityDate: str
    LastViewedDate: str
    LastReferencedDate: str
    Listing_Session__c: str
    Contact__c: str
    Allergies__c: str
    Amount_Paid__c: str
    Emergency_Contact_Email__c: str
    Emergency_Contact_Name__c: str
    Emergency_Contact_Number__c: str
    Financial_Aid_Applied__c: str
    Financial_Aid__c: str
    Membership_Registration__c: str
    Reevaluate_Waitlist__c: str
    Reggie_Registration_Id__c: str
    Status__c: str
    Trigger_Time__c: str
    Waitlist_Position__c: str
    Waitlist_Process__c: str
    Listing_record_type__c: str
    Delete_Expired_Registration__c: str
    Expiration_Date__c: str
    Bypass_Automation__c: str
    Count_Session_Registration__c: str
    Parent_Contact__c: str
    Listing_session_record_type__c: str
    SessionType_Replicated__c: str
    Charge_Amount__c: str
    Cost_Difference__c: str
    Discount__c: str
    Item_Price__c: str
    New_Listing_Session__c: str
    New_Session_Cost__c: str
    Transferred__c: str
    current_session_registration_number__c: str

@define(kw_only=True)
class SfSessionRegistrationRawDbModel(DbModel):
    """
    Base model for Raw Salesforce SessionRegistration
    """
    id: str
    lastmodifieddate: str
    isdeleted: str
    createddate: str
    name: str
    createdbyid: str
    lastmodifiedbyid: str
    systemmodstamp: str
    lastactivitydate: str
    lastvieweddate: str
    lastreferenceddate: str
    listing_session__c: str
    contact__c: str
    allergies__c: str
    amount_paid__c: str
    emergency_contact_email__c: str
    emergency_contact_name__c: str
    emergency_contact_number__c: str
    financial_aid_applied__c: str
    financial_aid__c: str
    membership_registration__c: str
    reevaluate_waitlist__c: str
    reggie_registration_id__c: str
    status__c: str
    trigger_time__c: str
    waitlist_position__c: str
    waitlist_process__c: str
    listing_record_type__c: str
    delete_expired_registration__c: str
    expiration_date__c: str
    bypass_automation__c: str
    count_session_registration__c: str
    parent_contact__c: str
    listing_session_record_type__c: str
    sessiontype_replicated__c: str
    charge_amount__c: str
    cost_difference__c: str
    discount__c: str
    item_price__c: str
    new_listing_session__c: str
    new_session_cost__c: str
    transferred__c: str
    current_session_registration_number__c: str
    dss_ingestion_timestamp: datetime

    # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)



def map_sf_session_registration_source_to_raw(source: SfSessionRegistrationSourceModel) -> SfSessionRegistrationRawDbModel:
    """
    Maps an instance of SfSessionRegistrationSourceModel to SfSessionRegistrationRawDbModel.
    """
    return SfSessionRegistrationRawDbModel(
        id=source.Id,
        lastmodifieddate=source.LastModifiedDate,
        isdeleted=source.IsDeleted,
        createddate=source.CreatedDate,
        name=source.Name,
        createdbyid=source.CreatedById,
        lastmodifiedbyid=source.LastModifiedById,
        systemmodstamp=source.SystemModstamp,
        lastactivitydate=source.LastActivityDate,
        lastvieweddate=source.LastViewedDate,
        lastreferenceddate=source.LastReferencedDate,
        listing_session__c=source.Listing_Session__c,
        contact__c=source.Contact__c,
        allergies__c=source.Allergies__c,
        amount_paid__c=source.Amount_Paid__c,
        emergency_contact_email__c=source.Emergency_Contact_Email__c,
        emergency_contact_name__c=source.Emergency_Contact_Name__c,
        emergency_contact_number__c=source.Emergency_Contact_Number__c,
        financial_aid_applied__c=source.Financial_Aid_Applied__c,
        financial_aid__c=source.Financial_Aid__c,
        membership_registration__c=source.Membership_Registration__c,
        reevaluate_waitlist__c=source.Reevaluate_Waitlist__c,
        reggie_registration_id__c=source.Reggie_Registration_Id__c,
        status__c=source.Status__c,
        trigger_time__c=source.Trigger_Time__c,
        waitlist_position__c=source.Waitlist_Position__c,
        waitlist_process__c=source.Waitlist_Process__c,
        listing_record_type__c=source.Listing_record_type__c,
        delete_expired_registration__c=source.Delete_Expired_Registration__c,
        expiration_date__c=source.Expiration_Date__c,
        bypass_automation__c=source.Bypass_Automation__c,
        count_session_registration__c=source.Count_Session_Registration__c,
        parent_contact__c=source.Parent_Contact__c,
        listing_session_record_type__c=source.Listing_session_record_type__c,
        sessiontype_replicated__c=source.SessionType_Replicated__c,
        charge_amount__c=source.Charge_Amount__c,
        cost_difference__c=source.Cost_Difference__c,
        discount__c=source.Discount__c,
        item_price__c=source.Item_Price__c,
        new_listing_session__c=source.New_Listing_Session__c,
        new_session_cost__c=source.New_Session_Cost__c,
        transferred__c=source.Transferred__c,
        current_session_registration_number__c=source.current_session_registration_number__c,
        dss_ingestion_timestamp=datetime.now()  # Set the current timestamp
    )

def map_sf_session_registration_sources_to_raws(source_list: List[SfSessionRegistrationSourceModel]) -> List[SfSessionRegistrationRawDbModel]:
    """
    Maps a list of SfSessionRegistrationSourceModel objects to a list of SfSessionRegistrationRawDbModel objects.

    Args:
    - source_list: List of SfSessionRegistrationSourceModel instances.

    Returns:
    - List of SfSessionRegistrationRawDbModel instances.
    """
    return [map_sf_session_registration_source_to_raw(source) for source in source_list]