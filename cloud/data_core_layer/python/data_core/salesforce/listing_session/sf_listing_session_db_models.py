from datetime import datetime
from attrs import define
from data_core.util.db_model import DbModel
from typing import List

@define(kw_only=True)
class SfListingSessionSourceModel():
    """
    Base model for Salesforce ListingSession from S3
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
    Listing__c: str
    Actual_Price__c: str
    Age_Restriction__c: str
    Base_Price__c: str
    Brief_Description__c: str
    Capacity_Notification_Threshold__c: str
    Coach_Assigned__c: str
    Confirmation_Supporting_Notes__c: str
    Curriculum_Hours__c: str
    Event_Coordinator__c: str
    Full_Description__c: str
    Gender_Restriction__c: str
    Listing_Session_Location_Address__c: str
    Listing_Session_Location_Name__c: str
    Max_Capacity__c: str
    Maximum_Age__c: str
    Membership_Discount_Active__c: str
    Membership_ID__c: str
    Membership_Required__c: str
    Military_Discount_Active__c: str
    Minimum_Age__c: str
    Number_of_Classes__c: str
    Youth_Serving_Program_Type__c: str
    Owner__c: str
    Participants_Reached__c: str
    Presented_By__c: str
    Primary_Image__c: str
    Primary_Program_Level_Restriction__c: str
    Program_Level__c: str
    Program_Sub_Level__c: str
    Publish_End_Date_Time__c: str
    Publish_Start_Date_Time__c: str
    Reggie_EventKey__c: str
    Reggie_EventType__c: str
    Reggie_Event_Id__c: str
    Register_End_Date_Time__c: str
    Register_Start_Date_Time__c: str
    Schedule_Price__c: str
    Season__c: str
    Secondary_Program_Level_Restriction__c: str
    Session_End_Date_Time__c: str
    Session_ID__c: str
    Session_Start_Date_Time__c: str
    Session_Status__c: str
    Sibling_Discount_Active__c: str
    Support_Coach_1__c: str
    Support_Coach_2__c: str
    Threshold_Notification_Email__c: str
    Total_Space_Available__c: str
    Total_Registrations__c: str
    Website__c: str
    Program_Coordinator__c: str
    Listing_Session_Location__c: str
    Presented_By_Name__c: str
    Region__c: str
    Days_Offered__c: str
    Third_Program_Level_Restriction__c: str
    Count_Listing_Session__c: str
    Priority__c: str
    Additional_Trade_Name__c: str
    Can_Be_Registered__c: str
    Waitlist_Counter_New__c: str
    Support_Coach_3_del__c: str
    Support_Coach_4_del__c: str
    Support_Coach_5_del__c: str
    Support_Coach_6_del__c: str
    Parent_Communication__c: str
    X18_Digit_ID__c: str
    WaitList_Space_Available__c: str
    Waitlist_Capacity__c: str
    Event_Hours__c: str
    Session_End_Date__c: str
    Session_End_Time__c: str
    Session_Start_Date__c: str
    Session_Start_Time__c: str
    Age_Eligibility_Date__c: str
    Allow_Early_Registration__c: str
    Direct_Session_Link__c: str
    Private_Event__c: str
    Parent_Communication_French__c: str
    Parent_Communication_Spanish__c: str
    Program_Type__c: str
    Lesson_Plan__c: str

@define(kw_only=True)
class SfListingSessionRawDbModel(DbModel):
    """
    Base model for Raw Salesforce listing_session
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
    listing__c: str
    actual_price__c: str
    age_restriction__c: str
    base_price__c: str
    brief_description__c: str
    capacity_notification_threshold__c: str
    coach_assigned__c: str
    confirmation_supporting_notes__c: str
    curriculum_hours__c: str
    event_coordinator__c: str
    full_description__c: str
    gender_restriction__c: str
    listing_session_location_address__c: str
    listing_session_location_name__c: str
    max_capacity__c: str
    maximum_age__c: str
    membership_discount_active__c: str
    membership_id__c: str
    membership_required__c: str
    military_discount_active__c: str
    minimum_age__c: str
    number_of_classes__c: str
    youth_serving_program_type__c: str
    owner__c: str
    participants_reached__c: str
    presented_by__c: str
    primary_image__c: str
    primary_program_level_restriction__c: str
    program_level__c: str
    program_sub_level__c: str
    publish_end_date_time__c: str
    publish_start_date_time__c: str
    reggie_eventkey__c: str
    reggie_eventtype__c: str
    reggie_event_id__c: str
    register_end_date_time__c: str
    register_start_date_time__c: str
    schedule_price__c: str
    season__c: str
    secondary_program_level_restriction__c: str
    session_end_date_time__c: str
    session_id__c: str
    session_start_date_time__c: str
    session_status__c: str
    sibling_discount_active__c: str
    support_coach_1__c: str
    support_coach_2__c: str
    threshold_notification_email__c: str
    total_space_available__c: str
    total_registrations__c: str
    website__c: str
    program_coordinator__c: str
    listing_session_location__c: str
    presented_by_name__c: str
    region__c: str
    days_offered__c: str
    third_program_level_restriction__c: str
    count_listing_session__c: str
    priority__c: str
    additional_trade_name__c: str
    can_be_registered__c: str
    waitlist_counter_new__c: str
    support_coach_3_del__c: str
    support_coach_4_del__c: str
    support_coach_5_del__c: str
    support_coach_6_del__c: str
    parent_communication__c: str
    x18_digit_id__c: str
    waitlist_space_available__c: str
    waitlist_capacity__c: str
    event_hours__c: str
    session_end_date__c: str
    session_end_time__c: str
    session_start_date__c: str
    session_start_time__c: str
    age_eligibility_date__c: str
    allow_early_registration__c: str
    direct_session_link__c: str
    private_event__c: str
    parent_communication_french__c: str
    parent_communication_spanish__c: str
    program_type__c: str
    lesson_plan__c: str
    dss_ingestion_timestamp: datetime

    # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)



def map_listing_session_source_to_raw(source: SfListingSessionSourceModel) -> SfListingSessionRawDbModel:
    """
    Maps an instance of SfListingSessionSourceModel to SfListingSessionRawDbModel.
    """
    return SfListingSessionRawDbModel(
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
        listing__c=source.Listing__c,
        actual_price__c=source.Actual_Price__c,
        age_restriction__c=source.Age_Restriction__c,
        base_price__c=source.Base_Price__c,
        brief_description__c=source.Brief_Description__c,
        capacity_notification_threshold__c=source.Capacity_Notification_Threshold__c,
        coach_assigned__c=source.Coach_Assigned__c,
        confirmation_supporting_notes__c=source.Confirmation_Supporting_Notes__c,
        curriculum_hours__c=source.Curriculum_Hours__c,
        event_coordinator__c=source.Event_Coordinator__c,
        full_description__c=source.Full_Description__c,
        gender_restriction__c=source.Gender_Restriction__c,
        listing_session_location_address__c=source.Listing_Session_Location_Address__c,
        listing_session_location_name__c=source.Listing_Session_Location_Name__c,
        max_capacity__c=source.Max_Capacity__c,
        maximum_age__c=source.Maximum_Age__c,
        membership_discount_active__c=source.Membership_Discount_Active__c,
        membership_id__c=source.Membership_ID__c,
        membership_required__c=source.Membership_Required__c,
        military_discount_active__c=source.Military_Discount_Active__c,
        minimum_age__c=source.Minimum_Age__c,
        number_of_classes__c=source.Number_of_Classes__c,
        youth_serving_program_type__c=source.Youth_Serving_Program_Type__c,
        owner__c=source.Owner__c,
        participants_reached__c=source.Participants_Reached__c,
        presented_by__c=source.Presented_By__c,
        primary_image__c=source.Primary_Image__c,
        primary_program_level_restriction__c=source.Primary_Program_Level_Restriction__c,
        program_level__c=source.Program_Level__c,
        program_sub_level__c=source.Program_Sub_Level__c,
        publish_end_date_time__c=source.Publish_End_Date_Time__c,
        publish_start_date_time__c=source.Publish_Start_Date_Time__c,
        reggie_eventkey__c=source.Reggie_EventKey__c,
        reggie_eventtype__c=source.Reggie_EventType__c,
        reggie_event_id__c=source.Reggie_Event_Id__c,
        register_end_date_time__c=source.Register_End_Date_Time__c,
        register_start_date_time__c=source.Register_Start_Date_Time__c,
        schedule_price__c=source.Schedule_Price__c,
        season__c=source.Season__c,
        secondary_program_level_restriction__c=source.Secondary_Program_Level_Restriction__c,
        session_end_date_time__c=source.Session_End_Date_Time__c,
        session_id__c=source.Session_ID__c,
        session_start_date_time__c=source.Session_Start_Date_Time__c,
        session_status__c=source.Session_Status__c,
        sibling_discount_active__c=source.Sibling_Discount_Active__c,
        support_coach_1__c=source.Support_Coach_1__c,
        support_coach_2__c=source.Support_Coach_2__c,
        threshold_notification_email__c=source.Threshold_Notification_Email__c,
        total_space_available__c=source.Total_Space_Available__c,
        total_registrations__c=source.Total_Registrations__c,
        website__c=source.Website__c,
        program_coordinator__c=source.Program_Coordinator__c,
        listing_session_location__c=source.Listing_Session_Location__c,
        presented_by_name__c=source.Presented_By_Name__c,
        region__c=source.Region__c,
        days_offered__c=source.Days_Offered__c,
        third_program_level_restriction__c=source.Third_Program_Level_Restriction__c,
        count_listing_session__c=source.Count_Listing_Session__c,
        priority__c=source.Priority__c,
        additional_trade_name__c=source.Additional_Trade_Name__c,
        can_be_registered__c=source.Can_Be_Registered__c,
        waitlist_counter_new__c=source.Waitlist_Counter_New__c,
        support_coach_3_del__c=source.Support_Coach_3_del__c,
        support_coach_4_del__c=source.Support_Coach_4_del__c,
        support_coach_5_del__c=source.Support_Coach_5_del__c,
        support_coach_6_del__c=source.Support_Coach_6_del__c,
        parent_communication__c=source.Parent_Communication__c,
        x18_digit_id__c=source.X18_Digit_ID__c,
        waitlist_space_available__c=source.WaitList_Space_Available__c,
        waitlist_capacity__c=source.Waitlist_Capacity__c,
        event_hours__c=source.Event_Hours__c,
        session_end_date__c=source.Session_End_Date__c,
        session_end_time__c=source.Session_End_Time__c,
        session_start_date__c=source.Session_Start_Date__c,
        session_start_time__c=source.Session_Start_Time__c,
        age_eligibility_date__c=source.Age_Eligibility_Date__c,
        allow_early_registration__c=source.Allow_Early_Registration__c,
        direct_session_link__c=source.Direct_Session_Link__c,
        private_event__c=source.Private_Event__c,
        parent_communication_french__c=source.Parent_Communication_French__c,
        parent_communication_spanish__c=source.Parent_Communication_Spanish__c,
        program_type__c=source.Program_Type__c,
        lesson_plan__c=source.Lesson_Plan__c,
        dss_ingestion_timestamp=datetime.now()
    )

def map_listing_session_sources_to_raws(source_list: List[SfListingSessionSourceModel]) -> List[SfListingSessionRawDbModel]:
    """
    Maps a list of SfListingSessionSourceModel objects to a list of SfListingSessionRawDbModel objects.

    Args:
    - source_list: List of SfListingSessionSourceModel instances.

    Returns:
    - List of SfListingSessionRawDbModel instances.
    """
    return [map_listing_session_source_to_raw(source) for source in source_list]