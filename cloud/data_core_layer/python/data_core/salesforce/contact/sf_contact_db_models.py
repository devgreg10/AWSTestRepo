from datetime import datetime
from attrs import define
from data_core.util.db_model import DbModel
from typing import List

@define(kw_only=True)
class SfContactSourceModel():
    """
    Base model for Salesforce Contact from S3
    """
    Id: str 
    IsDeleted: str
    MasterRecordId: str
    AccountId: str
    LastName: str
    FirstName: str
    Salutation: str
    Name: str
    MailingStreet: str
    MailingCity: str
    MailingState: str
    MailingPostalCode: str
    MailingCountry: str
    MailingLatitude: str
    MailingLongitude: str
    MailingGeocodeAccuracy: str
    MailingAddress: str
    Phone: str
    MobilePhone: str
    HomePhone: str
    OtherPhone: str
    ReportsToId: str
    Email: str
    Title: str
    Department: str
    LeadSource: str
    Birthdate: str
    Description: str
    OwnerId: str
    HasOptedOutOfEmail: str
    DoNotCall: str
    CreatedDate: str
    CreatedById: str
    LastModifiedDate: str
    LastModifiedById: str
    SystemModstamp: str
    EmailBouncedReason: str
    EmailBouncedDate: str
    IsEmailBounced: str
    IndividualId: str
    Pronouns: str
    GenderIdentity: str
    Chapter_Affiliation__c: str
    ChapterID__c: str
    Override__c: str
    Active__c: str
    OwnerID__c: str
    Trigger_Time__c: str
    Contact_Type_Text__c: str
    Military__c: str
    Contact_ID__c: str
    Primary_Contact_s_Email__c: str
    Region__c: str
    ChapterID_CONTACT__c: str
    Accomplishment_Level__c: str
    Additional_Information__c: str
    Area_of_Interest__c: str
    Attended_Coach_Advanced_Training__c: str
    Birthdate_Map__c: str
    Branded_Program_Location__c: str
    CASESAFEID__c: str
    Chapter_Title__c: str
    First_Tee_Employment_Type__c: str
    Coach_Fee_Amount_Paid__c: str
    Coach_Fee_Paid_Date__c: str
    Coach_Inactive_Date__c: str
    Coach_Level__c: str
    Golf_Level__c: str
    Inactive__c: str
    Legacy_ID__c: str
    School_City__c: str
    School_Name_Other__c: str
    School_Name__c: str
    School_State__c: str
    Transitioned__c: str
    Player_Coach_Training__c: str
    Total_Available_Credits__c: str
    Docebo_Is_Coach__c: str
    Request_Advanced_Training__c: str
    Participant_Level_Accomplishments__c: str
    Coach_Membership_Fee_Due__c: str
    Coach_Membership_Fee_Paid__c: str
    Coach_Program_Start_Date__c: str
    Coach_Qualified_For__c: str
    Coach_Retention_Status_Changed_Date__c: str
    Company__c: str
    Contact_Type__c: str
    Curriculum_Session_Registration_Date__c: str
    Custom_Created_Date__c: str
    Eligible_Participants__c: str
    Executive_Director_Email__c: str
    General_Availability_Days_of_the_Week__c: str
    General_Availability_Seasons__c: str
    Golf_Course_Affiliation__c: str
    Home_Phone_Map__c: str
    Hours_of_Training_Attended__c: str
    How_did_you_hear_about_us__c: str
    Is_Coach__c: str
    Lead_Coach_Total_Hours__c: str
    Age__c: str
    Allergies__c: str
    Contact_Inactive_Date__c: str
    County__c: str
    Dietary_Restrictions__c: str
    Disabilities__c: str
    Emergency_Contact_Email__c: str
    Emergency_Contact_Name__c: str
    Emergency_Contact_Number__c: str
    Request_ACT_Training__c: str
    Emergency_Contact__c: str
    Ethnicity__c: str
    Gender__c: str
    Lead_Coach_Total_Sessions__c: str
    Lead_Source_Other__c: str
    Lifetime_Member__c: str
    Listing__c: str
    Mailing_Address_Company__c: str
    Main_Program_Location__c: str
    Master_Coach_Recognition_Year__c: str
    Other_Address_Company__c: str
    Other_Golf_Certifications_Description__c: str
    Other_Qualifications__c: str
    Participation_Status__c: str
    Primary_Contact_s_Mobile_Phone_Provider__c: str
    Primary_Contact_s_Mobile__c: str
    Primary_Contact_s_Name__c: str
    Program_Director_Primary_Email__c: str
    Program_Director_Secondary_Email__c: str
    Program_Level__c: str
    Progressed_Participants__c: str
    FMP_ID__c: str
    Grade__c: str
    Historic_Health_Info__c: str
    Legacy_Participant_User_Id__c: str
    MRM_ID__c: str
    Membership_Created_Date__c: str
    Membership_Status__c: str
    Membership_Valid_From__c: str
    Membership_Valid_To__c: str
    Middle_Name__c: str
    Military_Affiliation__c: str
    Military_Base_Installation__c: str
    Military_Branch__c: str
    Mobile_Phone_Provider__c: str
    Nickname__c: str
    Preferred_Contact_Method__c: str
    Reggie_Id__c: str
    Reggie_Participant_Id__c: str
    Relationship_To_Participants__c: str
    Secondary_Email__c: str
    USGA_Handicap__c: str
    Work_Phone__c: str
    Progression_Status_Changed_Date__c: str
    Progression_Status__c: str
    Reason_For_Leaving__c: str
    Recognized_The_First_Tee_Coach_Year__c: str
    Retained_Coach_Count__c: str
    Retention_Status__c: str
    Secondary_Contact_s_Email__c: str
    Serve_as_National_Trainer__c: str
    Served_as_One_Day_Observer__c: str
    Session_Transfer_From__c: str
    Session_Transfer_To__c: str
    Status__c: str
    Support_Coach_Total_Coaching_Hours__c: str
    Support_Coach_Total_Sessions__c: str
    Title__c: str
    Total_Coaching_Hours1__c: str
    Total_Coaching_Hours__c: str
    Total_Listing_Sessions_Delivered__c: str
    Total_Sessions_Coach_Assigned__c: str
    Track_Program_Level__c: str
    Participant_Hours__c: str
    Parent_Contact__c: str
    Litmos__Access_Level__c: str
    Litmos__Completed_Percentage__c: str
    Litmos__Courses_Assigned__c: str
    Litmos__Courses_Completed__c: str
    Litmos__Deactivate_From_Litmos__c: str
    Litmos__Full_Name__c: str
    Litmos__Languages__c: str
    Litmos__Level__c: str
    Litmos__LitmosID__c: str
    Litmos__Litmos_Activated__c: str
    Litmos__Litmos_Login_Access__c: str
    Litmos__Litmos_UserId__c: str
    Litmos__OriginalId__c: str
    Litmos__Sync_Litmos__c: str
    Litmos__Total_Sum_Percentages__c: str
    Litmos__User_Id__c: str
    Litmos__p_Completed_Percentage__c: str
    Executive_Updates__c: str
    SafeSport_Compliance_not_Required__c: str
    Student_Notes__c: str
    Language__c: str
    FT_App_Pilot__c: str
    International_Chapter__c: str
    FYI__c: str
    Impact_Today__c: str
    Press_Clippings__c: str
    Market_Size__c: str
    Territory__c: str
    Active_Chapter__c: str
    CombinationforKey__c: str
    Created_by_lead__c: str
    Sports_Engine_ID__c: str
    Cloudingo_Related_Account_Type__c: str
    MDR__c: str
    Merged_Email__c: str
    MDR_NID__c: str
    Ethnicity_Location__c: str
    Duplicates_Removed__c: str
    AccountIdDUPMatch__c: str
    Birthdate_Dupe_Match__c: str
    Background_Check_Exp_bucket__c: str
    Background_Check_Expiration_Date__c: str
    Background_Check_Passed__c: str
    Background_Check_Status__c: str
    Compliant__c: str
    Days_until_Background_Check_Expires__c: str
    Days_until_SafeSport_Training_Expires__c: str
    Number_of_Safety_Records__c: str
    SafeSport_Training_Completed__c: str
    SafeSport_Training_Exp_bucket__c: str
    SafeSport_Training_Expiration_Date__c: str
    SafeSport_Training_Status__c: str
    Contact_Name__c: str
    Training_Type__c: str
    Chapter_Safe_Sport_Designation__c: str
    Docebo_Email__c: str
    Docebo_User_ID__c: str
    Docebo_Is_Community_Staff__c: str
    Docebo_Is_Parent__c: str
    Docebo_Is_Participant__c: str
    Docebo_Is_School_Staff__c: str
    Docebo_Username_Override__c: str
    Docebo_Username__c: str
    Salesforce_Username__c: str
    Docebo_Pilot_User__c: str
    Docebo_Is_HQ_Staff__c: str
    Docebo_Disconnect__c: str
    Docebo_Account_Active__c: str
    Master_Coach__c: str
    Primary_Contact_Home_Phone__c: str
    User_Email__c: str
    Impact_Today_Board_Members__c: str
    Impact_Today_Coaches__c: str
    Impact_Today_Leaders__c: str
    NCCP__c: str
    Pursuing_Ace__c: str
    Docebo_Is_Junior_Coach__c: str
    Lifetime_Gamification_Points__c: str
    Respect_In_Sport_Certification__c: str
    Canva_Admin__c: str
    Website_Admin__c: str
    Docebo_Username_Randomly_Generated__c: str

@define(kw_only=True)
class SfContactRawDbModel(DbModel):
    """
    Raw DB Model for Salesforce Contact from raw source with lowercase field names
    """
    id: str
    isdeleted: str
    masterrecordid: str
    accountid: str
    lastname: str
    firstname: str
    salutation: str
    name: str
    mailingstreet: str
    mailingcity: str
    mailingstate: str
    mailingpostalcode: str
    mailingcountry: str
    mailinglatitude: str
    mailinglongitude: str
    mailinggeocodeaccuracy: str
    mailingaddress: str
    phone: str
    mobilephone: str
    homephone: str
    otherphone: str
    reportstoid: str
    email: str
    title: str
    department: str
    leadsource: str
    birthdate: str
    description: str
    ownerid: str
    hasoptedoutofemail: str
    donotcall: str
    createddate: str
    createdbyid: str
    lastmodifieddate: str
    lastmodifiedbyid: str
    systemmodstamp: str
    emailbouncedreason: str
    emailbounceddate: str
    isemailbounced: str
    individualid: str
    pronouns: str
    genderidentity: str
    chapter_affiliation__c: str
    chapterid__c: str
    override__c: str
    active__c: str
    ownerid__c: str
    trigger_time__c: str
    contact_type_text__c: str
    military__c: str
    contact_id__c: str
    primary_contact_s_email__c: str
    region__c: str
    chapterid_contact__c: str
    accomplishment_level__c: str
    additional_information__c: str
    area_of_interest__c: str
    attended_coach_advanced_training__c: str
    birthdate_map__c: str
    branded_program_location__c: str
    casesafeid__c: str
    chapter_title__c: str
    first_tee_employment_type__c: str
    coach_fee_amount_paid__c: str
    coach_fee_paid_date__c: str
    coach_inactive_date__c: str
    coach_level__c: str
    golf_level__c: str
    inactive__c: str
    legacy_id__c: str
    school_city__c: str
    school_name_other__c: str
    school_name__c: str
    school_state__c: str
    transitioned__c: str
    player_coach_training__c: str
    total_available_credits__c: str
    docebo_is_coach__c: str
    request_advanced_training__c: str
    participant_level_accomplishments__c: str
    coach_membership_fee_due__c: str
    coach_membership_fee_paid__c: str
    coach_program_start_date__c: str
    coach_qualified_for__c: str
    coach_retention_status_changed_date__c: str
    company__c: str
    contact_type__c: str
    curriculum_session_registration_date__c: str
    custom_created_date__c: str
    eligible_participants__c: str
    executive_director_email__c: str
    general_availability_days_of_the_week__c: str
    general_availability_seasons__c: str
    golf_course_affiliation__c: str
    home_phone_map__c: str
    hours_of_training_attended__c: str
    how_did_you_hear_about_us__c: str
    is_coach__c: str
    lead_coach_total_hours__c: str
    age__c: str
    allergies__c: str
    contact_inactive_date__c: str
    county__c: str
    dietary_restrictions__c: str
    disabilities__c: str
    emergency_contact_email__c: str
    emergency_contact_name__c: str
    emergency_contact_number__c: str
    request_act_training__c: str
    emergency_contact__c: str
    ethnicity__c: str
    gender__c: str
    lead_coach_total_sessions__c: str
    lead_source_other__c: str
    lifetime_member__c: str
    listing__c: str
    mailing_address_company__c: str
    main_program_location__c: str
    master_coach_recognition_year__c: str
    other_address_company__c: str
    other_golf_certifications_description__c: str
    other_qualifications__c: str
    participation_status__c: str
    primary_contact_s_mobile_phone_provider__c: str
    primary_contact_s_mobile__c: str
    primary_contact_s_name__c: str
    program_director_primary_email__c: str
    program_director_secondary_email__c: str
    program_level__c: str
    progressed_participants__c: str
    fmp_id__c: str
    grade__c: str
    historic_health_info__c: str
    legacy_participant_user_id__c: str
    mrm_id__c: str
    membership_created_date__c: str
    membership_status__c: str
    membership_valid_from__c: str
    membership_valid_to__c: str
    middle_name__c: str
    military_affiliation__c: str
    military_base_installation__c: str
    military_branch__c: str
    mobile_phone_provider__c: str
    nickname__c: str
    preferred_contact_method__c: str
    reggie_id__c: str
    reggie_participant_id__c: str
    relationship_to_participants__c: str
    secondary_email__c: str
    usga_handicap__c: str
    work_phone__c: str
    progression_status_changed_date__c: str
    progression_status__c: str
    reason_for_leaving__c: str
    recognized_the_first_tee_coach_year__c: str
    retained_coach_count__c: str
    retention_status__c: str
    secondary_contact_s_email__c: str
    serve_as_national_trainer__c: str
    served_as_one_day_observer__c: str
    session_transfer_from__c: str
    session_transfer_to__c: str
    status__c: str
    support_coach_total_coaching_hours__c: str
    support_coach_total_sessions__c: str
    title__c: str
    total_coaching_hours1__c: str
    total_coaching_hours__c: str
    total_listing_sessions_delivered__c: str
    total_sessions_coach_assigned__c: str
    track_program_level__c: str
    participant_hours__c: str
    parent_contact__c: str
    litmos__access_level__c: str
    litmos__completed_percentage__c: str
    litmos__courses_assigned__c: str
    litmos__courses_completed__c: str
    litmos__deactivate_from_litmos__c: str
    litmos__full_name__c: str
    litmos__languages__c: str
    litmos__level__c: str
    litmos__litmosid__c: str
    litmos__litmos_activated__c: str
    litmos__litmos_login_access__c: str
    litmos__litmos_userid__c: str
    litmos__originalid__c: str
    litmos__sync_litmos__c: str
    litmos__total_sum_percentages__c: str
    litmos__user_id__c: str
    litmos__p_completed_percentage__c: str
    executive_updates__c: str
    safesport_compliance_not_required__c: str
    student_notes__c: str
    language__c: str
    ft_app_pilot__c: str
    international_chapter__c: str
    fyi__c: str
    impact_today__c: str
    press_clippings__c: str
    market_size__c: str
    territory__c: str
    active_chapter__c: str
    combinationforkey__c: str
    created_by_lead__c: str
    sports_engine_id__c: str
    cloudingo_related_account_type__c: str
    mdr__c: str
    merged_email__c: str
    mdr_nid__c: str
    ethnicity_location__c: str
    duplicates_removed__c: str
    accountiddubmatch__c: str
    birthdate_dupe_match__c: str
    background_check_exp_bucket__c: str
    background_check_expiration_date__c: str
    background_check_passed__c: str
    background_check_status__c: str
    compliant__c: str
    days_until_background_check_expires__c: str
    days_until_safesport_training_expires__c: str
    number_of_safety_records__c: str
    safesport_training_completed__c: str
    safesport_training_exp_bucket__c: str
    safesport_training_expiration_date__c: str
    safesport_training_status__c: str
    contact_name__c: str
    training_type__c: str
    chapter_safe_sport_designation__c: str
    docebo_email__c: str
    docebo_user_id__c: str
    docebo_is_community_staff__c: str
    docebo_is_parent__c: str
    docebo_is_participant__c: str
    docebo_is_school_staff__c: str
    docebo_username_override__c: str
    docebo_username__c: str
    salesforce_username__c: str
    docebo_pilot_user__c: str
    docebo_is_hq_staff__c: str
    docebo_disconnect__c: str
    docebo_account_active__c: str
    master_coach__c: str
    primary_contact_home_phone__c: str
    user_email__c: str
    impact_today_board_members__c: str
    impact_today_coaches__c: str
    impact_today_leaders__c: str
    nccp__c: str
    pursuing_ace__c: str
    docebo_is_junior_coach__c: str
    lifetime_gamification_points__c: str
    respect_in_sport_certification__c: str
    canva_admin__c: str
    website_admin__c: str
    docebo_username_randomly_generated__c: str

    # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)



def map_sf_contact_source_to_raw(source: SfContactSourceModel) -> SfContactRawDbModel:
    """
    Maps an instance of SfContactSourceModel to SfContactRawDbModel.
    """
    return SfContactRawDbModel(
        id=source.Id,
        isdeleted=source.IsDeleted,
        masterrecordid=source.MasterRecordId,
        accountid=source.AccountId,
        lastname=source.LastName,
        firstname=source.FirstName,
        salutation=source.Salutation,
        name=source.Name,
        mailingstreet=source.MailingStreet,
        mailingcity=source.MailingCity,
        mailingstate=source.MailingState,
        mailingpostalcode=source.MailingPostalCode,
        mailingcountry=source.MailingCountry,
        mailinglatitude=source.MailingLatitude,
        mailinglongitude=source.MailingLongitude,
        mailinggeocodeaccuracy=source.MailingGeocodeAccuracy,
        mailingaddress=source.MailingAddress,
        phone=source.Phone,
        mobilephone=source.MobilePhone,
        homephone=source.HomePhone,
        otherphone=source.OtherPhone,
        reportstoid=source.ReportsToId,
        email=source.Email,
        title=source.Title,
        department=source.Department,
        leadsource=source.LeadSource,
        birthdate=source.Birthdate,
        description=source.Description,
        ownerid=source.OwnerId,
        hasoptedoutofemail=source.HasOptedOutOfEmail,
        donotcall=source.DoNotCall,
        createddate=source.CreatedDate,
        createdbyid=source.CreatedById,
        lastmodifieddate=source.LastModifiedDate,
        lastmodifiedbyid=source.LastModifiedById,
        systemmodstamp=source.SystemModstamp,
        emailbouncedreason=source.EmailBouncedReason,
        emailbounceddate=source.EmailBouncedDate,
        isemailbounced=source.IsEmailBounced,
        individualid=source.IndividualId,
        pronouns=source.Pronouns,
        genderidentity=source.GenderIdentity,
        chapter_affiliation__c=source.Chapter_Affiliation__c,
        chapterid__c=source.ChapterID__c,
        override__c=source.Override__c,
        active__c=source.Active__c,
        ownerid__c=source.OwnerID__c,
        trigger_time__c=source.Trigger_Time__c,
        contact_type_text__c=source.Contact_Type_Text__c,
        military__c=source.Military__c,
        contact_id__c=source.Contact_ID__c,
        primary_contact_s_email__c=source.Primary_Contact_s_Email__c,
        region__c=source.Region__c,
        chapterid_contact__c=source.ChapterID_CONTACT__c,
        accomplishment_level__c=source.Accomplishment_Level__c,
        additional_information__c=source.Additional_Information__c,
        area_of_interest__c=source.Area_of_Interest__c,
        attended_coach_advanced_training__c=source.Attended_Coach_Advanced_Training__c,
        birthdate_map__c=source.Birthdate_Map__c,
        branded_program_location__c=source.Branded_Program_Location__c,
        casesafeid__c=source.CASESAFEID__c,
        chapter_title__c=source.Chapter_Title__c,
        first_tee_employment_type__c=source.First_Tee_Employment_Type__c,
        coach_fee_amount_paid__c=source.Coach_Fee_Amount_Paid__c,
        coach_fee_paid_date__c=source.Coach_Fee_Paid_Date__c,
        coach_inactive_date__c=source.Coach_Inactive_Date__c,
        coach_level__c=source.Coach_Level__c,
        golf_level__c=source.Golf_Level__c,
        inactive__c=source.Inactive__c,
        legacy_id__c=source.Legacy_ID__c,
        school_city__c=source.School_City__c,
        school_name_other__c=source.School_Name_Other__c,
        school_name__c=source.School_Name__c,
        school_state__c=source.School_State__c,
        transitioned__c=source.Transitioned__c,
        player_coach_training__c=source.Player_Coach_Training__c,
        total_available_credits__c=source.Total_Available_Credits__c,
        docebo_is_coach__c=source.Docebo_Is_Coach__c,
        request_advanced_training__c=source.Request_Advanced_Training__c,
        participant_level_accomplishments__c=source.Participant_Level_Accomplishments__c,
        coach_membership_fee_due__c=source.Coach_Membership_Fee_Due__c,
        coach_membership_fee_paid__c=source.Coach_Membership_Fee_Paid__c,
        coach_program_start_date__c=source.Coach_Program_Start_Date__c,
        coach_qualified_for__c=source.Coach_Qualified_For__c,
        coach_retention_status_changed_date__c=source.Coach_Retention_Status_Changed_Date__c,
        company__c=source.Company__c,
        contact_type__c=source.Contact_Type__c,
        curriculum_session_registration_date__c=source.Curriculum_Session_Registration_Date__c,
        custom_created_date__c=source.Custom_Created_Date__c,
        eligible_participants__c=source.Eligible_Participants__c,
        executive_director_email__c=source.Executive_Director_Email__c,
        general_availability_days_of_the_week__c=source.General_Availability_Days_of_the_Week__c,
        general_availability_seasons__c=source.General_Availability_Seasons__c,
        golf_course_affiliation__c=source.Golf_Course_Affiliation__c,
        home_phone_map__c=source.Home_Phone_Map__c,
        hours_of_training_attended__c=source.Hours_of_Training_Attended__c,
        how_did_you_hear_about_us__c=source.How_did_you_hear_about_us__c,
        is_coach__c=source.Is_Coach__c,
        lead_coach_total_hours__c=source.Lead_Coach_Total_Hours__c,
        age__c=source.Age__c,
        allergies__c=source.Allergies__c,
        contact_inactive_date__c=source.Contact_Inactive_Date__c,
        county__c=source.County__c,
        dietary_restrictions__c=source.Dietary_Restrictions__c,
        disabilities__c=source.Disabilities__c,
        emergency_contact_email__c=source.Emergency_Contact_Email__c,
        emergency_contact_name__c=source.Emergency_Contact_Name__c,
        emergency_contact_number__c=source.Emergency_Contact_Number__c,
        request_act_training__c=source.Request_ACT_Training__c,
        emergency_contact__c=source.Emergency_Contact__c,
        ethnicity__c=source.Ethnicity__c,
        gender__c=source.Gender__c,
        lead_coach_total_sessions__c=source.Lead_Coach_Total_Sessions__c,
        lead_source_other__c=source.Lead_Source_Other__c,
        lifetime_member__c=source.Lifetime_Member__c,
        listing__c=source.Listing__c,
        mailing_address_company__c=source.Mailing_Address_Company__c,
        main_program_location__c=source.Main_Program_Location__c,
        master_coach_recognition_year__c=source.Master_Coach_Recognition_Year__c,
        other_address_company__c=source.Other_Address_Company__c,
        other_golf_certifications_description__c=source.Other_Golf_Certifications_Description__c,
        other_qualifications__c=source.Other_Qualifications__c,
        participation_status__c=source.Participation_Status__c,
        primary_contact_s_mobile_phone_provider__c=source.Primary_Contact_s_Mobile_Phone_Provider__c,
        primary_contact_s_mobile__c=source.Primary_Contact_s_Mobile__c,
        primary_contact_s_name__c=source.Primary_Contact_s_Name__c,
        program_director_primary_email__c=source.Program_Director_Primary_Email__c,
        program_director_secondary_email__c=source.Program_Director_Secondary_Email__c,
        program_level__c=source.Program_Level__c,
        progressed_participants__c=source.Progressed_Participants__c,
        fmp_id__c=source.FMP_ID__c,
        grade__c=source.Grade__c,
        historic_health_info__c=source.Historic_Health_Info__c,
        legacy_participant_user_id__c=source.Legacy_Participant_User_Id__c,
        mrm_id__c=source.MRM_ID__c,
        membership_created_date__c=source.Membership_Created_Date__c,
        membership_status__c=source.Membership_Status__c,
        membership_valid_from__c=source.Membership_Valid_From__c,
        membership_valid_to__c=source.Membership_Valid_To__c,
        middle_name__c=source.Middle_Name__c,
        middle_name__c=source.Middle_Name__c,
        military_affiliation__c=source.Military_Affiliation__c,
        military_base_installation__c=source.Military_Base_Installation__c,
        military_branch__c=source.Military_Branch__c,
        mobile_phone_provider__c=source.Mobile_Phone_Provider__c,
        nickname__c=source.Nickname__c,
        preferred_contact_method__c=source.Preferred_Contact_Method__c,
        reggie_id__c=source.Reggie_Id__c,
        reggie_participant_id__c=source.Reggie_Participant_Id__c,
        relationship_to_participants__c=source.Relationship_To_Participants__c,
        secondary_email__c=source.Secondary_Email__c,
        usga_handicap__c=source.USGA_Handicap__c,
        work_phone__c=source.Work_Phone__c,
        progression_status_changed_date__c=source.Progression_Status_Changed_Date__c,
        progression_status__c=source.Progression_Status__c,
        reason_for_leaving__c=source.Reason_For_Leaving__c,
        recognized_the_first_tee_coach_year__c=source.Recognized_The_First_Tee_Coach_Year__c,
        retained_coach_count__c=source.Retained_Coach_Count__c,
        retention_status__c=source.Retention_Status__c,
        secondary_contact_s_email__c=source.Secondary_Contact_s_Email__c,
        serve_as_national_trainer__c=source.Serve_as_National_Trainer__c,
        served_as_one_day_observer__c=source.Served_as_One_Day_Observer__c,
        session_transfer_from__c=source.Session_Transfer_From__c,
        session_transfer_to__c=source.Session_Transfer_To__c,
        status__c=source.Status__c,
        support_coach_total_coaching_hours__c=source.Support_Coach_Total_Coaching_Hours__c,
        support_coach_total_sessions__c=source.Support_Coach_Total_Sessions__c,
        title__c=source.Title__c,
        total_coaching_hours1__c=source.Total_Coaching_Hours1__c,
        total_coaching_hours__c=source.Total_Coaching_Hours__c,
        total_listing_sessions_delivered__c=source.Total_Listing_Sessions_Delivered__c,
        total_sessions_coach_assigned__c=source.Total_Sessions_Coach_Assigned__c,
        track_program_level__c=source.Track_Program_Level__c,
        participant_hours__c=source.Participant_Hours__c,
        parent_contact__c=source.Parent_Contact__c,
        litmos__access_level__c=source.Litmos__Access_Level__c,
        litmos__completed_percentage__c=source.Litmos__Completed_Percentage__c,
        litmos__courses_assigned__c=source.Litmos__Courses_Assigned__c,
        litmos__courses_completed__c=source.Litmos__Courses_Completed__c,
        litmos__deactivate_from_litmos__c=source.Litmos__Deactivate_From_Litmos__c,
        litmos__full_name__c=source.Litmos__Full_Name__c,
        litmos__languages__c=source.Litmos__Languages__c,
        litmos__level__c=source.Litmos__Level__c,
        litmos__litmosid__c=source.Litmos__LitmosID__c,
        litmos__litmos_activated__c=source.Litmos__Litmos_Activated__c,
        litmos__litmos_login_access__c=source.Litmos__Litmos_Login_Access__c,
        litmos__litmos_userid__c=source.Litmos__Litmos_UserId__c,
        litmos__originalid__c=source.Litmos__OriginalId__c,
        litmos__sync_litmos__c=source.Litmos__Sync_Litmos__c,
        litmos__total_sum_percentages__c=source.Litmos__Total_Sum_Percentages__c,
        litmos__user_id__c=source.Litmos__User_Id__c,
        litmos__p_completed_percentage__c=source.Litmos__p_Completed_Percentage__c,
        executive_updates__c=source.Executive_Updates__c,
        safesport_compliance_not_required__c=source.SafeSport_Compliance_not_Required__c,
        student_notes__c=source.Student_Notes__c,
        language__c=source.Language__c,
        ft_app_pilot__c=source.FT_App_Pilot__c,
        international_chapter__c=source.International_Chapter__c,
        fyi__c=source.FYI__c,
        impact_today__c=source.Impact_Today__c,
        press_clippings__c=source.Press_Clippings__c,
        market_size__c=source.Market_Size__c,
        territory__c=source.Territory__c,
        active_chapter__c=source.Active_Chapter__c,
        combinationforkey__c=source.CombinationforKey__c,
        created_by_lead__c=source.Created_by_lead__c,
        sports_engine_id__c=source.Sports_Engine_ID__c,
        cloudingo_related_account_type__c=source.Cloudingo_Related_Account_Type__c,
        mdr__c=source.MDR__c,
        merged_email__c=source.Merged_Email__c,
        mdr_nid__c=source.MDR_NID__c,
        ethnicity_location__c=source.Ethnicity_Location__c,
        duplicates_removed__c=source.Duplicates_Removed__c,
        accountiddubmatch__c=source.AccountIdDUPMatch__c,
        birthdate_dupe_match__c=source.Birthdate_Dupe_Match__c,
        background_check_exp_bucket__c=source.Background_Check_Exp_bucket__c,
        background_check_expiration_date__c=source.Background_Check_Expiration_Date__c,
        background_check_passed__c=source.Background_Check_Passed__c,
        background_check_status__c=source.Background_Check_Status__c,
        compliant__c=source.Compliant__c,
        days_until_background_check_expires__c=source.Days_until_Background_Check_Expires__c,
        days_until_safesport_training_expires__c=source.Days_until_SafeSport_Training_Expires__c,
        number_of_safety_records__c=source.Number_of_Safety_Records__c,
        safesport_training_completed__c=source.SafeSport_Training_Completed__c,
        safesport_training_exp_bucket__c=source.SafeSport_Training_Exp_bucket__c,
        safesport_training_expiration_date__c=source.SafeSport_Training_Expiration_Date__c,
        safesport_training_status__c=source.SafeSport_Training_Status__c,
        contact_name__c=source.Contact_Name__c,
        training_type__c=source.Training_Type__c,
        chapter_safe_sport_designation__c=source.Chapter_Safe_Sport_Designation__c,
        docebo_email__c=source.Docebo_Email__c,
        docebo_user_id__c=source.Docebo_User_ID__c,
        docebo_is_community_staff__c=source.Docebo_Is_Community_Staff__c,
        docebo_is_parent__c=source.Docebo_Is_Parent__c,
        docebo_is_participant__c=source.Docebo_Is_Participant__c,
        docebo_is_school_staff__c=source.Docebo_Is_School_Staff__c,
        docebo_username_override__c=source.Docebo_Username_Override__c,
        docebo_username__c=source.Docebo_Username__c,
        salesforce_username__c=source.Salesforce_Username__c,
        docebo_pilot_user__c=source.Docebo_Pilot_User__c,
        docebo_is_hq_staff__c=source.Docebo_Is_HQ_Staff__c,
        docebo_disconnect__c=source.Docebo_Disconnect__c,
        docebo_account_active__c=source.Docebo_Account_Active__c,
        master_coach__c=source.Master_Coach__c,
        primary_contact_home_phone__c=source.Primary_Contact_Home_Phone__c,
        user_email__c=source.User_Email__c,
        impact_today_board_members__c=source.Impact_Today_Board_Members__c,
        impact_today_coaches__c=source.Impact_Today_Coaches__c,
        impact_today_leaders__c=source.Impact_Today_Leaders__c,
        nccp__c=source.NCCP__c,
        pursuing_ace__c=source.Pursuing_Ace__c,
        docebo_is_junior_coach__c=source.Docebo_Is_Junior_Coach__c,
        lifetime_gamification_points__c=source.Lifetime_Gamification_Points__c,
        respect_in_sport_certification__c=source.Respect_In_Sport_Certification__c,
        canva_admin__c=source.Canva_Admin__c,
        website_admin__c=source.Website_Admin__c,
        docebo_username_randomly_generated__c=source.Docebo_Username_Randomly_Generated__c,
        dss_ingestion_timestamp=datetime.now()
    )

def map_sf_contact_sources_to_raws(source_list: List[SfContactSourceModel]) -> List[SfContactRawDbModel]:
    """
    Maps a list of SfContactSourceModel objects to a list of SfContactRawDbModel objects.

    Args:
    - source_list: List of SfContactSourceModel instances.

    Returns:
    - List of SfContactRawDbModel instances.
    """
    return [map_sf_contact_source_to_raw(source) for source in source_list]