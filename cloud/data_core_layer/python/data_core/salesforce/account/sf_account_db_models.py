from datetime import datetime
from attrs import define
from data_core.util.db_model import DbModel
from typing import List

    
@define(kw_only=True)
class SfAccountSourceModel():
    """
    Base model for Salesforce Account from S3
    """
    Id: str
    IsDeleted: str
    MasterRecordId: str
    Name: str
    Type: str
    RecordTypeId: str
    ParentId: str
    BillingStreet: str
    BillingCity: str
    BillingState: str
    BillingPostalCode: str
    BillingCountry: str
    ShippingStreet: str
    ShippingCity: str
    ShippingState: str
    ShippingPostalCode: str
    ShippingCountry: str
    Phone: str
    AccountNumber: str
    Website: str
    NumberOfEmployees: str
    Ownership: str
    Description: str
    OwnerId: str
    CreatedDate: str
    CreatedById: str
    LastModifiedDate: str
    LastModifiedById: str
    SystemModstamp: str
    LastActivityDate: str
    LastViewedDate: str
    LastReferencedDate: str
    IsPartner: str
    IsCustomerPortal: str
    ChannelProgramName: str
    ChannelProgramLevelName: str
    Chapter_Administrator__c: str
    Chapter_Affiliation__c: str
    Territory__c: str
    Chapter_ID__c: str
    Membership_Offered__c: str
    Payments_Accepted_In_Person__c: str
    Time_Zone__c: str
    Coach_Retention_Rate__c: str
    Count_Account__c: str
    Current_Coach_Retention_Percentage__c: str
    Performance_Record_Type__c: str
    YS_Report_Chapter_Affiliation__c: str
    Insurance_Expires__c: str
    Affiliate_Delivery_Partner__c: str
    Date_Joined__c: str
    Equipment__c: str
    Graduate__c: str
    Home_School__c: str
    Kindergarten__c: str
    Account_ID__c: str
    Additional_Trade_NameChap_Affiliation__c: str
    Additional_Trade_Name_Logo__c: str
    Parent_Account_ID_18__c: str
    Account_Inactive_Date__c: str
    Legacy_ID__c: str
    EIN__c: str
    Governance_Structure__c: str
    Youth_Population__c: str
    Contract_Effective_Date__c: str
    Active__c: str
    Chapter_Membership_Price__c: str
    Chapter_Standing__c: str
    Contract_Expiration_Date__c: str
    Contract_Status__c: str
    County__c: str
    HO_Contracted_Location__c: str
    Inactive_Date__c: str
    Legal_Entity_Name__c: str
    Membership_Active__c: str
    Membership_Discount_Amount__c: str
    Membership_Discount_Percentage__c: str
    Membership_End_Date__c: str
    Membership_Start_Date__c: str
    Military_Discount_Amount__c: str
    Military_Discount_Percentage__c: str
    Primary_Contact_Email__c: str
    Primary_Contact__c: str
    Primary_Contact_email_address__c: str
    Program_Location_Key__c: str
    Program_Location_Type__c: str
    Reggie_Account_Id__c: str
    Reggie_Id__c: str
    Reggie_Location_Id__c: str
    Reggie_Name__c: str
    Region__c: str
    Secondary_Contact__c: str
    Location_Type__c: str
    Sibling_Discount_Amount__c: str
    Sibling_Discount_Percentage__c: str
    Service_Area__c: str
    Insurance_Expiration_Date__c: str
    Former_Trade_Names__c: str
    Former_Legal_Entity__c: str
    Organization_City__c: str
    Organization_State__c: str
    Partner_Program_Type__c: str
    Pre_School__c: str
    Status__c: str
    Title_I__c: str
    Enrollment__c: str
    Market_Size__c: str
    DCR__c: str
    Owner_ID__c: str
    Program_Director__c: str
    Executive_Director__c: str
    Board_Chair__c: str
    MDR_PID__c: str
    Partner_Program_Type_Validation_Check__c: str
    Duplicates_Removed__c: str
    NCES_ID__c: str
    Currency__c: str
    Territory_Partner_Acct__c: str
    Chapter_Country__c: str
    Financial_Aid_Active__c: str
    New_Parent_Registration__c: str
    Additional_Trade_Name__c: str
    FT_App_Pilot__c: str
    International_Chapter__c: str
    Customer_ID__c: str
    Award_Badges_Enabled__c: str
    Peer_Group__c: str
    In_Market_Partners__c: str
    How_many_holes__c: str
    If_other_fill_in_how_many__c: str
    Golf_Course_Type__c: str
    Please_describe_discount__c: str
    Chapter_owns_this_facility__c: str
    If_yes_how_long_is_the_lease__c: str
    Who_is_the_lease_with__c: str
    Notes_about_the_partnership__c: str
    Please_describe_the_learning_center__c: str
    dedicated_First_Tee_Learning_Center__c: str
    Discounts_offered_to_Participants__c: str
    Chapter_Operates_the_facility_through_a__c: str


@define(kw_only=True)
class SfAccountRawDbModel(DbModel):
    """
    Base model for Raw Salesforce Account
    """
    id: str
    isdeleted: str
    masterrecordid: str
    name: str
    type: str
    recordtypeid: str
    parentid: str
    billingstreet: str
    billingcity: str
    billingstate: str
    billingpostalcode: str
    billingcountry: str
    shippingstreet: str
    shippingcity: str
    shippingstate: str
    shippingpostalcode: str
    shippingcountry: str
    phone: str
    accountnumber: str
    website: str
    numberofemployees: str
    ownership: str
    description: str
    ownerid: str
    createddate: str
    createdbyid: str
    lastmodifieddate: str
    lastmodifiedbyid: str
    systemmodstamp: str
    lastactivitydate: str
    lastvieweddate: str
    lastreferenceddate: str
    ispartner: str
    iscustomerportal: str
    channelprogramname: str
    channelprogramlevelname: str
    chapter_administrator__c: str
    chapter_affiliation__c: str
    territory__c: str
    chapter_id__c: str
    membership_offered__c: str
    payments_accepted_in_person__c: str
    time_zone__c: str
    coach_retention_rate__c: str
    count_account__c: str
    current_coach_retention_percentage__c: str
    performance_record_type__c: str
    ys_report_chapter_affiliation__c: str
    insurance_expires__c: str
    affiliate_delivery_partner__c: str
    date_joined__c: str
    equipment__c: str
    graduate__c: str
    home_school__c: str
    kindergarten__c: str
    account_id__c: str
    additional_trade_namechap_affiliation__c: str
    additional_trade_name_logo__c: str
    parent_account_id_18__c: str
    account_inactive_date__c: str
    legacy_id__c: str
    ein__c: str
    governance_structure__c: str
    youth_population__c: str
    contract_effective_date__c: str
    active__c: str
    chapter_membership_price__c: str
    chapter_standing__c: str
    contract_expiration_date__c: str
    contract_status__c: str
    county__c: str
    ho_contracted_location__c: str
    inactive_date__c: str
    legal_entity_name__c: str
    membership_active__c: str
    membership_discount_amount__c: str
    membership_discount_percentage__c: str
    membership_end_date__c: str
    membership_start_date__c: str
    military_discount_amount__c: str
    military_discount_percentage__c: str
    primary_contact_email__c: str
    primary_contact__c: str
    primary_contact_email_address__c: str
    program_location_key__c: str
    program_location_type__c: str
    reggie_account_id__c: str
    reggie_id__c: str
    reggie_location_id__c: str
    reggie_name__c: str
    region__c: str
    secondary_contact__c: str
    location_type__c: str
    sibling_discount_amount__c: str
    sibling_discount_percentage__c: str
    service_area__c: str
    insurance_expiration_date__c: str
    former_trade_names__c: str
    former_legal_entity__c: str
    organization_city__c: str
    organization_state__c: str
    partner_program_type__c: str
    pre_school__c: str
    status__c: str
    title_i__c: str
    enrollment__c: str
    market_size__c: str
    dcr__c: str
    owner_id__c: str
    program_director__c: str
    executive_director__c: str
    board_chair__c: str
    mdr_pid__c: str
    partner_program_type_validation_check__c: str
    duplicates_removed__c: str
    nces_id__c: str
    currency__c: str
    territory_partner_acct__c: str
    chapter_country__c: str
    financial_aid_active__c: str
    new_parent_registration__c: str
    additional_trade_name__c: str
    ft_app_pilot__c: str
    international_chapter__c: str
    customer_id__c: str
    award_badges_enabled__c: str
    peer_group__c: str
    in_market_partners__c: str
    how_many_holes__c: str
    if_other_fill_in_how_many__c: str
    golf_course_type__c: str
    please_describe_discount__c: str
    chapter_owns_this_facility__c: str
    if_yes_how_long_is_the_lease__c: str
    who_is_the_lease_with__c: str
    notes_about_the_partnership__c: str
    please_describe_the_learning_center__c: str
    dedicated_first_tee_learning_center__c: str
    discounts_offered_to_participants__c: str
    chapter_operates_the_facility_through_a__c: str
    dss_ingestion_timestamp: datetime

     # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)

def map_sf_account_source_to_raw(source: SfAccountSourceModel) -> SfAccountRawDbModel:
    """
    Maps an instance of SfListingSourceModel to SfListingRawDbModel.
    """   
    return SfAccountRawDbModel( 
        id=source.Id,
        isdeleted=source.IsDeleted,
        masterrecordid=source.MasterRecordId,
        name=source.Name,
        type=source.Type,
        recordtypeid=source.RecordTypeId,
        parentid=source.ParentId,
        billingstreet=source.BillingStreet,
        billingcity=source.BillingCity,
        billingstate=source.BillingState,
        billingpostalcode=source.BillingPostalCode,
        billingcountry=source.BillingCountry,
        shippingstreet=source.ShippingStreet,
        shippingcity=source.ShippingCity,
        shippingstate=source.ShippingState,
        shippingpostalcode=source.ShippingPostalCode,
        shippingcountry=source.ShippingCountry,
        phone=source.Phone,
        accountnumber=source.AccountNumber,
        website=source.Website,
        numberofemployees=source.NumberOfEmployees,
        ownership=source.Ownership,
        description=source.Description,
        ownerid=source.OwnerId,
        createddate=source.CreatedDate,
        createdbyid=source.CreatedById,
        lastmodifieddate=source.LastModifiedDate,
        lastmodifiedbyid=source.LastModifiedById,
        systemmodstamp=source.SystemModstamp,
        lastactivitydate=source.LastActivityDate,
        lastvieweddate=source.LastViewedDate,
        lastreferenceddate=source.LastReferencedDate,
        ispartner=source.IsPartner,
        iscustomerportal=source.IsCustomerPortal,
        channelprogramname=source.ChannelProgramName,
        channelprogramlevelname=source.ChannelProgramLevelName,
        chapter_administrator__c=source.Chapter_Administrator__c,
        chapter_affiliation__c=source.Chapter_Affiliation__c,
        territory__c=source.Territory__c,
        chapter_id__c=source.Chapter_ID__c,
        membership_offered__c=source.Membership_Offered__c,
        payments_accepted_in_person__c=source.Payments_Accepted_In_Person__c,
        time_zone__c=source.Time_Zone__c,
        coach_retention_rate__c=source.Coach_Retention_Rate__c,
        count_account__c=source.Count_Account__c,
        current_coach_retention_percentage__c=source.Current_Coach_Retention_Percentage__c,
        performance_record_type__c=source.Performance_Record_Type__c,
        ys_report_chapter_affiliation__c=source.YS_Report_Chapter_Affiliation__c,
        insurance_expires__c=source.Insurance_Expires__c,
        affiliate_delivery_partner__c=source.Affiliate_Delivery_Partner__c,
        date_joined__c=source.Date_Joined__c,
        equipment__c=source.Equipment__c,
        graduate__c=source.Graduate__c,
        home_school__c=source.Home_School__c,
        kindergarten__c=source.Kindergarten__c,
        account_id__c=source.Account_ID__c,
        additional_trade_namechap_affiliation__c=source.Additional_Trade_NameChap_Affiliation__c,
        additional_trade_name_logo__c=source.Additional_Trade_Name_Logo__c,
        parent_account_id_18__c=source.Parent_Account_ID_18__c,
        account_inactive_date__c=source.Account_Inactive_Date__c,
        legacy_id__c=source.Legacy_ID__c,
        ein__c=source.EIN__c,
        governance_structure__c=source.Governance_Structure__c,
        youth_population__c=source.Youth_Population__c,
        contract_effective_date__c=source.Contract_Effective_Date__c,
        active__c=source.Active__c,
        chapter_membership_price__c=source.Chapter_Membership_Price__c,
        chapter_standing__c=source.Chapter_Standing__c,
        contract_expiration_date__c=source.Contract_Expiration_Date__c,
        contract_status__c=source.Contract_Status__c,
        county__c=source.County__c,
        ho_contracted_location__c=source.HO_Contracted_Location__c,
        inactive_date__c=source.Inactive_Date__c,
        legal_entity_name__c=source.Legal_Entity_Name__c,
        membership_active__c=source.Membership_Active__c,
        membership_discount_amount__c=source.Membership_Discount_Amount__c,
        membership_discount_percentage__c=source.Membership_Discount_Percentage__c,
        membership_end_date__c=source.Membership_End_Date__c,
        membership_start_date__c=source.Membership_Start_Date__c,
        military_discount_amount__c=source.Military_Discount_Amount__c,
        military_discount_percentage__c=source.Military_Discount_Percentage__c,
        primary_contact_email__c=source.Primary_Contact_Email__c,
        primary_contact__c=source.Primary_Contact__c,
        primary_contact_email_address__c=source.Primary_Contact_email_address__c,
        program_location_key__c=source.Program_Location_Key__c,
        program_location_type__c=source.Program_Location_Type__c,
        reggie_account_id__c=source.Reggie_Account_Id__c,
        reggie_id__c=source.Reggie_Id__c,
        reggie_location_id__c=source.Reggie_Location_Id__c,
        reggie_name__c=source.Reggie_Name__c,
        region__c=source.Region__c,
        secondary_contact__c=source.Secondary_Contact__c,
        location_type__c=source.Location_Type__c,
        sibling_discount_amount__c=source.Sibling_Discount_Amount__c,
        sibling_discount_percentage__c=source.Sibling_Discount_Percentage__c,
        service_area__c=source.Service_Area__c,
        insurance_expiration_date__c=source.Insurance_Expiration_Date__c,
        former_trade_names__c=source.Former_Trade_Names__c,
        former_legal_entity__c=source.Former_Legal_Entity__c,
        organization_city__c=source.Organization_City__c,
        organization_state__c=source.Organization_State__c,
        partner_program_type__c=source.Partner_Program_Type__c,
        pre_school__c=source.Pre_School__c,
        status__c=source.Status__c,
        title_i__c=source.Title_I__c,
        enrollment__c=source.Enrollment__c,
        market_size__c=source.Market_Size__c,
        dcr__c=source.DCR__c,
        owner_id__c=source.Owner_ID__c,
        program_director__c=source.Program_Director__c,
        executive_director__c=source.Executive_Director__c,
        board_chair__c=source.Board_Chair__c,
        mdr_pid__c=source.MDR_PID__c,
        partner_program_type_validation_check__c=source.Partner_Program_Type_Validation_Check__c,
        duplicates_removed__c=source.Duplicates_Removed__c,
        nces_id__c=source.NCES_ID__c,
        currency__c=source.Currency__c,
        territory_partner_acct__c=source.Territory_Partner_Acct__c,
        chapter_country__c=source.Chapter_Country__c,
        financial_aid_active__c=source.Financial_Aid_Active__c,
        new_parent_registration__c=source.New_Parent_Registration__c,
        additional_trade_name__c=source.Additional_Trade_Name__c,
        ft_app_pilot__c=source.FT_App_Pilot__c,
        international_chapter__c=source.International_Chapter__c,
        customer_id__c=source.Customer_ID__c,
        award_badges_enabled__c=source.Award_Badges_Enabled__c,
        peer_group__c=source.Peer_Group__c,
        in_market_partners__c=source.In_Market_Partners__c,
        how_many_holes__c=source.How_many_holes__c,
        if_other_fill_in_how_many__c=source.If_other_fill_in_how_many__c,
        golf_course_type__c=source.Golf_Course_Type__c,
        please_describe_discount__c=source.Please_describe_discount__c,
        chapter_owns_this_facility__c=source.Chapter_owns_this_facility__c,
        if_yes_how_long_is_the_lease__c=source.If_yes_how_long_is_the_lease__c,
        who_is_the_lease_with__c=source.Who_is_the_lease_with__c,
        notes_about_the_partnership__c=source.Notes_about_the_partnership__c,
        please_describe_the_learning_center__c=source.Please_describe_the_learning_center__c,
        dedicated_first_tee_learning_center__c=source.dedicated_First_Tee_Learning_Center__c,
        discounts_offered_to_participants__c=source.Discounts_offered_to_Participants__c,
        chapter_operates_the_facility_through_a__c=source.Chapter_Operates_the_facility_through_a__c,
        dss_ingestion_timestamp=datetime.now()  # Capture current timestamp
        )
    
def map_sf_account_sources_to_raws(source_list: List[SfAccountSourceModel]) -> List[SfAccountRawDbModel]:
    """
    Maps a list of SfAccountSourceModel objects to a list of SfAccountRawDbModel objects.

    Args:
    - source_list: List of SfAccountSourceModel instances.

    Returns:
    - List of SfAccountRawDbModel instances.
    """
    return [map_sf_account_source_to_raw(source) for source in source_list]