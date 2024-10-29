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
    IsDeleted: bool
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
    BillingAddress: str
    ShippingStreet: str
    ShippingCity: str
    ShippingState: str
    ShippingPostalCode: str
    ShippingCountry: str
    ShippingAddress: str
    Phone: str
    AccountNumber: str
    Website: str
    NumberOfEmployees: int
    Ownership: str
    Description: str
    OwnerId: str
    CreatedDate: datetime
    CreatedById: str
    LastModifiedDate: datetime
    LastModifiedById: str
    SystemModstamp: datetime
    LastActivityDate: datetime
    LastViewedDate: datetime
    LastReferencedDate: datetime
    IsPartner: bool
    IsCustomerPortal: bool
    ChannelProgramName: str
    ChannelProgramLevelName: str
    Chapter_Administrator__c: str
    Chapter_Affiliation__c: str
    Territory__c: str
    Chapter_ID__c: str
    Membership_Offered__c: bool
    Payments_Accepted_In_Person__c: bool
    Time_Zone__c: str
    Coach_Retention_Rate__c: float
    Count_Account__c: int
    Current_Coach_Retention_Percentage__c: float
    Performance_Record_Type__c: str
    YS_Report_Chapter_Affiliation__c: str
    Insurance_Expires__c: datetime
    Affiliate_Delivery_Partner__c: bool
    Date_Joined__c: datetime
    Equipment__c: str
    Graduate__c: bool
    Home_School__c: bool
    Kindergarten__c: bool
    Account_ID__c: str
    Additional_Trade_NameChap_Affiliation__c: str
    Additional_Trade_Name_Logo__c: str
    Parent_Account_ID_18__c: str
    Account_Inactive_Date__c: datetime
    Legacy_ID__c: str
    EIN__c: str
    Governance_Structure__c: str
    Youth_Population__c: int
    Contract_Effective_Date__c: datetime
    Active__c: bool
    Chapter_Membership_Price__c: float
    Chapter_Standing__c: str
    Contract_Expiration_Date__c: datetime
    Contract_Status__c: str
    County__c: str
    HO_Contracted_Location__c: bool
    Inactive_Date__c: datetime
    Legal_Entity_Name__c: str
    Membership_Active__c: bool
    Membership_Discount_Amount__c: float
    Membership_Discount_Percentage__c: float
    Membership_End_Date__c: datetime
    Membership_Start_Date__c: datetime
    Military_Discount_Amount__c: float
    Military_Discount_Percentage__c: float
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
    Sibling_Discount_Amount__c: float
    Sibling_Discount_Percentage__c: float
    Service_Area__c: str
    Insurance_Expiration_Date__c: datetime
    Former_Trade_Names__c: str
    Former_Legal_Entity__c: str
    Organization_City__c: str
    Organization_State__c: str
    Partner_Program_Type__c: str

@define(kw_only=True)
class SfAccountRawDbModel(DbModel):
    """
    Base model for Raw Salesforce Account
    """
    id: str
    isdeleted: bool
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
    billingaddress: str
    shippingstreet: str
    shippingcity: str
    shippingstate: str
    shippingpostalcode: str
    shippingcountry: str
    shippingaddress: str
    phone: str
    accountnumber: str
    website: str
    numberofemployees: int
    ownership: str
    description: str
    ownerid: str
    createddate: datetime
    createdbyid: str
    lastmodifieddate: datetime
    lastmodifiedbyid: str
    systemmodstamp: datetime
    lastactivitydate: datetime
    lastvieweddate: datetime
    lastreferenceddate: datetime
    ispartner: bool
    iscustomerportal: bool
    channelprogramname: str
    channelprogramlevelname: str
    chapter_administrator__c: str
    chapter_affiliation__c: str
    territory__c: str
    chapter_id__c: str
    membership_offered__c: bool
    payments_accepted_in_person__c: bool
    time_zone__c: str
    coach_retention_rate__c: float
    count_account__c: int
    current_coach_retention_percentage__c: float
    performance_record_type__c: str
    ys_report_chapter_affiliation__c: str
    insurance_expires__c: datetime
    affiliate_delivery_partner__c: bool
    date_joined__c: datetime
    equipment__c: str
    graduate__c: bool
    home_school__c: bool
    kindergarten__c: bool
    account_id__c: str
    additional_trade_namechap_affiliation__c: str
    additional_trade_name_logo__c: str
    parent_account_id_18__c: str
    account_inactive_date__c: datetime
    legacy_id__c: str
    ein__c: str
    governance_structure__c: str
    youth_population__c: int
    contract_effective_date__c: datetime
    active__c: bool
    chapter_membership_price__c: float
    chapter_standing__c: str
    contract_expiration_date__c: datetime
    contract_status__c: str
    county__c: str
    ho_contracted_location__c: bool
    inactive_date__c: datetime
    legal_entity_name__c: str
    membership_active__c: bool
    membership_discount_amount__c: float
    membership_discount_percentage__c: float
    membership_end_date__c: datetime
    membership_start_date__c: datetime
    military_discount_amount__c: float
    military_discount_percentage__c: float
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
    sibling_discount_amount__c: float
    sibling_discount_percentage__c: float
    service_area__c: str
    insurance_expiration_date__c: datetime
    former_trade_names__c: str
    former_legal_entity__c: str
    organization_city__c: str
    organization_state__c: str
    partner_program_type__c: str
    
     # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)

def map_account_source_to_raw(source: SfAccountSourceModel) -> SfAccountRawDbModel:
    """
    Maps an instance of SfListingSourceModel to SfListingRawDbModel.
    """   
    return SfAccountRawDbModel( 
        id=source.Id, 
        isdeleled=source.IsDeleted, 
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
        billingaddress=source.BillingAddress,
        shippingstreet=source.ShippingStreet,
        shippingcity=source.ShippingCity,
        shippingstate=source.ShippingState,
        shippingpostalcode=source.ShippingPostalCode,
        shippingcountry=source.ShippingCountry,
        shippingaddress=source.ShippingAddress,
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
        dss_ingestion_timestamp=datetime.now()  # Capture current timestamp
        )
    
def map_account_sources_to_raws(source_list: List[SfAccountSourceModel]) -> List[SfAccountRawDbModel]:
    """
    Maps a list of SfAccountSourceModel objects to a list of SfAccountRawDbModel objects.

    Args:
    - source_list: List of SfAccountSourceModel instances.

    Returns:
    - List of SfAccountRawDbModel instances.
    """
    return [map_account_source_to_raw(source) for source in source_list]