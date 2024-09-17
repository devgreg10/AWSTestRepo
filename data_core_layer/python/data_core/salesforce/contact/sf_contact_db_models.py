from datetime import datetime
from attrs import define
from data_core.util.db_model import DbModel
from typing import List

import json

@define(kw_only=True)
class SfContactSourceModel():
    """
    Base model for Salesforce Contact from S3
    """
    Id: str 
    Chapter_Affiliation__c: str 
    ChapterID_CONTACT__c: str 
    CASESAFEID__c: str 
    Contact_Type__c: str 
    Age__c: str 
    Ethnicity__c: str 
    Gender__c: str 
    Grade__c: str 
    Participation_Status__c: str 
    MailingPostalCode: str 
    MailingStreet: str 
    MailingCity: str 
    MailingState: str 
    School_Name__c: str 
    School_Name_Other__c: str 
    FirstName: str 
    LastName: str 
    Birthdate: str 
    AccountId: str 
    LastModifiedDate: str 
    IsDeleted: str 
    CreatedDate: str 
    SystemModstamp: str

    # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)

@define(kw_only=True)
class SfContactRawDbModel(DbModel):
    """
    Base model for Raw Salesforce Contact
    """
    id: str 
    mailingpostalcode: str 
    chapter_affiliation__c: str 
    chapterid_contact__c: str 
    casesafeid__c: str 
    contact_type__c: str 
    age__c: str 
    ethnicity__c: str 
    gender__c: str 
    grade__c: str 
    participation_status__c: str 
    isdeleted: str 
    lastmodifieddate: str 
    createddate: str 
    dss_last_modified_timestamp: str 

    # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)



def map_source_to_raw(source: SfContactSourceModel) -> SfContactRawDbModel:
    """
    Maps an instance of SfContactSourceModel to SfContactRawDbModel.
    """
    return SfContactRawDbModel(
        Id=source.Id,
        Chapter_Affiliation__c=source.Chapter_Affiliation__c,
        ChapterID_CONTACT__c=source.ChapterID_CONTACT__c,
        CASESAFEID__c=source.CASESAFEID__c,
        Contact_Type__c=source.Contact_Type__c,
        Age__c=source.Age__c,
        Ethnicity__c=source.Ethnicity__c,
        Gender__c=source.Gender__c,
        Grade__c=source.Grade__c,
        Participation_Status__c=source.Participation_Status__c,
        MailingPostalCode=source.MailingPostalCode,
        MailingStreet=source.MailingStreet,
        MailingCity=source.MailingCity,
        MailingState=source.MailingState,
        School_Name__c=source.School_Name__c,
        School_Name_Other__c=source.School_Name_Other__c,
        FirstName=source.FirstName,
        LastName=source.LastName,
        Birthdate=source.Birthdate,
        AccountId=source.AccountId,
        LastModifiedDate=source.LastModifiedDate,
        IsDeleted=source.IsDeleted,
        CreatedDate=source.CreatedDate,
        SystemModstamp=source.SystemModstamp,
    )

def map_sources_to_raws(source_list: List[SfContactSourceModel]) -> List[SfContactRawDbModel]:
    """
    Maps a list of SfContactSourceModel objects to a list of SfContactRawDbModel objects.

    Args:
    - source_list: List of SfContactSourceModel instances.

    Returns:
    - List of SfContactRawDbModel instances.
    """
    return [map_source_to_raw(source) for source in source_list]