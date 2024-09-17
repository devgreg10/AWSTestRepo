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

@define(kw_only=True)
class SfContactRawDbModel(DbModel):
    """
    Base model for Raw Salesforce Contact
    """
    id: str
    chapter_affiliation__c: str
    chapterid_contact__c: str
    casesafeid__c: str
    contact_type__c: str
    age__c: str
    ethnicity__c: str
    gender__c: str
    grade__c: str
    participation_status__c: str
    mailingpostalcode: str
    mailingstreet: str
    mailingcity: str
    mailingstate: str
    school_name__c: str
    school_name_other__c: str
    firstname: str
    lastname: str
    birthdate: str
    accountid: str
    lastmodifieddate: str
    isdeleted: str
    createddate: str
    systemmodstamp: str
    dss_ingestion_timestamp: datetime

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
        id=source.Id,
        chapter_affiliation__c=source.Chapter_Affiliation__c,
        chapterid_contact__c=source.ChapterID_CONTACT__c,
        casesafeid__c=source.CASESAFEID__c,
        contact_type__c=source.Contact_Type__c,
        age__c=source.Age__c,
        ethnicity__c=source.Ethnicity__c,
        gender__c=source.Gender__c,
        grade__c=source.Grade__c,
        participation_status__c=source.Participation_Status__c,
        mailingpostalcode=source.MailingPostalCode,
        mailingstreet=source.MailingStreet,
        mailingcity=source.MailingCity,
        mailingstate=source.MailingState,
        school_name__c=source.School_Name__c,
        school_name_other__c=source.School_Name_Other__c,
        firstname=source.FirstName,
        lastname=source.LastName,
        birthdate=source.Birthdate,
        accountid=source.AccountId,
        lastmodifieddate=source.LastModifiedDate,
        isdeleted=source.IsDeleted,
        createddate=source.CreatedDate,
        systemmodstamp=source.SystemModstamp,
        dss_ingestion_timestamp=datetime.now()
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