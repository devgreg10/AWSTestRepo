from datetime import datetime
from attrs import define
from data_core.util.db_model import DbModel

from dataclasses import dataclass, field
from dataclasses_json import config, dataclass_json, LetterCase, Undefined

import json

@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)
@dataclass
class SfContactRawDbModel(DbModel):
    """
    Base model for Raw Salesforce Contact
    """
    dss_last_modified_timestamp: str
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

    # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)

