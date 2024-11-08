from datetime import datetime
from attrs import define
from data_core.util.db_model import DbModel
from typing import List

@define(kw_only=True)
class SfEarnedBadgeSourceModel():
    """
    Base model for Salesforce earned_badge from S3
    """
    Id: str
    IsDeleted: str
    Name: str
    CreatedDate: str
    CreatedById: str
    LastModifiedDate: str
    LastModifiedById: str
    SystemModstamp: str
    Contact__c: str
    Badge__c: str
    Id__c: str
    Date_Earned__c: str
    Listing_Session__c: str
    Pending_AWS_Callout__c: str
    Points__c: str
    Source_System__c: str   

    
@define(kw_only=True)
class SfEarnedBadgeRawDbModel(DbModel):
    """
    Base model for Raw Salesforce earned_badge
    """
    id: str
    isdeleted: str
    name: str
    createddate: str
    createdbyid: str
    lastmodifieddate: str
    lastmodifiedbyid: str
    systemmodstamp: str
    contact__c: str
    badge__c: str
    id__c: str
    date_earned__c: str
    listing_session__c: str
    pending_aws_callout__c: str
    points__c: str
    source_system__c: str    
    dss_ingestion_timestamp: datetime
    
    # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)


def map_earned_badge_source_to_raw(source: SfEarnedBadgeSourceModel) -> SfEarnedBadgeRawDbModel:
  
    return SfEarnedBadgeRawDbModel(
        id=source.Id,
        isdeleted=source.IsDeleted,
        name=source.Name,
        createddate=source.CreatedDate,
        createdbyid=source.CreatedById,
        lastmodifieddate=source.LastModifiedDate,
        lastmodifiedbyid=source.LastModifiedById,
        systemmodstamp=source.SystemModstamp,
        contact__c=source.Contact__c,
        badge__c=source.Badge__c,
        id__c=source.Id__c,
        date_earned__c=source.Date_Earned__c,
        listing_session__c=source.Listing_Session__c,
        pending_aws_callout__c=source.Pending_AWS_Callout__c,
        points__c=source.Points__c,
        source_system__c=source.Source_System__c,
        dss_ingestion_timestamp=datetime.now()   
        )
def map_earned_badge_sources_to_raws(source_list:List[SfEarnedBadgeSourceModel]) -> List [SfEarnedBadgeRawDbModel]: 
    """"
    Maps a list of SfEarnedBadgeSourceModel objects to a list of SfEarnedBadgeRawDbModel objects.
    Args:
    - source_list: List of SfEarnedBadgeSourceModel instances.

    Returns:
    - List of SfEarnedBadgeRawDbModel instances.
    """
    return [map_earned_badge_source_to_raw(source) for source in source_list]
