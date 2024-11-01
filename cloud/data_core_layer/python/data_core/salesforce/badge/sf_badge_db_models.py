from datetime import datetime
from attrs import define
from data_core.util.db_model import DbModel
from typing import List

@define(kw_only=True)
class SfBadgeSourceModel():
    """
    Base model for Salesforce badge from S3
    """
    Id: str
    OwnerId: str
    IsDeleted: str
    Name: str
    CreatedDate: str
    CreatedById: str
    LastModifiedDate: str
    LastModifiedById: str     
    SystemModstamp: str
    LastViewedDate: str
    LastReferencedDate: str
    Description: str    
    Category__c: str
    Badge_Type__c: str
    Coaches_App_Image_Id__c: str
    Coaches_App_Image_Url__c: str
    Is_Active__c: str
    Parent_Registration_Image_Id__c: str
    Parent_Registration_Image_Url__c: str
    Points__c: str
    Sort_Order__c: str
    badge_id__c: str
    Age_Group__c:str
   

    
@define(kw_only=True)
class SfBadgeRawDbModel(DbModel):
    """
    Base model for Raw Salesforce badge
    """
    id: str
    ownerid: str
    isdeleted: str
    name: str
    createddate: str
    createdbyid: str
    lastmodifieddate: str
    lastmodifiedbyid: str
    systemmodstamp: str
    lastvieweddate: str
    lastreferenceddate: str
    description: str
    category__c: str
    badgetype__c: str
    coachesappimageid__c: str
    coachesappimageurl__c: str
    isactive__c: str
    parentregistrationimageid__c: str
    parentregistrationimageurl__c: str
    points__c: str
    sortorder__c: str
    badgeid__c: str
    age_group__c: str
    dss_ingestion_timestamp: datetime
    
    # extend the DbModel to ignore extra arguments not included in the model that are passed in by the database view
    def __init__(self, **kwargs):
        """
        Overriding init method
        """
        DbModel.__init__(self, **kwargs)


def map_badge_source_to_raw(source: SfBadgeSourceModel) -> SfBadgeRawDbModel:
  
    return SfBadgeRawDbModel(
        id=source.Id,
        ownerId=source.OwnerId,
        isdeleted=source.IsDeleted,
        name=source.Name,
        createddate=source.CreatedDate,
        createdbyid=source.CreatedById,
        lastmodifieddate=source.LastModifiedDate,
        lastmodifiedbyid=source.LastModifiedById,
        systemmodstamp=source.SystemModstamp,
        lastvieweddate=source.LastViewedDate,
        lastreferenceddate=source.LastReferencedDate,
        description=source.Description,
        category__c=source.Category__c,
        badge_type__c=source.Badge_Type__c,
        coaches_app_image_id__c=source.Coaches_App_Image_Id__c,
        coaches_app_image_url__c=source.Coaches_App_Image_Url__c,
        is_active__c=source.Is_Active__c,
        parent_registration_image_id__c=source.Parent_Registration_Image_Id__c,
        parent_registration_image_url__c=source.Parent_Registration_Image_Url__c,
        points__c=source.Points__c,
        sort_order__c=source.Sort_Order__c,
        badge_id__c=source.badge_id__c,
        age_group__c=source.Age_Group__c, 
        dss_ingestion_timestamp=datetime.now()   
        )
def map_badge_sources_to_raws(source_list:List [SfBadgeSourceModel]) -> List[SfBadgeRawDbModel]: 
    """"
    Maps a list of SfBadgeSourceModel objects to a list of SfBadgeRawDbModel objects.
    Args:
    - source_list: List of SfBadgeSourceModel instances.

    Returns:
    - List of SfBadgeRawDbModel instances.
    """
    return [map_badge_source_to_raw(source) for source in source_list]
