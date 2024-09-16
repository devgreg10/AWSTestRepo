--planning on removing this function. It will be stored in Python
CREATE OR REPLACE FUNCTION ft_ds_admin.write_sf_contact_s3_to_raw (
    id ft_ds_raw.sf_contact.Id%TYPE,
    mailing_postal_code ft_ds_raw.sf_contact.MailingPostalCode%TYPE,
    chapter_affiliation ft_ds_raw.sf_contact.Chapter_Affiliation__c%TYPE,
    chapter_id_contact ft_ds_raw.sf_contact.ChapterID_CONTACT__c%TYPE,
    casesafe_id ft_ds_raw.sf_contact.CASESAFEID__c%TYPE,
    contact_type ft_ds_raw.sf_contact.Contact_Type__c%TYPE,
    age ft_ds_raw.sf_contact.Age__c%TYPE,
    ethnicity ft_ds_raw.sf_contact.Ethnicity__c%TYPE,
    gender ft_ds_raw.sf_contact.Gender__c%TYPE,
    grade ft_ds_raw.sf_contact.Grade__c%TYPE,
    participation_status ft_ds_raw.sf_contact.Participation_Status__c%TYPE,
    is_deleted ft_ds_raw.sf_contact.IsDeleted%TYPE,
    last_modified_date ft_ds_raw.sf_contact.LastModifiedDate%TYPE,
    created_date ft_ds_raw.sf_contact.CreatedDate%TYPE,
    dss_last_modified_timestamp ft_ds_raw.sf_contact.dss_last_modified_timestamp%TYPE
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_raw.sf_contact
    VALUES (
        id,
        mailing_postal_code,
        chapter_affiliation, 
        chapter_id_contact,
        casesafe_id,
        contact_type,
        age, 
        ethnicity,
        gender,
        grade,
        participation_status, 
        is_deleted,
        last_modified_date,
        created_date,
        dss_last_modified_timestamp
    )
    ON CONFLICT (Id, LastModifiedDate) DO UPDATE SET
        MailingPostalCode = EXCLUDED.MailingPostalCode,
        Chapter_Affiliation__c = EXCLUDED.Chapter_Affiliation__c,
        ChapterID_CONTACT__c = EXCLUDED.ChapterID_CONTACT__c,
        CASESAFEID__c = EXCLUDED.CASESAFEID__c,
        Contact_Type__c = EXCLUDED.Contact_Type__c,
        Age__c = EXCLUDED.Age__c,
        Ethnicity__c = EXCLUDED.Ethnicity__c,
        Gender__c = EXCLUDED.Gender__c,
        Grade__c = EXCLUDED.Grade__c,
        Participation_Status__c = EXCLUDED.Participation_Status__c,
        IsDeleted = EXCLUDED.IsDeleted,
        CreatedDate = EXCLUDED.CreatedDate,
        dss_last_modified_timestamp = EXCLUDED.dss_last_modified_timestamp
        ;
END;
$$;