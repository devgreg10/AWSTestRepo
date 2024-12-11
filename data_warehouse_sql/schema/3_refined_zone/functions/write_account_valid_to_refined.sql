CREATE OR REPLACE FUNCTION ft_ds_admin.write_account_valid_to_refined()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE ft_ds_refined.account;

    INSERT INTO ft_ds_refined.account
    SELECT
        *
    FROM ft_ds_valid.sf_account
    WHERE
        is_deleted = false
    AND
        (
            --this filter handles accounts of the record type = 'Household' and 'Partner Organization'
            --while partner orgs can have parent partner orgs, all partner orgs in prod associated with the testing chapters have no children partner orgs
            chapter_affiliation_id NOT IN (
                '0011R00002oM2hNQAS',
                '0013600000xOm3cAAC'
            )
            --this filter handles accounts of the record type = 'Chapter', aka the testing chapters themselves
            OR
            account_id NOT IN (
                '0011R00002oM2hNQAS',
                '0013600000xOm3cAAC'
            )
            --this filter handles accounts of the record type = 'Program Location'
            --you could also use the parent_account_id, <- this field is a formula based on parent_account in salesforce
            OR
            parent_account NOT IN (
                '0011R00002oM2hNQAS',
                '0013600000xOm3cAAC'
            )
        )
    ;
END;
$$;