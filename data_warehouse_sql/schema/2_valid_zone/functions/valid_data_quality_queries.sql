-- number of records in valid
select count(*)
from ft_ds_valid.sf_contact
;

-- number of unique sf_ids in RAW, not including test chapters, which should be number of records in valid
select count(distinct Id)
from ft_ds_raw.sf_contact
WHERE
chapter_affiliation__c NOT IN (
    '0011R00002oM2hNQAS',
    '0013600000xOm3cAAC'
)
;

--locate a record in raw that has had an actual change happen to it
-- include a better way of find this