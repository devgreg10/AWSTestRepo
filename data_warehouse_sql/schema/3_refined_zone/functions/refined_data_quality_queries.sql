-- number of records in refined
select count(*)
from ft_ds_refined.contact
;

-- number of unique sf_ids in valid, not including those deleted
select count(distinct contact_id_18)
from ft_ds_valid.sf_contact
WHERE
is_deleted = FALSE
;