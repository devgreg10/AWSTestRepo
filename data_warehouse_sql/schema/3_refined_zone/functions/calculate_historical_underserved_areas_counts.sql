CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_underserved_areas_counts()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_refined.metric_historical_underserved_areas_counts
    SELECT
        NOW() as metric_calc_date,
        participants.chapter_id,
        COUNT(*) AS count_in_underserved_areas
    FROM ft_ds_refined.active_participants_view participants
    JOIN
    ft_ds_refined.dci_reference_data dci
    ON
    LEFT(participants.mailing_zip_postal_code, 5) = LEFT(dci.zip_code, 5)
    WHERE dci.dci_score IN (4, 5)
    GROUP BY
    participants.chapter_id
    ;
END;
$$;