-- --sf_metric_historical_active_participants_counts various data models
-- --
-- CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_active_participants_counts (
--     snapshot_date TIMESTAMPTZ,
--     participant_count INTEGER
-- );

-- --this can also be string to keep it more flexible
-- CREATE TYPE intl_status_dtype as ENUM (
--     'International',
--     'Domestic'
-- );
CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_active_participants_counts (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    participant_count INTEGER
);

-- CREATE TABLE IF NOT EXISTS ft_ds_refined.sf_metric_historical_active_participants_counts (
--     snapshot_date TIMESTAMPTZ,
--     international_participant_count INTEGER,
--     domestic_participant_count INTEGER
-- );

CREATE OR REPLACE PROCEDURE ft_ds_admin.historical_metric_sf_contact_counts_by_chapter()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_refined.sf_metric_historical_active_participants_counts
    SELECT
        NOW() as metric_calc_date,
        chapter_id,
        COUNT(contact_id_18) AS participant_count
    FROM ft_ds_refined.active_participants_view
    GROUP BY
    chapter_id
    ;
END;
$$;