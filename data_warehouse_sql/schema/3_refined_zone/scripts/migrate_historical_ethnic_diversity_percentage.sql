--this script is meant to be run manually and will include several SQL statements that can be executed in a manner determined by the person executing them, with some guidance from the comments left
--it is specifically designed to expand the historical_active_participant_counts table in refined to include an eoy_indicator field that indicates if a given record is the final record for a given year, and insert the EOY records for the calculation from the historical data

--create a temp table to store the data as it is while we drop and recreate the original table with the new column added
CREATE TEMP TABLE temp_historical_ethnic_diversity_percentage AS
SELECT
    *
FROM ft_ds_refined.metric_historical_ethnic_diversity_percentage
;

--drop the table so we can add a new column
--you will also have to drop the downstream views
DROP TABLE ft_ds_refined.metric_historical_ethnic_diversity_percentage;

--recreate the downstream views

--recreate the table with the new column
--copied from /Users/hschuster/FT-decision-support-cloud/data_warehouse_sql/schema/3_refined_zone/tables/refined_historical_active_participant_counts_table.sql
CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_ethnic_diversity_percentage (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    ethnic_diversity_percentage NUMERIC(5,2),
    eoy_indicator VARCHAR(20)
);

--insert old data back into newly recreated table
INSERT INTO ft_ds_refined.metric_historical_ethnic_diversity_percentage
SELECT
    *
FROM temp_historical_ethnic_diversity_percentage
;

--insert historical data into table with eoy_indicator field
INSERT INTO ft_ds_refined.metric_historical_ethnic_diversity_percentage 
SELECT
    TO_DATE(year::TEXT || '-12-31', 'YYYY-MM-DD') AS metric_calc_date,
    chapter_id,
    (COUNT(CASE WHEN ethnicity NOT IN ('White or Caucasian', 'Prefer not to respond') THEN 1 END) * 1.0)/
    NULLIF(COUNT(CASE WHEN ethnicity <> 'Prefer not to respond' THEN 1 END), 0)
    AS ethnic_diversity_percentage,
    year::TEXT AS eoy_indicator
FROM ft_ds_refined.current_and_historical_participants_view 
WHERE year != CAST(EXTRACT(YEAR FROM NOW()) AS INTEGER)
GROUP BY year, chapter_id
ON CONFLICT (metric_calc_date, chapter_id) DO UPDATE SET
        ethnic_diversity_percentage = EXCLUDED.ethnic_diversity_percentage,
        eoy_indicator = EXCLUDED.eoy_indicator
;

--update table to have the EOY indicator for this current year be correct
UPDATE
    ft_ds_refined.metric_historical_active_participant_counts
SET
    eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
WHERE
    metric_calc_date = (
        SELECT
            MAX(metric_calc_date)
        FROM ft_ds_refined.metric_historical_active_participant_counts
    )
;