--this script is meant to be run manually and will include several SQL statements that can be executed in a manner determined by the person executing them, with some guidance from the comments left
--it is specifically designed to expand the historical_retention_percentage table in refined to include an eoy_indicator field that indicates if a given record is the final record for a given year, and insert the EOY records for the calculation from the historical data

--create a temp table to store the data as it is while we drop and recreate the original table with the new column added
CREATE TEMP TABLE temp_historical_retention_percentage AS
SELECT
    *
FROM ft_ds_refined.metric_historical_retention_percentage
;

--drop the table so we can add a new column
--you will also have to drop the downstream views
DROP TABLE ft_ds_refined.metric_historical_retention_percentage;

--recreate the downstream views

--recreate the table with the new column
--copied from /Users/hschuster/FT-decision-support-cloud/data_warehouse_sql/schema/3_refined_zone/tables/refined_historical_retention_percentage_table.sql
CREATE TABLE IF NOT EXISTS ft_ds_refined.metric_historical_retention_percentage (
    PRIMARY KEY(metric_calc_date, chapter_id),
    metric_calc_date TIMESTAMPTZ,
    chapter_id CHAR(18),
    retention_percentage NUMERIC(5,2),
    eoy_indicator VARCHAR(20)
);

--insert old data back into newly recreated table
INSERT INTO ft_ds_refined.metric_historical_retention_percentage
SELECT
    *
FROM temp_historical_retention_percentage
;

--insert historical data into table with eoy_indicator field

--this subquery produces a list of contacts, the year that the contact was recorded, and the next year that contact appears as being recorded in one row
INSERT INTO ft_ds_refined.metric_historical_retention_percentage
WITH participant_years AS (
    SELECT
        contact_id,
        year,
        LEAD(year) OVER (PARTITION BY contact_id ORDER BY year) AS next_year
    FROM
        ft_ds_refined.current_and_historical_participants_view
    --this filter ensures that only the last two years are being looked at, so we can get the current retention
    --WHERE year IN (CAST(DATE_PART('year', CURRENT_DATE) AS NUMERIC), CAST(DATE_PART('year', CURRENT_DATE) AS NUMERIC) - 1)
)
,
-- this join has to happen since there are some situations when the participant switches chapters, so we have to know to which chapter to assign the retention, and how many from each chapter were eligible to return the next year
participant_years_with_chapter_ids AS (
    SELECT
        py.contact_id,
        curr_chapters.chapter_id as curr_chapter_id,
        last_years_chapters.chapter_id as last_years_chapter_id,
        py.year,
        py.next_year
    FROM
        participant_years py
    LEFT JOIN
        (
            SELECT
                contact_id,
                chapter_id,
                year
            FROM
                ft_ds_refined.current_and_historical_participants_view
            --WHERE
             --   year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC)
        ) curr_chapters
    ON
        py.contact_id = curr_chapters.contact_id
        AND py.next_year = curr_chapters.year
    LEFT JOIN
        (
            SELECT
                contact_id,
                chapter_id,
                year
            FROM
                ft_ds_refined.current_and_historical_participants_view
            --WHERE
             --   year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC) - 1
        ) last_years_chapters
    ON
        py.contact_id = last_years_chapters.contact_id
        AND py.year = last_years_chapters.year
)
,
retention AS (
    SELECT
        py.curr_chapter_id,
        py.year,
        COUNT(DISTINCT CASE WHEN next_year = py.year + 1 THEN py.contact_id END) AS retained_participants
    FROM
        participant_years_with_chapter_ids py
    --WHERE py.year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC) - 1
    GROUP BY
        --the calculation of retention and totals happens separately since retention is credited to the new chapter
        py.curr_chapter_id,
        py.year
)
,
totals_by_chapter AS (
    SELECT
        py.last_years_chapter_id,
        py.year,
        --the filter below only counts those who are under 19 years old last calendar year.
        COUNT(DISTINCT CASE WHEN part_view.birthdate >= (SELECT MAKE_DATE(CAST(part_view.year AS INTEGER) - 19, 1, 1)) THEN py.contact_id END) AS total_participants
    FROM
        participant_years_with_chapter_ids py
    JOIN
        ft_ds_refined.current_and_historical_participants_view part_view
    ON
        py.contact_id = part_view.contact_id
        AND py.year = part_view.year
    --WHERE py.year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC) - 1
    --this filter excludes non-salesforce contacts from international chapters, which is necessary since they do not have year-over-year common contact IDs
    where LENGTH(py.contact_id) = 18
    GROUP BY
        --the calculation of retention and totals happens separately since the total eligible to return is from the old chapter
        py.last_years_chapter_id,
        py.year
)
SELECT
    TO_DATE((list_of_all_chapters.year + 1)::TEXT || '-12-31', 'YYYY-MM-DD') AS metric_calc_date,
    list_of_all_chapters.chapter_id,
    (retention.retained_participants * 1.0) / NULLIF(totals_by_chapter.total_participants, 0) AS retention_percentage,
    (list_of_all_chapters.year + 1)::TEXT AS eoy_indicator
FROM
    (   
        SELECT
            DISTINCT chapter_id, year
        FROM ft_ds_refined.current_and_historical_participants_view
        WHERE year < CAST(date_part('year', CURRENT_DATE) AS NUMERIC) - 1
    ) list_of_all_chapters
LEFT JOIN
    retention
ON
    list_of_all_chapters.chapter_id = retention.curr_chapter_id
    and list_of_all_chapters.year = retention.year
LEFT JOIN
    totals_by_chapter
ON
    list_of_all_chapters.chapter_id = totals_by_chapter.last_years_chapter_id
    and list_of_all_chapters.year = totals_by_chapter.year
;

--update table to have the EOY indicator for this current year be correct
UPDATE
    ft_ds_refined.metric_historical_retention_percentage
SET
    eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
WHERE
    metric_calc_date = (
        SELECT
            MAX(metric_calc_date)
        FROM ft_ds_refined.metric_historical_retention_percentage
    )
;