CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_retention_percentage()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_refined.metric_historical_retention_percentage
    WITH participant_years AS (
        SELECT
            chapter_id,
            contact_id,
            year,
            LEAD(year) OVER (PARTITION BY chapter_id, contact_id ORDER BY year) AS next_year
        FROM
            ft_ds_refined.current_and_historical_participants_view
        WHERE year IN (CAST(DATE_PART('year', CURRENT_DATE) AS NUMERIC), CAST(DATE_PART('year', CURRENT_DATE) AS NUMERIC) - 1)
    )
    ,
    retention AS (
        SELECT
            py.chapter_id,
            py.year,
            COUNT(DISTINCT CASE WHEN next_year = py.year + 1 THEN py.contact_id END) AS retained_participants,
            --the filter below only counts those who are under 19 years old last calendar year.
            COUNT(DISTINCT CASE WHEN part_view.birthdate >= (SELECT MAKE_DATE(CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 19, 1, 1)) THEN py.contact_id END) AS total_participants
        FROM
            participant_years py
        JOIN
            ft_ds_refined.current_and_historical_participants_view part_view
        ON
            py.contact_id = part_view.contact_id
            AND py.year = part_view.year
        WHERE py.year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC) - 1
        GROUP BY
            py.chapter_id,
            py.year
    )
    SELECT
        NOW() AS metric_calc_date,
        list_of_all_chapters.chapter_id,
        (retention.retained_participants * 1.0) / NULLIF(retention.total_participants, 0) AS retention_percentage
    FROM
        retention
    RIGHT JOIN
        (   
            SELECT
                DISTINCT chapter_id
            FROM ft_ds_refined.current_and_historical_participants_view
            WHERE year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC) - 1
        ) list_of_all_chapters
    ON retention.chapter_id = list_of_all_chapters.chapter_id;
END;
$$;