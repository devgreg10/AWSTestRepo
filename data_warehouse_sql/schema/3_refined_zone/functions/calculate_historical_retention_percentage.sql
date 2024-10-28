CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_retention_percentage()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_refined.metric_historical_retention_percentage
    --this subquery produces a list of contacts, the year that the contact was recorded, and the next year that contact appears as being recorded in one row
    WITH participant_years AS (
        SELECT
            contact_id,
            year,
            LEAD(year) OVER (PARTITION BY contact_id ORDER BY year) AS next_year
        FROM
            ft_ds_refined.current_and_historical_participants_view
        --this filter ensures that only the last two years are being looked at, so we can get the current retention
        WHERE year IN (CAST(DATE_PART('year', CURRENT_DATE) AS NUMERIC), CAST(DATE_PART('year', CURRENT_DATE) AS NUMERIC) - 1)
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
                WHERE
                    year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC)
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
                WHERE
                    year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC) - 1
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
        WHERE py.year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC) - 1
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
            COUNT(DISTINCT CASE WHEN part_view.birthdate >= (SELECT MAKE_DATE(CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) - 19, 1, 1)) THEN py.contact_id END) AS total_participants
        FROM
            participant_years_with_chapter_ids py
        JOIN
            ft_ds_refined.current_and_historical_participants_view part_view
        ON
            py.contact_id = part_view.contact_id
            AND py.year = part_view.year
        WHERE py.year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC) - 1
        GROUP BY
            --the calculation of retention and totals happens separately since the total eligible to return is from the old chapter
            py.last_years_chapter_id,
            py.year
    )
    SELECT
        NOW() AS metric_calc_date,
        list_of_all_chapters.chapter_id,
        (retention.retained_participants * 1.0) / NULLIF(totals_by_chapter.total_participants, 0) AS retention_percentage
    FROM
        (   
            SELECT
                DISTINCT chapter_id
            FROM ft_ds_refined.current_and_historical_participants_view
            WHERE year = CAST(date_part('year', CURRENT_DATE) AS NUMERIC) - 1
        ) list_of_all_chapters
    LEFT JOIN
        retention
    ON
        list_of_all_chapters.chapter_id = retention.curr_chapter_id
    LEFT JOIN
        totals_by_chapter
    ON
        list_of_all_chapters.chapter_id = totals_by_chapter.last_years_chapter_id
    ;
END;
$$;