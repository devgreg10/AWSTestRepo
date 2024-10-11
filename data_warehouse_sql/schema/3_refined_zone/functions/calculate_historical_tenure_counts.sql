CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_tenure_counts()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE cur_timestamp TIMESTAMPTZ;
BEGIN
    --saves the timestamp for use as the metric_calc_date for each INSERT statement for the rest of the function, so each years_tenured has the same metric_calc_date
    cur_timestamp = NOW();

    DROP TABLE IF EXISTS temp_participant_years_for_tenure;

    CREATE TEMP TABLE IF NOT EXISTS temp_participant_years_for_tenure AS
        SELECT
            contact_id,
            chapter_id,
            year,
            LEAD(year) OVER (PARTITION BY chapter_id, contact_id ORDER BY year) AS next_year
        FROM
            ft_ds_refined.current_and_historical_participants_view
    ;

    DROP TABLE IF EXISTS temp_chapter_list;

    CREATE TEMP TABLE IF NOT EXISTS temp_chapter_list AS
        SELECT
            DISTINCT chapter_id
        FROM ft_ds_refined.active_participants_view
    ;

    --this first INSERT statement is less of a template than the second to understand how each subsequent one works. For a reference, move to the second statement
    INSERT INTO ft_ds_refined.metric_historical_tenure_counts
    WITH one_year_participants AS (
        SELECT
            contact_id,
            chapter_id
        FROM
        (
            SELECT
                contact_id,
                chapter_id
            FROM temp_participant_years_for_tenure
            WHERE
            	--we count this year as one of the years in a row they participated
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER)
        ) participated_last_one_year
        WHERE NOT EXISTS
        (
            SELECT
                1
            FROM
                temp_participant_years_for_tenure
            WHERE
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 1
                AND participated_last_one_year.contact_id = temp_participant_years_for_tenure.contact_id
                AND participated_last_one_year.chapter_id = temp_participant_years_for_tenure.chapter_id
        )
    )
    SELECT
        cur_timestamp AS metric_calc_date,
        temp_chapter_list.chapter_id,
        '1' AS years_tenured,
        COALESCE(tenured.tenured_participants, 0) AS tenure_count
    FROM temp_chapter_list
    LEFT JOIN (
        SELECT
            chapter_id,
            count(*) AS tenured_participants
        FROM one_year_participants
        GROUP BY
            chapter_id
    ) tenured
    ON
        temp_chapter_list.chapter_id = tenured.chapter_id
    ;

    INSERT INTO ft_ds_refined.metric_historical_tenure_counts
    WITH two_year_participants AS (
        SELECT
            contact_id,
            chapter_id
        FROM
        (
            SELECT
                contact_id,
                chapter_id,
                COUNT(*) AS consecutive_years_participated
            FROM temp_participant_years_for_tenure
            WHERE
            	--we count this year as one of the years in a row they participated
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) OR
            	--this filter ensures that we are only looking at years where the participants participated back to back
                (next_year = year + 1
                --this filter ensures that we are only looking at the two previous years (and this year but that will never have a consecutive participant because next year hasnt happened yet)
                AND year > CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 2)
            GROUP BY
                contact_id,
                chapter_id
            --this filter ensures that the two previous years were participated in
            HAVING COUNT(*) = 2
        ) participated_last_two_years
        WHERE NOT EXISTS
        (
            SELECT
                1
            FROM
                temp_participant_years_for_tenure
            WHERE
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 2
                AND participated_last_two_years.contact_id = temp_participant_years_for_tenure.contact_id
                AND participated_last_two_years.chapter_id = temp_participant_years_for_tenure.chapter_id
        )
    )
    SELECT
        cur_timestamp AS metric_calc_date,
        temp_chapter_list.chapter_id,
        '2' AS years_tenured,
        COALESCE(tenured.tenured_participants , 0) AS tenure_count
    FROM temp_chapter_list
    LEFT JOIN (
        SELECT
            chapter_id,
            count(*) AS tenured_participants
        FROM two_year_participants
        GROUP BY
            chapter_id
        ) tenured
    ON
        temp_chapter_list.chapter_id = tenured.chapter_id
    ;

    INSERT INTO ft_ds_refined.metric_historical_tenure_counts
    WITH three_year_participants AS (
        SELECT
            contact_id,
            chapter_id
        FROM
        (
            SELECT
                contact_id,
                chapter_id,
                COUNT(*) AS consecutive_years_participated
            FROM temp_participant_years_for_tenure
            WHERE
            	--we count this year as one of the years in a row they participated
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) OR
            	--this filter ensures that we are only looking at years where the participants participated back to back
                (next_year = year + 1
                --this filter ensures that we are only looking at the three previous years (and this year but that will never have a consecutive participant because next year hasnt happened yet)
                AND year > CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 3)
            GROUP BY
                contact_id,
                chapter_id
            --this filter ensures that the three previous years were participated in
            HAVING COUNT(*) = 3
        ) participated_last_three_years
        WHERE NOT EXISTS
        (
            SELECT
                1
            FROM
                temp_participant_years_for_tenure
            WHERE
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 3
                AND participated_last_three_years.contact_id = temp_participant_years_for_tenure.contact_id
                AND participated_last_three_years.chapter_id = temp_participant_years_for_tenure.chapter_id
        )
    )
    SELECT
        cur_timestamp AS metric_calc_date,
        temp_chapter_list.chapter_id,
        '3' AS years_tenured,
        COALESCE(tenured.tenured_participants, 0) AS tenure_count
    FROM temp_chapter_list
    LEFT JOIN (
        SELECT
            chapter_id,
            count(*) AS tenured_participants
        FROM three_year_participants
        GROUP BY
            chapter_id
        ) tenured
    ON
        temp_chapter_list.chapter_id = tenured.chapter_id
    ;

    INSERT INTO ft_ds_refined.metric_historical_tenure_counts
    WITH four_year_participants AS (
        SELECT
            contact_id,
            chapter_id
        FROM
        (
            SELECT
                contact_id,
                chapter_id,
                COUNT(*) AS consecutive_years_participated
            FROM temp_participant_years_for_tenure
            WHERE
            	--we count this year as one of the years in a row they participated
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) OR
            	--this filter ensures that we are only looking at years where the participants participated back to back
                (next_year = year + 1
                --this filter ensures that we are only looking at the four previous years (and this year but that will never have a consecutive participant because next year hasnt happened yet)
                AND year > CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 4)
            GROUP BY
                contact_id,
                chapter_id
            --this filter ensures that the four previous years were participated in
            HAVING COUNT(*) = 4
        ) participated_last_four_years
        WHERE NOT EXISTS
        (
            SELECT
                1
            FROM
                temp_participant_years_for_tenure
            WHERE
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 4
                AND participated_last_four_years.contact_id = temp_participant_years_for_tenure.contact_id
                AND participated_last_four_years.chapter_id = temp_participant_years_for_tenure.chapter_id
        )
    )
    SELECT
        cur_timestamp AS metric_calc_date,
        temp_chapter_list.chapter_id,
        '4' AS years_tenured,
        COALESCE(tenured.tenured_participants, 0) AS tenure_count
    FROM temp_chapter_list
    LEFT JOIN (
        SELECT
            chapter_id,
            count(*) AS tenured_participants
        FROM four_year_participants
        GROUP BY
            chapter_id
        ) tenured
    ON
        temp_chapter_list.chapter_id = tenured.chapter_id
    ;

    INSERT INTO ft_ds_refined.metric_historical_tenure_counts
    WITH five_year_participants AS (
        SELECT
            contact_id,
            chapter_id
        FROM
        (
            SELECT
                contact_id,
                chapter_id,
                COUNT(*) AS consecutive_years_participated
            FROM temp_participant_years_for_tenure
            WHERE
            	--we count this year as one of the years in a row they participated
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) OR
            	--this filter ensures that we are only looking at years where the participants participated back to back
                (next_year = year + 1
                --this filter ensures that we are only looking at the five previous years (and this year but that will never have a consecutive participant because next year hasnt happened yet)
                AND year > CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 5)
            GROUP BY
                contact_id,
                chapter_id
            --this filter ensures that the five previous years were participated in
            HAVING COUNT(*) = 5
        ) participated_last_five_years
        WHERE NOT EXISTS
        (
            SELECT
                1
            FROM
                temp_participant_years_for_tenure
            WHERE
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 5
                AND participated_last_five_years.contact_id = temp_participant_years_for_tenure.contact_id
                AND participated_last_five_years.chapter_id = temp_participant_years_for_tenure.chapter_id
        )
    )
    SELECT
        cur_timestamp AS metric_calc_date,
        temp_chapter_list.chapter_id,
        '5' AS years_tenured,
        COALESCE(tenured.tenured_participants, 0) AS tenure_count
    FROM temp_chapter_list
    LEFT JOIN (
        SELECT
            chapter_id,
            count(*) AS tenured_participants
        FROM five_year_participants
        GROUP BY
            chapter_id
        ) tenured
    ON
        temp_chapter_list.chapter_id = tenured.chapter_id
    ;

    INSERT INTO ft_ds_refined.metric_historical_tenure_counts
    WITH six_plus_year_participants AS (
        SELECT
            contact_id,
            chapter_id
        FROM
        (
            SELECT
                contact_id,
                chapter_id,
                COUNT(*) AS consecutive_years_participated
            FROM temp_participant_years_for_tenure
            WHERE
            	--we count this year as one of the years in a row they participated
                year = CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) OR
            	--this filter ensures that we are only looking at years where the participants participated back to back
                (next_year = year + 1
                --this filter ensures that we are only looking at the six previous years (and this year but that will never have a consecutive participant because next year hasnt happened yet)
                AND year > CAST(EXTRACT(YEAR FROM current_date) AS INTEGER) - 6)
            GROUP BY
                contact_id,
                chapter_id
            --this filter ensures that the six previous years were participated in
            HAVING COUNT(*) = 6
        ) participated_last_six_years
        --since this can be 6+ year, then it does not matter that they had participated before then
    )
    SELECT
        cur_timestamp AS metric_calc_date,
        temp_chapter_list.chapter_id,
        '6+' AS years_tenured,
        COALESCE(tenured.tenured_participants, 0) AS tenure_count
    FROM temp_chapter_list
    LEFT JOIN (
        SELECT
            chapter_id,
            count(*) AS tenured_participants
        FROM six_plus_year_participants
        GROUP BY
            chapter_id
        ) tenured
    ON
        temp_chapter_list.chapter_id = tenured.chapter_id
    ;

    INSERT INTO ft_ds_refined.metric_historical_tenure_counts
    SELECT
        cur_timestamp AS metric_calc_date,
        chapter_id,
        'Total' AS years_tenured,
        COUNT(*) AS tenure_count
    FROM ft_ds_refined.active_participants_view
    GROUP BY chapter_id
    ;
END;
$$;