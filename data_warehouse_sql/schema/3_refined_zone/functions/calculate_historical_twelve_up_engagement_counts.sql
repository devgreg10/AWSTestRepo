CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_twelve_up_engagement_counts()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ft_ds_refined.metric_historical_twelve_up_engagement_counts
    SELECT
        NOW() as metric_calc_date,
        chapter_list.chapter_id,
        COALESCE(metric.twelve_up_engagement_counts, 0) AS twelve_up_engagement_counts,
        COALESCE(chapter_totals.twelve_up_total_counts, 0) AS twelve_up_total_counts,
        --it looks dumb to coalesce to 0 then NULLIF to a 0, but I think it's a bit more future proof
        (1.0 * COALESCE(metric.twelve_up_engagement_counts, 0))/NULLIF(COALESCE(chapter_totals.twelve_up_total_counts, 0), 0) AS twelve_up_engagement_percentage
    FROM
        (
            SELECT
                DISTINCT chapter_id
            FROM
                ft_ds_refined.active_participants_view
        ) chapter_list
    LEFT JOIN
        (
            SELECT
                chapter_id,
                count(*) AS twelve_up_engagement_counts
            FROM
                (
                    SELECT
                        pv.contact_id_18,
                        pv.chapter_id,
                        COUNT(*) AS num_times_participated
                    FROM ft_ds_refined.active_participants_view pv
                    JOIN
                    --this view is already filtered for session registrations that are "Registered"
                        ft_ds_refined.registered_session_registrations_view sr
                    ON
                        pv.contact_id_18 = sr.contact_id_18
                    JOIN
                        ft_ds_refined.listing_session ls
                    ON
                        sr.listing_session_id_18 = ls.listing_session_id_18
                    WHERE
                        --this filter ensures that the query only looks at 12 year olds and up
                        CAST(pv.birthdate AS DATE) < (SELECT CURRENT_DATE - INTERVAL '12 years')
                        --this filter ensures that we are only looking at session registrations for sessions that happened this calendar year
                        AND ls.session_start_date > (SELECT MAKE_DATE(CAST(EXTRACT(YEAR FROM current_date) AS INTEGER), 1, 1))
                        --this filter ensures that we are only looking at Curriculum session types
                        AND ls.record_type_id = '01236000000nmeLAAQ'
                    GROUP BY
                        pv.contact_id_18,
                        pv.chapter_id
                    HAVING
                        --this filter ensures that the participants have participated in at least two activities in this year
                        COUNT(*) >= 2
            ) repeat_participants
            GROUP BY
                repeat_participants.chapter_id
        ) metric
    ON chapter_list.chapter_id = metric.chapter_id
    LEFT JOIN
        (
            SELECT
                chapter_id,
                COUNT(*) AS twelve_up_total_counts
            FROM ft_ds_refined.active_participants_view
            WHERE
                --this filter ensures that the query only looks at 12 year olds and up
                CAST(birthdate AS DATE) < (SELECT CURRENT_DATE - INTERVAL '12 years')
            GROUP BY
                chapter_id
        ) chapter_totals
    ON chapter_list.chapter_id = chapter_totals.chapter_id
    ;
END;
$$;