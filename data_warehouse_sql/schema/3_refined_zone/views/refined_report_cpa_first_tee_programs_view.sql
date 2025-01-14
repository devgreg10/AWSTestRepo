CREATE OR REPLACE VIEW ft_ds_refined.report_cpa_first_tee_programs AS
SELECT
    chapter_info.account_name as chapter_name,
    averages_info.first_tee_program_particpants,
    averages_info.percent_annual_retention,
    averages_info.percent_annual_teen_retention,
    averages_info.eoy_indicator
FROM
--this subquery contains the information about the EOY information for each of the chapters, for each year
(
    --include each metric that will be on the report in this select statement, along with a LEFT JOIN to the table that contains that metric
    --the base table everything else is LEFT JOINED to is the active participant counts since it is the most basic metric
    SELECT
        counts.chapter_id,
        counts.participant_count AS first_tee_program_particpants,
        retention.retention_percentage AS percent_annual_retention,
        teen_retention.teen_retention_percentage AS percent_annual_teen_retention,
        counts.eoy_indicator
    FROM ft_ds_refined.metric_historical_active_participant_counts counts
    LEFT JOIN ft_ds_refined.metric_historical_retention_percentage retention
        ON counts.chapter_id = retention.chapter_id
        AND counts.eoy_indicator = retention.eoy_indicator
    LEFT JOIN ft_ds_refined.metric_historical_teen_retention_percentage teen_retention
        ON counts.chapter_id = teen_retention.chapter_id
        AND counts.eoy_indicator = teen_retention.eoy_indicator
    --this part will be the peer group averages of the current year. It needs to include all the metrics that are listed above
    UNION
    (
        WITH peer_group_map AS
        (
            SELECT
                account_id,
                peer_group_level
            FROM ft_ds_refined.chapter_account_view
        )
        SELECT
            peer_group_map.account_id AS chapter_id,
            peer_group_averages.participant_count AS first_tee_program_particpants,
            peer_group_averages.retention_percentage AS percent_annual_retention,
            peer_group_averages.teen_retention_percentage AS percent_annual_teen_retention,
            'Curr Yr Peer Grp Avg' AS eoy_indicator
        FROM
        (
            SELECT
                peer_group_map.peer_group_level,
                AVG(active_participants_counts_info.participant_count) AS participant_count,
                AVG(retention_percentage_info.retention_percentage) AS retention_percentage,
                AVG(teen_retention_percentage_info.teen_retention_percentage) AS teen_retention_percentage
            FROM
            peer_group_map
            LEFT JOIN
            (
                SELECT
                    chapter_id,
                    participant_count
                FROM ft_ds_refined.metric_historical_active_participant_counts
                WHERE eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
            ) active_participants_counts_info
                ON peer_group_map.account_id = active_participants_counts_info.chapter_id
            LEFT JOIN
            (
                SELECT
                    chapter_id,
                    retention_percentage
                FROM ft_ds_refined.metric_historical_retention_percentage
                WHERE eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
            ) retention_percentage_info
                ON peer_group_map.account_id = retention_percentage_info.chapter_id
            LEFT JOIN
            (
                SELECT
                    chapter_id,
                    teen_retention_percentage
                FROM ft_ds_refined.metric_historical_teen_retention_percentage
                WHERE eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
            ) teen_retention_percentage_info
                ON peer_group_map.account_id = teen_retention_percentage_info.chapter_id
            GROUP BY peer_group_map.peer_group_level
        ) peer_group_averages
        JOIN 
        peer_group_map
        ON peer_group_map.peer_group_level = peer_group_averages.peer_group_level
    )
) averages_info
--this join merely adds the chapter names to the view for better human readability/BI use
JOIN ft_ds_refined.chapter_account_view chapter_info
    ON averages_info.chapter_id = chapter_info.account_id
;