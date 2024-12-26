CREATE OR REPLACE VIEW ft_ds_refined.report_cpa_first_tee_programs AS
SELECT
    counts.chapter_id,
    counts.participant_count AS first_tee_program_particpants,
    counts.eoy_indicator
FROM ft_ds_refined.metric_historical_active_participant_counts counts
--will eventually have joins to it that look like this: (going to use counts as the main table to join to since it's the most basic metric)
--JOIN ft_ds_refined.metric_historical_retention_percentage retention
--ON counts.chapter_id = retention.chapter_id
--AND counts.chapter_id = retention.chapter_id
--this part will be the peer group averages of the current year
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
        peer_group_count_averages.participant_count,
        'Curr Yr Peer Grp Avg' AS eoy_indicator
    FROM
    (
        SELECT
            peer_group_map.peer_group_level,
            AVG(counts_info.participant_count) AS participant_count
        FROM
        (
            SELECT
                chapter_id,
                participant_count
            FROM ft_ds_refined.metric_historical_active_participant_counts
            WHERE eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
        ) counts_info
        JOIN
        peer_group_map
        ON
        peer_group_map.account_id = counts_info.chapter_id
        GROUP BY peer_group_map.peer_group_level
    ) peer_group_count_averages
    JOIN 
    peer_group_map
    ON peer_group_map.peer_group_level = peer_group_count_averages.peer_group_level
)
;