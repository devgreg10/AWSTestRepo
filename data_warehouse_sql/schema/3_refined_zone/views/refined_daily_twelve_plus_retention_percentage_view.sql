CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_daily_twelve_plus_retention_percentage_view AS
    SELECT
        dates.metric_calc_date,
        info.chapter_id,
        account.account_name as chapter_name,
        info.twelve_plus_retention_percentage
    FROM (
        SELECT
            MAX(metric_calc_date) AS latest_calc_timestamp,
            chapter_id,
            CAST(metric_calc_date AS DATE) as metric_calc_date
        FROM
        	ft_ds_refined.metric_historical_twelve_plus_retention_percentage
        GROUP BY
            CAST(metric_calc_date AS DATE),
            chapter_id
    ) dates
    JOIN
        ft_ds_refined.metric_historical_twelve_plus_retention_percentage info
    ON
        dates.latest_calc_timestamp = info.metric_calc_date
        AND dates.chapter_id = info.chapter_id
    left join ft_ds_refined.account account 
    	on info.chapter_id = account.account_id
;