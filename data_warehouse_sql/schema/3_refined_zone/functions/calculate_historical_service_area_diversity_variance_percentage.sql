CREATE OR REPLACE FUNCTION ft_ds_admin.calculate_historical_service_area_diversity_variance_percentage()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE
        ft_ds_refined.metric_historical_service_area_diversity_variance_percentage
    SET
        eoy_indicator = NULL
    WHERE
        eoy_indicator = CAST(EXTRACT(YEAR FROM NOW()) AS TEXT)
    ;

    INSERT INTO ft_ds_refined.metric_historical_service_area_diversity_variance_percentage
    SELECT
        NOW() as metric_calc_date,
        diverse_metric.chapter_id,
        chapter_info.service_area,
        diverse_metric.ethnic_diversity_percentage - ref_data.market_diversity_percentage AS service_area_diversity_variance_percentage,
        CAST(EXTRACT(YEAR FROM NOW()) AS TEXT) AS eoy_indicator
    FROM (
        SELECT
            chapter_id,
            ethnic_diversity_percentage
        FROM ft_ds_refined.metric_historical_ethnic_diversity_percentage
        WHERE metric_calc_date = (SELECT MAX(metric_calc_date) FROM ft_ds_refined.metric_historical_ethnic_diversity_percentage)
    ) diverse_metric
    LEFT JOIN ft_ds_refined.service_area_diversity_reference_data ref_data
        ON diverse_metric.chapter_id = ref_data.chapter_id
    LEFT JOIN ft_ds_refined.chapter_account_view chapter_info
        ON diverse_metric.chapter_id = chapter_info.account_id
    ;
END;
$$;