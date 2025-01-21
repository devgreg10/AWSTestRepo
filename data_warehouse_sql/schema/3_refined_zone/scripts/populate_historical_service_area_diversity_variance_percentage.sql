INSERT INTO ft_ds_refined.metric_historical_service_area_diversity_variance_percentage 
SELECT
    metric_calc_date,
    diverse_metric.chapter_id,
    chapter_info.service_area,
    diverse_metric.ethnic_diversity_percentage - ref_data.market_diversity_percentage AS service_area_diversity_variance_percentage,
    eoy_indicator
FROM ft_ds_refined.metric_historical_ethnic_diversity_percentage diverse_metric
LEFT JOIN ft_ds_refined.service_area_diversity_reference_data ref_data
        ON diverse_metric.chapter_id = ref_data.chapter_id
LEFT JOIN ft_ds_refined.chapter_account_view chapter_info
    ON diverse_metric.chapter_id = chapter_info.account_id
WHERE diverse_metric.eoy_indicator < '2025'
ON CONFLICT (metric_calc_date, chapter_id) DO UPDATE SET
        service_area = EXCLUDED.service_area,
        service_area_diversity_variance_percentage = EXCLUDED.service_area_diversity_variance_percentage,
        eoy_indicator = EXCLUDED.eoy_indicator
;