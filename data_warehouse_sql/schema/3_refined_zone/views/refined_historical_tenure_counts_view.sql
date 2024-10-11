CREATE OR REPLACE VIEW ft_ds_refined.metric_historical_tenure_counts_view AS
    SELECT
        metric_calc_date,
        chapter_id,
        MIN(CASE WHEN years_tenured = '1' THEN tenure_count END) AS one_year_tenure_count,
        MIN(CASE WHEN years_tenured = '2' THEN tenure_count END) AS two_year_tenure_count,
        MIN(CASE WHEN years_tenured = '3' THEN tenure_count END) AS three_year_tenure_count,
        MIN(CASE WHEN years_tenured = '4' THEN tenure_count END) AS four_year_tenure_count,
        MIN(CASE WHEN years_tenured = '5' THEN tenure_count END) AS five_year_tenure_count,
        MIN(CASE WHEN years_tenured = '6+' THEN tenure_count END) AS six_plus_year_tenure_count,
        MIN(CASE WHEN years_tenured = 'Total' THEN tenure_count END) AS total_count
    FROM ft_ds_refined.metric_historical_tenure_counts
    GROUP BY
        metric_calc_date,
        chapter_id
;