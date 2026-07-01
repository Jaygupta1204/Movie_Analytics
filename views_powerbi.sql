-- =====================================================================
-- Views consumed directly by Power BI (live connection / DirectQuery)
-- Keeping these as views means Power BI never needs to know about the
-- star-schema joins — analysts can build visuals straight off these.
-- =====================================================================
SET search_path TO warehouse;

CREATE OR REPLACE VIEW vw_genre_revenue AS
SELECT
    g.genre_name,
    COUNT(DISTINCT f.movie_key)            AS movie_count,
    SUM(f.revenue_usd)                      AS total_revenue,
    AVG(f.revenue_usd)                       AS avg_revenue_per_movie,
    AVG(f.avg_rating)                         AS avg_rating
FROM fact_movie_performance f
JOIN dim_movie_genre mg ON f.movie_key = mg.movie_key
JOIN dim_genre g        ON mg.genre_key = g.genre_key
WHERE f.revenue_usd IS NOT NULL
GROUP BY g.genre_name;

CREATE OR REPLACE VIEW vw_top_rated_clusters AS
SELECT
    m.title,
    m.release_date,
    f.avg_rating,
    f.rating_count,
    f.revenue_usd,
    f.rating_cluster,
    CASE f.rating_cluster
        WHEN 0 THEN 'Low-rated'
        WHEN 1 THEN 'Mid-tier'
        WHEN 2 THEN 'Top-rated'
        ELSE 'Unclassified'
    END AS cluster_label
FROM fact_movie_performance f
JOIN dim_movie m ON f.movie_key = m.movie_key;

CREATE OR REPLACE VIEW vw_cpi_vs_boxoffice AS
SELECT
    d.year_month,
    c.cpi_value,
    c.yoy_pct_change,
    SUM(f.revenue_usd)        AS monthly_revenue,
    COUNT(f.movie_key)         AS movies_released
FROM fact_movie_performance f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_cpi c  ON f.cpi_key = c.cpi_key
GROUP BY d.year_month, c.cpi_value, c.yoy_pct_change
ORDER BY d.year_month;

CREATE OR REPLACE VIEW vw_latest_validation_report AS
SELECT *
FROM etl_validation_log
WHERE batch_id = (SELECT batch_id FROM etl_validation_log ORDER BY run_timestamp DESC LIMIT 1)
ORDER BY check_name;
