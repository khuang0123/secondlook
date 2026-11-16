-- =====================================================================
-- SecondLook - Assignment 3, Part 2: Analytical queries on the star schema
-- Every query joins fact_sales to dimension tables in secondlook_gold.
-- Revenue is in H&M's scaled price units, not real currency.
-- =====================================================================
USE secondlook_gold;

-- ---------------------------------------------------------------------
-- Q1. How do units and revenue split by index group and by the article's sales tier at the
--     time of sale, with subtotals per index group and a grand total?
-- Constructs: fact x dim_article (SCD2 version valid on the sale date), GROUP BY ... WITH ROLLUP
-- ---------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT COALESCE(a.index_group_name, 'ALL INDEX GROUPS') AS index_group,
       COALESCE(a.sales_tier, 'ALL TIERS')              AS sales_tier_at_sale,
       SUM(f.quantity)                                  AS units,
       ROUND(SUM(f.revenue), 2)                         AS revenue,
       COUNT(DISTINCT f.order_id)                       AS orders
FROM fact_sales f
JOIN dim_article a ON a.article_sk = f.article_sk
GROUP BY a.index_group_name, a.sales_tier WITH ROLLUP;

-- ---------------------------------------------------------------------
-- Q2. Do store and online sales react differently to weather? Units per day by anomaly band
--     and channel, with each channel's share of that band's units.
-- Constructs: fact x dim_date (weather attributes) x dim_channel, window function
-- ---------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT d.anomaly_band, ch.channel_name,
       COUNT(DISTINCT d.date_key)                                       AS days,
       SUM(f.quantity)                                                  AS units,
       ROUND(SUM(f.quantity) / COUNT(DISTINCT d.date_key), 1)           AS units_per_day,
       ROUND(100 * SUM(f.quantity) / SUM(SUM(f.quantity)) OVER (PARTITION BY d.anomaly_band), 1)
                                                                        AS pct_of_band_units
FROM fact_sales f
JOIN dim_date d     ON d.date_key = f.date_key
JOIN dim_channel ch ON ch.channel_sk = f.channel_sk
GROUP BY d.anomaly_band, ch.channel_name
ORDER BY d.anomaly_band, ch.channel_name;

-- ---------------------------------------------------------------------
-- Q3. Which articles changed tier between 30 June and 22 September, and how did they sell in
--     the second period (July 1 onward)?
-- Constructs: SCD2 self-join on dim_article (expired vs current version), fact x dim_article
-- ---------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT old_v.sales_tier                    AS tier_at_2020_06_30,
       new_v.sales_tier                    AS tier_at_2020_09_22,
       COUNT(DISTINCT new_v.article_id)    AS articles,
       COALESCE(SUM(f.quantity), 0)        AS units_since_jul_1,
       ROUND(COALESCE(SUM(f.quantity), 0) / COUNT(DISTINCT new_v.article_id), 2) AS units_per_article
FROM dim_article old_v
JOIN dim_article new_v ON new_v.article_id = old_v.article_id AND new_v.is_current = TRUE
LEFT JOIN fact_sales f ON f.article_sk = new_v.article_sk
WHERE old_v.is_current = FALSE
GROUP BY old_v.sales_tier, new_v.sales_tier
ORDER BY articles DESC;

-- ---------------------------------------------------------------------
-- Q4. Which customer age bands buy slow movers most? Share of each age band's units that came
--     from Slow Mover articles (the audience SecondLook should target first).
-- Constructs: fact x dim_customer x dim_article, conditional aggregation, HAVING
-- ---------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT c.age_band,
       COUNT(DISTINCT c.customer_sk)                                         AS customers,
       SUM(f.quantity)                                                       AS units,
       SUM(CASE WHEN a.sales_tier = 'Slow Mover' THEN f.quantity ELSE 0 END) AS slow_mover_units,
       ROUND(100 * SUM(CASE WHEN a.sales_tier = 'Slow Mover' THEN f.quantity ELSE 0 END)
                 / SUM(f.quantity), 2)                                       AS slow_mover_pct
FROM fact_sales f
JOIN dim_customer c ON c.customer_sk = f.customer_sk
JOIN dim_article a  ON a.article_sk = f.article_sk
GROUP BY c.age_band
HAVING SUM(f.quantity) >= 500
ORDER BY slow_mover_pct DESC;

-- ---------------------------------------------------------------------
-- Q5. Month by month, how much revenue does each channel bring in, and what is the
--     cumulative revenue to date?
-- Constructs: fact x dim_date x dim_channel, window function SUM() OVER (ORDER BY)
-- ---------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT d.year_month_label AS month, ch.channel_name,
       SUM(f.quantity)          AS units,
       ROUND(SUM(f.revenue), 2) AS revenue,
       ROUND(SUM(SUM(f.revenue)) OVER (PARTITION BY ch.channel_name ORDER BY d.year_month_label), 2)
                                AS cumulative_revenue
FROM fact_sales f
JOIN dim_date d     ON d.date_key = f.date_key
JOIN dim_channel ch ON ch.channel_sk = f.channel_sk
GROUP BY d.year_month_label, ch.channel_name
ORDER BY ch.channel_name, d.year_month_label;
