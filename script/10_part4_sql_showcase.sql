-- =====================================================================
-- SecondLook - Assignment 3, Part 4a: Relational warehouse showcase (3 queries)
-- Queries that play to the star schema's strengths: multi-dimensional rollups,
-- time-series trends, and point-in-time (SCD2) attribution across conformed dimensions.
-- Run on the RDS connection; one query at a time.
-- =====================================================================
USE secondlook_gold;

-- ---------------------------------------------------------------------
-- 4a.1  How do units and revenue split by index group and by each article's sales tier at the
--       time of sale, with a subtotal per index group and a grand total?
-- Strength: multi-dimensional aggregation with subtotals in one pass (WITH ROLLUP).
-- (Same query as Part 2 Q1; benefits from idx_fact_article_cover.)
-- ---------------------------------------------------------------------
SELECT COALESCE(a.index_group_name, 'ALL INDEX GROUPS') AS index_group,
       COALESCE(a.sales_tier, 'ALL TIERS')              AS sales_tier_at_sale,
       SUM(f.quantity)                                  AS units,
       ROUND(SUM(f.revenue), 2)                         AS revenue,
       COUNT(DISTINCT f.order_id)                       AS orders
FROM fact_sales f
JOIN dim_article a ON a.article_sk = f.article_sk
GROUP BY a.index_group_name, a.sales_tier WITH ROLLUP;

-- ---------------------------------------------------------------------
-- 4a.2  Month by month, how much revenue does each channel bring in, and what is each channel's
--       cumulative revenue to date?
-- Strength: time-series trend over the date dimension with a running total (window function).
-- (Same query as Part 2 Q5.)
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 4a.3  NEW: Which sales would be attributed to the WRONG tier if we used each article's tier
--       today instead of its tier when it was sold?  By month, tier at the time of sale and
--       tier today.
-- Strength: SCD2 point-in-time lookup. Each fact row already points to the article version
--       valid on its sale date; joining that version to the current version by business key
--       shows exactly where history differs from today's view.
-- ---------------------------------------------------------------------
SELECT d.year_month_label                  AS month,
       a_sale.sales_tier                   AS tier_at_time_of_sale,
       a_now.sales_tier                    AS tier_today,
       COUNT(DISTINCT a_sale.article_id)   AS articles,
       SUM(f.quantity)                     AS units,
       ROUND(SUM(f.revenue), 2)            AS revenue
FROM fact_sales f
JOIN dim_date d          ON d.date_key = f.date_key
JOIN dim_article a_sale  ON a_sale.article_sk = f.article_sk
JOIN dim_article a_now   ON a_now.article_id = a_sale.article_id AND a_now.is_current = TRUE
WHERE a_sale.sales_tier <> a_now.sales_tier
GROUP BY d.year_month_label, a_sale.sales_tier, a_now.sales_tier
ORDER BY month, units DESC;
