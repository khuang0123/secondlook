-- =====================================================================
-- SecondLook - Assignment 3, Part 1, Step 4: Gold layer (SQL ELT)
-- Reads ONLY from secondlook_silver; writes to secondlook_gold.
-- Run top to bottom in Workbench (plain lightning bolt). Safe to re-run: rebuilds every Gold table.
-- Window end (tier as-of date) = MAX(order_date) in Silver = 2020-09-22.
-- =====================================================================
CREATE DATABASE IF NOT EXISTS secondlook_gold CHARACTER SET utf8mb4;
USE secondlook_gold;


-- ---------------------------------------------------------------------
-- gold_article_sales_tier
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold_article_sales_tier;
CREATE TABLE gold_article_sales_tier (
    article_id CHAR(10) PRIMARY KEY, prod_name VARCHAR(120), product_type_name VARCHAR(60),
    product_group_name VARCHAR(60), index_group_name VARCHAR(60),
    units INT NOT NULL, first_sale_date DATE NULL, weeks_on_sale DECIMAL(6,2) NULL,
    units_per_week DECIMAL(10,4) NULL, is_eligible_for_slow BOOLEAN NOT NULL,
    pct_rank DECIMAL(8,6) NULL, sales_tier VARCHAR(12) NOT NULL, tier_as_of DATE NOT NULL,
    KEY ix_tier (sales_tier)) ENGINE=InnoDB;
INSERT INTO gold_article_sales_tier
WITH sales AS (
    SELECT article_id, SUM(quantity) AS units, MIN(order_date) AS first_sale_date
    FROM secondlook_silver.silver_order_lines GROUP BY article_id),
base AS (
    SELECT a.article_id, a.prod_name, a.product_type_name, a.product_group_name, a.index_group_name,
           COALESCE(s.units, 0) AS units, s.first_sale_date,
           CASE WHEN s.first_sale_date IS NULL THEN NULL
                ELSE GREATEST(DATEDIFF(DATE '2020-09-22', s.first_sale_date) + 1, 7) / 7 END AS weeks_on_sale
    FROM secondlook_silver.silver_articles a LEFT JOIN sales s ON s.article_id = a.article_id),
rated AS (
    SELECT base.*, units / weeks_on_sale AS units_per_week,
           (first_sale_date IS NOT NULL
            AND first_sale_date <= DATE '2020-09-22' - INTERVAL 28 DAY) AS is_eligible_for_slow
    FROM base),
ranked AS (
    SELECT rated.*,
           CASE WHEN is_eligible_for_slow THEN
                PERCENT_RANK() OVER (PARTITION BY is_eligible_for_slow ORDER BY units_per_week) END AS pct_rank
    FROM rated)
SELECT article_id, prod_name, product_type_name, product_group_name, index_group_name,
       units, first_sale_date, weeks_on_sale, units_per_week, is_eligible_for_slow, pct_rank,
       CASE WHEN units = 0 THEN 'Never Sold'
            WHEN is_eligible_for_slow AND pct_rank <= 0.1 THEN 'Slow Mover'
            ELSE 'Normal' END AS sales_tier,
       DATE '2020-09-22' AS tier_as_of
FROM ranked;

-- ---------------------------------------------------------------------
-- gold_daily_sales_weather
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold_daily_sales_weather;
CREATE TABLE gold_daily_sales_weather (
    order_date DATE NOT NULL, sales_channel_id TINYINT NOT NULL,
    orders INT, order_lines INT, units INT, revenue DECIMAL(22,10), customers INT,
    slow_mover_units INT, slow_mover_unit_share DECIMAL(6,4),
    temp_mean_c DECIMAL(5,2), temp_anomaly_c DECIMAL(5,2), temp_band VARCHAR(10),
    anomaly_band VARCHAR(20), precip_mm DECIMAL(6,2), is_rain_day BOOLEAN,
    PRIMARY KEY (order_date, sales_channel_id)) ENGINE=InnoDB;
INSERT INTO gold_daily_sales_weather
SELECT l.order_date, l.sales_channel_id,
       COUNT(DISTINCT l.order_id), COUNT(*), SUM(l.quantity), SUM(l.revenue),
       COUNT(DISTINCT l.customer_key),
       SUM(CASE WHEN t.sales_tier = 'Slow Mover' THEN l.quantity ELSE 0 END),
       SUM(CASE WHEN t.sales_tier = 'Slow Mover' THEN l.quantity ELSE 0 END) / SUM(l.quantity),
       MAX(l.temp_mean_c), MAX(l.temp_anomaly_c), MAX(l.temp_band), MAX(l.anomaly_band),
       MAX(l.precip_mm), MAX(l.is_rain_day)
FROM secondlook_silver.silver_order_lines_weather l
JOIN gold_article_sales_tier t ON t.article_id = l.article_id
GROUP BY l.order_date, l.sales_channel_id;

-- ---------------------------------------------------------------------
-- gold_tier_sales_by_weather
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold_tier_sales_by_weather;
CREATE TABLE gold_tier_sales_by_weather (
    sales_tier VARCHAR(12) NOT NULL, anomaly_band VARCHAR(20) NOT NULL, sales_channel_id TINYINT NOT NULL,
    days_in_band INT, articles_in_tier INT, articles_sold INT, units INT, revenue DECIMAL(22,10),
    units_per_day DECIMAL(12,4), units_per_1000_articles_per_day DECIMAL(12,4),
    share_of_band_units DECIMAL(6,4),
    PRIMARY KEY (sales_tier, anomaly_band, sales_channel_id)) ENGINE=InnoDB;
INSERT INTO gold_tier_sales_by_weather
WITH tiers AS (SELECT sales_tier, COUNT(*) AS articles_in_tier FROM gold_article_sales_tier GROUP BY sales_tier),
bands AS (SELECT anomaly_band, COUNT(*) AS days_in_band FROM secondlook_silver.silver_weather_national GROUP BY anomaly_band),
channels AS (SELECT DISTINCT sales_channel_id FROM secondlook_silver.silver_order_lines),
sales AS (
    SELECT t.sales_tier, l.anomaly_band, l.sales_channel_id,
           COUNT(DISTINCT l.article_id) AS articles_sold, SUM(l.quantity) AS units, SUM(l.revenue) AS revenue
    FROM secondlook_silver.silver_order_lines_weather l JOIN gold_article_sales_tier t ON t.article_id = l.article_id
    GROUP BY t.sales_tier, l.anomaly_band, l.sales_channel_id),
grid AS (
    SELECT tr.sales_tier, b.anomaly_band, c.sales_channel_id, b.days_in_band, tr.articles_in_tier,
           COALESCE(s.articles_sold, 0) AS articles_sold, COALESCE(s.units, 0) AS units,
           COALESCE(s.revenue, 0) AS revenue
    FROM tiers tr CROSS JOIN bands b CROSS JOIN channels c
    LEFT JOIN sales s ON s.sales_tier = tr.sales_tier AND s.anomaly_band = b.anomaly_band
                     AND s.sales_channel_id = c.sales_channel_id)
SELECT sales_tier, anomaly_band, sales_channel_id, days_in_band, articles_in_tier, articles_sold,
       units, revenue,
       units / days_in_band,
       1000 * units / (days_in_band * articles_in_tier),
       units / NULLIF(SUM(units) OVER (PARTITION BY anomaly_band, sales_channel_id), 0)
FROM grid;

-- ---------------------------------------------------------------------
-- gold_product_group_weather_index
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS gold_product_group_weather_index;
CREATE TABLE gold_product_group_weather_index (
    product_group_name VARCHAR(60) NOT NULL, anomaly_band VARCHAR(20) NOT NULL,
    days_in_band INT, units INT, units_per_day DECIMAL(12,4), group_units_per_day DECIMAL(12,4),
    weather_index DECIMAL(8,1),
    PRIMARY KEY (product_group_name, anomaly_band)) ENGINE=InnoDB;
INSERT INTO gold_product_group_weather_index
WITH bands AS (SELECT anomaly_band, COUNT(*) AS days_in_band FROM secondlook_silver.silver_weather_national GROUP BY anomaly_band),
total_days AS (SELECT COUNT(*) AS n FROM secondlook_silver.silver_weather_national),
g AS (
    SELECT a.product_group_name, l.anomaly_band, SUM(l.quantity) AS units
    FROM secondlook_silver.silver_order_lines_weather l JOIN secondlook_silver.silver_articles a ON a.article_id = l.article_id
    GROUP BY a.product_group_name, l.anomaly_band),
big AS (SELECT product_group_name, SUM(units) AS total_units FROM g GROUP BY product_group_name
        HAVING SUM(units) >= 200)
SELECT g.product_group_name, g.anomaly_band, b.days_in_band, g.units,
       g.units / b.days_in_band,
       big.total_units / td.n,
       100 * (g.units / b.days_in_band) / (big.total_units / td.n)
FROM g JOIN big ON big.product_group_name = g.product_group_name
JOIN bands b ON b.anomaly_band = g.anomaly_band CROSS JOIN total_days td;

-- ---------------------------------------------------------------------
-- Reconciliation: every Gold units total must equal Silver's (39,030)
-- ---------------------------------------------------------------------
SELECT 'silver_order_lines' AS source, SUM(quantity) AS units FROM secondlook_silver.silver_order_lines
UNION ALL SELECT 'gold_article_sales_tier', SUM(units) FROM gold_article_sales_tier
UNION ALL SELECT 'gold_daily_sales_weather', SUM(units) FROM gold_daily_sales_weather
UNION ALL SELECT 'gold_tier_sales_by_weather', SUM(units) FROM gold_tier_sales_by_weather;

-- Tier distribution
SELECT sales_tier, COUNT(*) AS articles, SUM(units) AS units, SUM(is_eligible_for_slow) AS eligible
FROM gold_article_sales_tier GROUP BY sales_tier ORDER BY sales_tier;
