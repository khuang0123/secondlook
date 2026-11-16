-- =====================================================================
-- SecondLook - Assignment 3, Part 2: Star schema with SCD Type 2
-- Reads ONLY from secondlook_silver; builds the star schema in secondlook_gold.
-- Run top to bottom in Workbench (plain lightning bolt). Safe to re-run: rebuilds everything.
--
-- Grain of fact_sales: ONE ROW PER ORDER LINE (one article, at one price, in one order;
--   an order = one customer, one date, one sales channel).
--
-- SCD Type 2 on dim_article.sales_tier, using two period-end snapshots:
--   version 1: tier measured on sales 2020-03-01..2020-06-30, valid 2020-03-01..2020-06-30
--   version 2: tier measured on sales 2020-03-01..2020-09-22, valid 2020-07-01..9999-12-31
--   Articles whose tier is unchanged keep a single current row.
-- Tier rule (same as Gold): Never Sold = 0 units; Slow Mover = bottom 10% of units/week since
--   first sale among articles first sold >= 28 days before the snapshot; Normal = everything else.
-- =====================================================================
USE secondlook_gold;

-- Workbench blocks UPDATEs whose WHERE clause doesn't use a key column ("safe update mode",
-- error 1175). The SCD2 expiry in step 4a is intentionally set-based, so switch it off for this
-- session only; it is switched back on at the end of the script.
SET SQL_SAFE_UPDATES = 0;

DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_article;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_channel;
DROP TABLE IF EXISTS stg_article_tiers;
DROP PROCEDURE IF EXISTS sp_article_tiers;

-- ---------------------------------------------------------------------
-- 1. Dimensions
-- ---------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,                -- YYYYMMDD
    full_date       DATE NOT NULL UNIQUE,
    day_name        VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    iso_week        TINYINT NOT NULL,
    month_num       TINYINT NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    year_month_label CHAR(7) NOT NULL,
    quarter_num     TINYINT NOT NULL,
    year_num        SMALLINT NOT NULL,
    temp_mean_c     DECIMAL(5,2),                   -- weather attributes (from the Open-Meteo API)
    temp_anomaly_c  DECIMAL(5,2),
    temp_band       VARCHAR(10),
    anomaly_band    VARCHAR(20),
    precip_mm       DECIMAL(6,2),
    is_rain_day     BOOLEAN
) ENGINE=InnoDB;

INSERT INTO dim_date
SELECT CAST(DATE_FORMAT(weather_date, '%Y%m%d') AS UNSIGNED), weather_date, DAYNAME(weather_date),
       DAYOFWEEK(weather_date) IN (1, 7), WEEK(weather_date, 3), MONTH(weather_date), MONTHNAME(weather_date),
       DATE_FORMAT(weather_date, '%Y-%m'), QUARTER(weather_date), YEAR(weather_date),
       temp_mean_c, temp_anomaly_c, temp_band, anomaly_band, precip_mm, is_rain_day
FROM secondlook_silver.silver_weather_national;

CREATE TABLE dim_customer (
    customer_sk             INT AUTO_INCREMENT PRIMARY KEY,
    customer_key            CHAR(64) NOT NULL UNIQUE,   -- salted SHA-256 business key from Silver
    age_band                VARCHAR(10),
    club_member_status      VARCHAR(20),
    fashion_news_frequency  VARCHAR(20),
    fn                      TINYINT,
    active                  TINYINT
) ENGINE=InnoDB;

INSERT INTO dim_customer (customer_key, age_band, club_member_status, fashion_news_frequency, fn, active)
SELECT customer_key, age_band, club_member_status, fashion_news_frequency, fn, active
FROM secondlook_silver.silver_customers_masked
ORDER BY customer_key;

CREATE TABLE dim_channel (
    channel_sk    TINYINT PRIMARY KEY,
    channel_name  VARCHAR(20) NOT NULL              -- assumption: H&M data only has codes 1 and 2
) ENGINE=InnoDB;

INSERT INTO dim_channel VALUES (1, 'Store'), (2, 'Online');

CREATE TABLE dim_article (
    article_sk                 INT AUTO_INCREMENT PRIMARY KEY,   -- surrogate key (one per version)
    article_id                 CHAR(10) NOT NULL,                -- business key
    prod_name                  VARCHAR(120),
    product_type_name          VARCHAR(60),
    product_group_name         VARCHAR(60),
    colour_group_name          VARCHAR(60),
    graphical_appearance_name  VARCHAR(60),
    department_name            VARCHAR(80),
    section_name               VARCHAR(80),
    index_name                 VARCHAR(60),
    index_group_name           VARCHAR(60),
    sales_tier                 VARCHAR(12) NOT NULL,             -- SCD Type 2 attribute
    tier_units                 INT NOT NULL,                     -- units behind this version's tier
    tier_units_per_week        DECIMAL(10,4) NULL,
    valid_from                 DATE NOT NULL,
    valid_to                   DATE NOT NULL,
    is_current                 BOOLEAN NOT NULL,
    UNIQUE KEY uq_article_version (article_id, valid_from),
    KEY ix_article_current (article_id, is_current)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- 2. Tier snapshots (one procedure, run once per snapshot date)
-- ---------------------------------------------------------------------
CREATE TABLE stg_article_tiers (
    as_of               DATE NOT NULL,
    article_id          CHAR(10) NOT NULL,
    units               INT NOT NULL,
    first_sale_date     DATE NULL,
    units_per_week      DECIMAL(10,4) NULL,
    sales_tier          VARCHAR(12) NOT NULL,
    PRIMARY KEY (as_of, article_id)
) ENGINE=InnoDB;

DELIMITER $$
CREATE PROCEDURE sp_article_tiers(IN p_as_of DATE)
BEGIN
    DELETE FROM stg_article_tiers WHERE as_of = p_as_of;
    INSERT INTO stg_article_tiers (as_of, article_id, units, first_sale_date, units_per_week, sales_tier)
    WITH sales AS (
        SELECT article_id, SUM(quantity) AS units, MIN(order_date) AS first_sale_date
        FROM secondlook_silver.silver_order_lines
        WHERE order_date <= p_as_of
        GROUP BY article_id),
    rated AS (
        SELECT a.article_id, COALESCE(s.units, 0) AS units, s.first_sale_date,
               s.units / (GREATEST(DATEDIFF(p_as_of, s.first_sale_date) + 1, 7) / 7) AS units_per_week,
               (s.first_sale_date IS NOT NULL AND s.first_sale_date <= p_as_of - INTERVAL 28 DAY) AS eligible
        FROM secondlook_silver.silver_articles a
        LEFT JOIN sales s ON s.article_id = a.article_id),
    ranked AS (
        SELECT rated.*,
               CASE WHEN eligible THEN PERCENT_RANK() OVER (PARTITION BY eligible ORDER BY units_per_week) END AS pct_rank
        FROM rated)
    SELECT p_as_of, article_id, units, first_sale_date, units_per_week,
           CASE WHEN units = 0 THEN 'Never Sold'
                WHEN eligible AND pct_rank <= 0.10 THEN 'Slow Mover'
                ELSE 'Normal' END
    FROM ranked;
END$$
DELIMITER ;

-- ---------------------------------------------------------------------
-- 3. SCD2 initial load: version 1 = tier as of 2020-06-30
-- ---------------------------------------------------------------------
CALL sp_article_tiers('2020-06-30');

INSERT INTO dim_article (article_id, prod_name, product_type_name, product_group_name, colour_group_name,
        graphical_appearance_name, department_name, section_name, index_name, index_group_name,
        sales_tier, tier_units, tier_units_per_week, valid_from, valid_to, is_current)
SELECT a.article_id, a.prod_name, a.product_type_name, a.product_group_name, a.colour_group_name,
       a.graphical_appearance_name, a.department_name, a.section_name, a.index_name, a.index_group_name,
       t.sales_tier, t.units, t.units_per_week, DATE '2020-03-01', DATE '9999-12-31', TRUE
FROM secondlook_silver.silver_articles a
JOIN stg_article_tiers t ON t.article_id = a.article_id AND t.as_of = DATE '2020-06-30'
ORDER BY a.article_id;

-- ---------------------------------------------------------------------
-- 4. SCD2 change: recompute tiers as of 2020-09-22, expire changed rows, insert new versions
-- ---------------------------------------------------------------------
CALL sp_article_tiers('2020-09-22');

-- 4a. Expire the current row of every article whose tier changed
UPDATE dim_article d
JOIN stg_article_tiers t ON t.article_id = d.article_id AND t.as_of = DATE '2020-09-22'
SET d.valid_to = DATE '2020-06-30', d.is_current = FALSE
WHERE d.is_current = TRUE AND d.sales_tier <> t.sales_tier;

-- 4b. Insert a new current row for every article that no longer has one
INSERT INTO dim_article (article_id, prod_name, product_type_name, product_group_name, colour_group_name,
        graphical_appearance_name, department_name, section_name, index_name, index_group_name,
        sales_tier, tier_units, tier_units_per_week, valid_from, valid_to, is_current)
SELECT d.article_id, d.prod_name, d.product_type_name, d.product_group_name, d.colour_group_name,
       d.graphical_appearance_name, d.department_name, d.section_name, d.index_name, d.index_group_name,
       t.sales_tier, t.units, t.units_per_week, DATE '2020-07-01', DATE '9999-12-31', TRUE
FROM dim_article d
JOIN stg_article_tiers t ON t.article_id = d.article_id AND t.as_of = DATE '2020-09-22'
WHERE d.valid_to = DATE '2020-06-30' AND d.is_current = FALSE
  AND NOT EXISTS (SELECT 1 FROM (SELECT article_id FROM dim_article WHERE is_current = TRUE) c
                  WHERE c.article_id = d.article_id);

-- ---------------------------------------------------------------------
-- 5. Fact table: each order line joins the article version valid on its order date
-- ---------------------------------------------------------------------
CREATE TABLE fact_sales (
    sales_line_id  INT PRIMARY KEY,                 -- order_item_id from Silver
    order_id       INT NOT NULL,                    -- degenerate dimension
    date_key       INT NOT NULL,
    customer_sk    INT NOT NULL,
    article_sk     INT NOT NULL,
    channel_sk     TINYINT NOT NULL,
    quantity       SMALLINT NOT NULL,
    unit_price     DECIMAL(19,16) NOT NULL,
    revenue        DECIMAL(22,16) NOT NULL,
    CONSTRAINT fk_fact_date     FOREIGN KEY (date_key)    REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_customer FOREIGN KEY (customer_sk) REFERENCES dim_customer (customer_sk),
    CONSTRAINT fk_fact_article  FOREIGN KEY (article_sk)  REFERENCES dim_article (article_sk),
    CONSTRAINT fk_fact_channel  FOREIGN KEY (channel_sk)  REFERENCES dim_channel (channel_sk)
) ENGINE=InnoDB;

INSERT INTO fact_sales
SELECT l.order_item_id, l.order_id, CAST(DATE_FORMAT(l.order_date, '%Y%m%d') AS UNSIGNED),
       c.customer_sk, a.article_sk, l.sales_channel_id, l.quantity, l.price, l.revenue
FROM secondlook_silver.silver_order_lines l
JOIN dim_customer c ON c.customer_key = l.customer_key
JOIN dim_article a  ON a.article_id = l.article_id
                   AND l.order_date BETWEEN a.valid_from AND a.valid_to;

-- =====================================================================
-- 6. Verification (screenshot these)
-- =====================================================================
-- 6a. Row counts and reconciliation: fact lines = 35,620 and units = 39,030 (same as Silver)
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count, NULL AS units FROM dim_date
UNION ALL SELECT 'dim_customer', COUNT(*), NULL FROM dim_customer
UNION ALL SELECT 'dim_channel', COUNT(*), NULL FROM dim_channel
UNION ALL SELECT 'dim_article (all versions)', COUNT(*), NULL FROM dim_article
UNION ALL SELECT 'dim_article (current)', SUM(is_current), NULL FROM dim_article
UNION ALL SELECT 'fact_sales', COUNT(*), SUM(quantity) FROM fact_sales
UNION ALL SELECT 'silver_order_lines', COUNT(*), SUM(quantity) FROM secondlook_silver.silver_order_lines;

-- 6b. SCD2 transitions between the two snapshots
SELECT old_v.sales_tier AS tier_2020_06_30, new_v.sales_tier AS tier_2020_09_22, COUNT(*) AS articles
FROM dim_article old_v
JOIN dim_article new_v ON new_v.article_id = old_v.article_id AND new_v.is_current = TRUE
WHERE old_v.is_current = FALSE
GROUP BY old_v.sales_tier, new_v.sales_tier
ORDER BY articles DESC;

-- 6c. SCD2 proof: one article with its expired and active rows
SELECT article_sk, article_id, prod_name, sales_tier, tier_units, valid_from, valid_to, is_current
FROM dim_article
WHERE article_id = (SELECT MIN(article_id) FROM dim_article WHERE is_current = FALSE
                    AND sales_tier = 'Slow Mover')
ORDER BY valid_from;

-- 6d. Current tiers must match gold_article_sales_tier (1,000 / 11,747 / 1,246 on the full data)
SELECT sales_tier, COUNT(*) AS current_articles FROM dim_article WHERE is_current = TRUE
GROUP BY sales_tier ORDER BY sales_tier;

SET SQL_SAFE_UPDATES = 1;