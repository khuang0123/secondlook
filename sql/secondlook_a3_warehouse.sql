-- =====================================================================
-- SecondLook - Assignment 3: Warehouse SQL script (single submission file)
-- Author: Ingrid (Keke) Huang
--
-- Contents
--   1. Bronze layer (Amazon S3): layout and audit metadata (no SQL DDL; object storage)
--   2. Silver DDL (secondlook_silver)          - tables written by script/03_silver_build.py
--   3. Gold layer ELT (secondlook_gold)        - script/04_gold_build.sql
--   4. Star schema + SCD Type 2 load           - script/05_star_schema_build.sql
--   5. Part 2 analytical queries (Q1-Q5)       - script/06_star_schema_queries.sql
--   6. Part 2 profiling and optimization       - script/07_star_schema_optimization.sql
--   7. Part 4a relational showcase queries     - script/10_part4_sql_showcase.sql
--
-- Layer rule: each step reads ONLY from the layer immediately before it:
--   Bronze (S3) -> Silver -> Gold / star schema -> Neo4j
-- Platform: AWS RDS MySQL 8.4.11 (db.t4g.micro). Run in MySQL Workbench, section by section.
-- No credentials, salts or endpoints appear in this file.
-- =====================================================================

-- =====================================================================
-- SECTION 1. BRONZE LAYER (Amazon S3, bucket kh-uchi-de, us-east-2)
-- Bronze is object storage, so it has no SQL DDL. script/02_land_bronze_s3.py lands files byte-for-byte:
--   bronze/open_meteo/archive/city=<city>/window=<YYYY-MM>/ingest_date=<YYYY-MM-DD>/response.json   (35 objects)
--   bronze/hm_kaggle/transactions/ingest_date=<YYYY-MM-DD>/transactions_sample.csv              (39,030 rows)
--   bronze/hm_kaggle/customers/ingest_date=<YYYY-MM-DD>/customers_sample.csv                    (3,365 rows)
--   bronze/hm_kaggle/articles/ingest_date=<YYYY-MM-DD>/articles_sample.csv                      (13,993 rows)
--   bronze/_manifests/batch_id=<id>.json        one manifest per run: every object, sha256, size, rows
-- Audit metadata: every API response is wrapped with ingested_at, source_name, request params and status;
-- every S3 object carries user metadata source-name, ingested-at, sha256, batch-id.
-- Immutable: each run lands under its own ingest_date; objects with an identical sha256 are skipped.
-- Bucket: Block Public Access on, default SSE encryption, versioning enabled.
-- =====================================================================


-- =====================================================================
-- SECTION 2. SILVER DDL (secondlook_silver)
-- Created and loaded by script/03_silver_build.py, which reads ONLY from S3 Bronze.
-- PII: customer_id and postal_code are re-hashed with a secret salt (SHA-256) before any write;
-- age is generalized to a 5-year band. silver_quarantine and silver_run_log are append-only.
-- =====================================================================
CREATE DATABASE IF NOT EXISTS secondlook_silver CHARACTER SET utf8mb4;
USE secondlook_silver;

CREATE TABLE IF NOT EXISTS silver_weather_daily (
    city VARCHAR(40) NOT NULL, weather_date DATE NOT NULL, latitude DECIMAL(8,4), longitude DECIMAL(8,4),
    temperature_2m_max DECIMAL(5,2), temperature_2m_min DECIMAL(5,2), temperature_2m_mean DECIMAL(5,2),
    precipitation_sum DECIMAL(6,2), rain_sum DECIMAL(6,2), snowfall_sum DECIMAL(6,2), weather_code SMALLINT,
    ingested_at DATETIME, bronze_key VARCHAR(255), PRIMARY KEY (city, weather_date)) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS silver_weather_national (
    weather_date DATE PRIMARY KEY, temp_mean_c DECIMAL(5,2), temp_max_c DECIMAL(5,2), temp_min_c DECIMAL(5,2),
    precip_mm DECIMAL(6,2), snowfall_cm DECIMAL(6,2), cities_reporting TINYINT, month CHAR(7),
    temp_anomaly_c DECIMAL(5,2), temp_band VARCHAR(10), anomaly_band VARCHAR(20), is_rain_day BOOLEAN) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS silver_customers_masked (
    customer_key CHAR(64) PRIMARY KEY, postal_key CHAR(64), age_band VARCHAR(10),
    club_member_status VARCHAR(20), fashion_news_frequency VARCHAR(20), fn TINYINT, active TINYINT) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS silver_articles (
    article_id CHAR(10) PRIMARY KEY, product_code CHAR(7), prod_name VARCHAR(120),
    product_type_no INT, product_type_name VARCHAR(60), product_group_name VARCHAR(60),
    graphical_appearance_no INT, graphical_appearance_name VARCHAR(60),
    colour_group_code INT, colour_group_name VARCHAR(60),
    perceived_colour_value_id INT, perceived_colour_value_name VARCHAR(60),
    perceived_colour_master_id INT, perceived_colour_master_name VARCHAR(60),
    department_no INT, department_name VARCHAR(80), index_code CHAR(1), index_name VARCHAR(60),
    index_group_no INT, index_group_name VARCHAR(60), section_no INT, section_name VARCHAR(80),
    garment_group_no INT, garment_group_name VARCHAR(60), detail_desc TEXT) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS silver_order_lines (
    order_item_id INT PRIMARY KEY, order_id INT NOT NULL, customer_key CHAR(64) NOT NULL,
    order_date DATE NOT NULL, sales_channel_id TINYINT NOT NULL, article_id CHAR(10) NOT NULL,
    quantity SMALLINT NOT NULL, price DECIMAL(19,16) NOT NULL, revenue DECIMAL(22,16) NOT NULL,
    UNIQUE KEY uq_line (order_id, article_id, price), KEY ix_lines_date (order_date),
    KEY ix_lines_customer (customer_key), KEY ix_lines_article (article_id)) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS silver_order_lines_weather (
    order_item_id INT PRIMARY KEY, order_id INT NOT NULL, customer_key CHAR(64) NOT NULL,
    order_date DATE NOT NULL, sales_channel_id TINYINT NOT NULL, article_id CHAR(10) NOT NULL,
    quantity SMALLINT NOT NULL, price DECIMAL(19,16) NOT NULL, revenue DECIMAL(22,16) NOT NULL,
    temp_mean_c DECIMAL(5,2), temp_anomaly_c DECIMAL(5,2), precip_mm DECIMAL(6,2),
    temp_band VARCHAR(10), anomaly_band VARCHAR(20), is_rain_day BOOLEAN, KEY ix_lw_date (order_date)) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS silver_quarantine (
    id BIGINT AUTO_INCREMENT PRIMARY KEY, run_id CHAR(12), quarantined_at DATETIME,
    source VARCHAR(20), reject_reasons VARCHAR(255), record TEXT) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS silver_run_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY, run_id CHAR(12), run_at DATETIME,
    metric VARCHAR(64), value DOUBLE, detail VARCHAR(255)) ENGINE=InnoDB;


-- =====================================================================
-- SECTION 3. GOLD LAYER ELT (secondlook_gold)
-- Source: script/04_gold_build.sql
-- =====================================================================
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


-- =====================================================================
-- SECTION 4. STAR SCHEMA + SCD TYPE 2
-- Source: script/05_star_schema_build.sql
-- =====================================================================
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


-- =====================================================================
-- SECTION 5. PART 2 ANALYTICAL QUERIES (Q1-Q5, Q1 uses WITH ROLLUP)
-- Source: script/06_star_schema_queries.sql
-- =====================================================================
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


-- =====================================================================
-- SECTION 6. PART 2 QUERY PROFILING AND OPTIMIZATION
-- Source: script/07_star_schema_optimization.sql
-- =====================================================================
-- =====================================================================
-- SecondLook - Assignment 3, Part 2: Query profiling & optimization
-- Most expensive query: Q1 (index group x sales tier WITH ROLLUP), 229 ms baseline.
--   Q1 229 ms | Q4 212 ms | Q2 146 ms | Q5 127 ms | Q3 113 ms   (EXPLAIN ANALYZE top line)
--
-- Baseline plan: scan + sort dim_article (18,650 rows), then for each article an index lookup into
-- fact_sales through the foreign-key index on article_sk. That index holds only article_sk, so every
-- one of the 35,620 matching fact rows needs a second read of the table row to get quantity, revenue
-- and order_id. The join step alone took 165 ms of the 229 ms.
--
-- Hypothesis: a composite covering index on fact_sales(article_sk, quantity, revenue, order_id)
-- lets MySQL read everything Q1 needs from fact_sales from the index alone (covering index lookup),
-- removing the 35,620 table-row reads. Run each step separately; run each EXPLAIN ANALYZE 2-3 times.
-- =====================================================================
USE secondlook_gold;

-- ---------------------------------------------------------------------
-- STEP 1. BEFORE (you already have this: 229 ms). Re-run only if you need a fresh baseline.
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
-- STEP 2. Create the covering index
-- ---------------------------------------------------------------------
CREATE INDEX idx_fact_article_cover ON fact_sales (article_sk, quantity, revenue, order_id);
ANALYZE TABLE fact_sales;
SHOW INDEX FROM fact_sales;

-- ---------------------------------------------------------------------
-- STEP 3. AFTER: same query, same EXPLAIN ANALYZE
-- Look for: "Covering index lookup on f using idx_fact_article_cover"
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

-- Note: because the new index starts with article_sk, MySQL uses it to enforce the
-- fk_fact_article foreign key and silently drops the original single-column FK index.
-- So the covering index replaces that index rather than duplicating it.

-- ---------------------------------------------------------------------
-- RESET (only to re-measure the baseline): uncomment and run BOTH lines, in this order.
-- The FK needs an index on article_sk, so recreate the plain one before dropping the covering one.
-- ---------------------------------------------------------------------
-- CREATE INDEX fk_fact_article ON fact_sales (article_sk);
-- DROP INDEX idx_fact_article_cover ON fact_sales;


-- =====================================================================
-- SECTION 7. PART 4a RELATIONAL WAREHOUSE SHOWCASE (3 queries)
-- Source: script/10_part4_sql_showcase.sql
-- =====================================================================
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
