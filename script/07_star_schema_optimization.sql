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
