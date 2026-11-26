# SecondLook – Data Warehouse & Polyglot Persistence

SecondLook finds never-sold and slow-moving H&M articles and the customers most likely to buy them. This repo extends the Assignment 1 architecture and the Assignment 2 relational model with live weather data from the Open-Meteo API, a Medallion pipeline, an SCD Type 2 star schema on AWS RDS MySQL, and a Neo4j property graph for relationship-based targeting.

## Architecture

```
Kaggle H&M ──00──► sample CSVs ─┐
Open-Meteo API ──01──► raw JSON ─┴─02──► Bronze (S3) ──03──► Silver (MySQL) ──04──► Gold (MySQL)
                                                                     └──05──► Star schema + SCD2 (MySQL) ──08──► Neo4j Aura
```

Each step reads **only** from the layer immediately before it.

| Layer | Store | Contents |
|---|---|---|
| Bronze | Amazon S3 (`bronze/` prefix, us-east-2) | 35 raw API responses + 3 H&M sample CSVs, immutable `ingest_date` partitions, audit metadata, per-run manifest |
| Silver | RDS MySQL `secondlook_silver` | 8 typed, quality-checked, PII-masked, weather-enriched tables |
| Gold | RDS MySQL `secondlook_gold` | 4 summary tables + star schema (`fact_sales`, `dim_date`, `dim_customer`, `dim_channel`, `dim_article` SCD2) |
| Graph | Neo4j AuraDB Free | `Customer`, `Article`, `ProductType`, `Colour`, `Pattern`; `PURCHASED`, `OF_TYPE`, `HAS_COLOUR`, `HAS_PATTERN` |

## Repository layout

```
script/
  00_build_sample.py               Lineage: Kaggle originals -> 3 sample CSVs (reused from Assignment 2)
  01_ingest_weather.py             Part 1a: Open-Meteo ingestion (windowed pagination, rate limit, retries, 4 QC checks)
  02_land_bronze_s3.py             Part 1b: Bronze landing in S3 with audit metadata + manifest
  03_silver_build.py               Part 1b: Silver from Bronze (typing, QC + quarantine, salted SHA-256 masking, enrichment)
  04_gold_build.sql                Part 1b: Gold summary tables (SQL ELT from Silver)
  05_star_schema_build.sql         Part 2: star schema + SCD Type 2 (stored procedure sp_article_tiers)
  06_star_schema_queries.sql       Part 2: 5 analytical queries (Q1 uses WITH ROLLUP)
  07_star_schema_optimization.sql  Part 2: EXPLAIN ANALYZE profiling + covering index
  08_load_neo4j.py                 Part 3: graph load from the Gold star schema
  09_graph_queries.cypher          Part 3 + 4b: load statements and all Cypher queries
  10_part4_sql_showcase.sql        Part 4a: 3 relational showcase queries
  A2_hm_normalization.ipynb        Assignment 2 3NF work (source system; not part of the A3 run)
sql/
  secondlook_a3_warehouse.sql      Single submission script: Bronze notes, Silver DDL, Gold, star schema, all SQL queries
```

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
aws configure                      # or: aws sso login --profile <name>; export AWS_PROFILE=<name>
cp .env.example .env               # fill in locally; .env is git-ignored
set -a && source .env && set +a
```

Environment variables: `MYSQL_HOST`, `MYSQL_USER`, `SILVER_SALT`, `NEO4J_URI`, `NEO4J_USER` (optional: `MYSQL_PORT`, `MYSQL_SILVER_DB`, `MYSQL_GOLD_DB`, `MYSQL_SOCKET`). MySQL and Neo4j passwords are prompted at runtime.

## Run order

Run every command from the repository root.

| # | Command | Expected result |
|---|---|---|
| 00 | `python script/00_build_sample.py` (Kaggle CSVs in `raw/`) | `data/bronze/hm/` with 39,030 transactions, 3,365 customers, 13,993 articles |
| 01 | `python script/01_ingest_weather.py` | 35/35 requests OK, 1,030 records, all 4 checks pass; files in `data/bronze/weather/` |
| 02 | `python script/02_land_bronze_s3.py --bucket <bucket> --region us-east-2` (add `--create-bucket` the first time) | 38 objects + manifest in `s3://<bucket>/bronze/` |
| 03 | `python script/03_silver_build.py --bucket <bucket> --region us-east-2` | 8 Silver tables; 10,915 orders, 35,620 order lines; all reconciliations MATCH |
| 04 | Run `script/04_gold_build.sql` in MySQL Workbench | 4 Gold tables; every units total = 39,030 |
| 05 | Run `script/05_star_schema_build.sql` | `fact_sales` 35,620 rows; `dim_article` 18,650 versions / 13,993 current |
| 06 | Run `script/06_star_schema_queries.sql` | Q1–Q5 results and timings |
| 07 | Run `script/07_star_schema_optimization.sql` step by step | Q1: 229 ms → 154 ms with `idx_fact_article_cover` |
| 08 | `python script/08_load_neo4j.py` | 17,539 nodes, 75,983 relationships, 39,030 PURCHASED units |
| 09 | Run `script/09_graph_queries.cypher` in the Aura Query tool, one statement at a time | Checks, visualization, Q3.1–Q3.3, Q4.1–Q4.3 |
| 10 | Run `script/10_part4_sql_showcase.sql` | 4a.1–4a.3 results |

`sql/secondlook_a3_warehouse.sql` bundles steps 04–07 and 10 with the Silver DDL, so the warehouse can be reviewed or rebuilt from one file.

## Data and privacy

- **Data is not committed.** Download the H&M Personalized Fashion Recommendations dataset from Kaggle into `raw/`. Weather data comes from [Open-Meteo.com](https://open-meteo.com/) under CC BY 4.0.
- **PII:** Kaggle's `customer_id` and `postal_code` are public SHA-256 hashes, so Silver re-hashes them with a secret salt (`SILVER_SALT`) and generalizes age to 5-year bands. Raw identifiers exist only in the private, encrypted Bronze bucket; Gold and Neo4j see only the masked key.
- **Credentials** are read from environment variables or prompts and never written to code, logs or the database.

## Notes

- Open-Meteo has no native pagination, so `01` paginates over monthly date windows × 5 Swedish cities (35 requests).
- Weather joins to sales by date through a 5-city national average (postal codes are hashed and country is not identified); weather effects are associations, not causation.
- Sales channel labels (1 = Store, 2 = Online) are an assumption.
- The S3 bucket (us-east-2) and RDS (us-east-1) are in different regions; this was kept and documented.
- Revenue is in H&M's scaled price units, not real currency.
