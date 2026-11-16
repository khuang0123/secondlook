"""
Assignment 3, Part 1, Step 3: Silver layer, built ONLY from Bronze (S3).

Layer rule: every step reads only from the step immediately before it.
  Bronze (S3)  ->  Silver (MySQL secondlook_silver)  ->  Gold (MySQL secondlook_gold)  ->  ...
  The Assignment 2 database (secondlook) is NOT read here.

Inputs (latest ingest_date per object in S3 Bronze)
  bronze/open_meteo/archive/city=*/window=*/ingest_date=*/response.json
  bronze/hm_kaggle/transactions/ingest_date=*/transactions_sample.csv
  bronze/hm_kaggle/customers/ingest_date=*/customers_sample.csv
  bronze/hm_kaggle/articles/ingest_date=*/articles_sample.csv

Outputs (MySQL database secondlook_silver)
  silver_weather_daily          typed city x date weather that passed all checks
  silver_weather_national       one row per date: 5-city mean, monthly anomaly, temp band, rain flag
  silver_customers_masked       salted SHA-256 customer_key / postal_key, 5-year age band, standardized casing
  silver_articles               all articles (incl. unsold), typed and denormalized with category names
  silver_order_lines            one row per order line (customer + date + channel = order), masked key only
  silver_order_lines_weather    order lines enriched with that day's national weather
  silver_quarantine             every rejected row (weather and H&M) with source and reason code
  silver_run_log                quality-check, quarantine and reconciliation counts per run

PII: customer_id and postal_code are public SHA-256 hashes in the Kaggle data, so they are re-hashed
with a secret salt (SILVER_SALT env var or prompt). Raw IDs exist only in Bronze (private, encrypted S3)
and are never written to Silver.

Setup
  pip install boto3 pandas sqlalchemy pymysql
  aws configure
  export MYSQL_HOST=<rds-endpoint> MYSQL_USER=<user>
  export SILVER_SALT=<long random string; reuse it every run>
Run
  python script/03_silver_build.py --bucket <your-bucket>
"""
import argparse
import hashlib
import io
import json
import os
import uuid
from datetime import datetime, timezone
from getpass import getpass
from urllib.parse import quote_plus

import boto3
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

LAKE_DB = os.environ.get("MYSQL_SILVER_DB", "secondlook_silver")
RUN_ID = uuid.uuid4().hex[:12]
RUN_AT = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
DAILY_VARS = ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
              "precipitation_sum", "rain_sum", "snowfall_sum", "weather_code"]
WMO_CODES = {0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 71, 73, 75, 77,
             80, 81, 82, 85, 86, 95, 96, 99}
run_log, quarantine = [], []


# ------------------------------------------------------------------ logging helpers
def log(metric, value, detail=""):
    run_log.append({"run_id": RUN_ID, "run_at": RUN_AT, "metric": metric, "value": float(value), "detail": detail})
    shown = f"{value:,.4f}" if isinstance(value, float) and not float(value).is_integer() else f"{int(value):,}"
    print(f"  {metric:<44} {shown:>14}  {detail}")


def apply_checks(df, source, checks):
    """checks: {reason: boolean mask of FAILING rows}. Logs counts, quarantines failures, returns passing rows."""
    failed_any = pd.Series(False, index=df.index)
    reasons = pd.Series("", index=df.index)
    for reason, mask in checks.items():
        mask = mask.reindex(df.index).fillna(False).astype(bool)
        log(f"{source}_check_{reason}_failed", int(mask.sum()), f"passed={int((~mask).sum()):,}")
        reasons = reasons.where(~mask, reasons.str.cat([reason] * len(df), sep=";").str.lstrip(";"))
        failed_any |= mask
    for idx in df.index[failed_any]:
        quarantine.append({"run_id": RUN_ID, "quarantined_at": RUN_AT, "source": source,
                           "reject_reasons": reasons[idx],
                           "record": json.dumps(df.loc[idx].to_dict(), default=str)[:4000]})
    log(f"{source}_rows_passed", int((~failed_any).sum()))
    log(f"{source}_rows_quarantined", int(failed_any.sum()))
    return df[~failed_any].copy()


# ------------------------------------------------------------------ connections
def lake_engine():
    pw = os.environ.get("MYSQL_PASSWORD") or getpass("MySQL password: ")
    base = (f"mysql+pymysql://{os.environ['MYSQL_USER']}:{quote_plus(pw)}"
            f"@{os.environ['MYSQL_HOST']}:{os.environ.get('MYSQL_PORT', '3306')}")
    sock = f"?unix_socket={os.environ['MYSQL_SOCKET']}" if os.environ.get("MYSQL_SOCKET") else ""
    with create_engine(base + "/" + sock).begin() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {LAKE_DB} CHARACTER SET utf8mb4"))
    return create_engine(f"{base}/{LAKE_DB}{sock}")


def get_salt():
    salt = os.environ.get("SILVER_SALT") or getpass("Masking salt (SILVER_SALT): ")
    if len(salt) < 16:
        raise SystemExit("Use a salt of at least 16 characters, and reuse the same one every run.")
    return salt


# ------------------------------------------------------------------ Bronze readers
def latest_keys(s3, bucket, prefix, group_parts):
    """Newest ingest_date object per group, e.g. per (city, window)."""
    best = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            parts = dict(p.split("=", 1) for p in obj["Key"].split("/") if "=" in p)
            group = tuple(parts.get(g) for g in group_parts)
            if group not in best or parts.get("ingest_date", "") > best[group][0]:
                best[group] = (parts.get("ingest_date", ""), obj["Key"])
    return sorted(k for _, k in best.values())


def read_bronze_csv(s3, bucket, dataset):
    keys = latest_keys(s3, bucket, f"bronze/hm_kaggle/{dataset}/", [])
    if not keys:
        raise SystemExit(f"No Bronze file found under bronze/hm_kaggle/{dataset}/ - run Step 2 first.")
    body = s3.get_object(Bucket=bucket, Key=keys[0])["Body"].read()
    df = pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False, na_values=[""])
    print(f"read {len(df):,} rows from s3://{bucket}/{keys[0]}")
    return df


def read_bronze_weather(s3, bucket):
    rows = []
    keys = latest_keys(s3, bucket, "bronze/open_meteo/archive/", ["city", "window"])
    for key in keys:
        doc = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        params = doc.get("params", {})
        daily = (doc.get("response") or {}).get("daily") or {}
        city = next((p.split("=", 1)[1] for p in key.split("/") if p.startswith("city=")), None)
        for i, day in enumerate(daily.get("time") or []):
            row = {"city": city, "latitude": params.get("latitude"), "longitude": params.get("longitude"),
                   "weather_date": day, "ingested_at": doc.get("ingested_at"), "bronze_key": key}
            for v in DAILY_VARS:
                arr = daily.get(v) or []
                row[v] = arr[i] if i < len(arr) else None
            rows.append(row)
    print(f"read {len(keys)} weather objects ({len(rows):,} city-days) from s3://{bucket}/bronze/open_meteo/")
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ weather -> Silver
def silver_weather(raw, start, end):
    df = raw.copy()
    df["weather_date"] = pd.to_datetime(df["weather_date"], format="%Y-%m-%d", errors="coerce")
    for v in DAILY_VARS:
        df[v] = pd.to_numeric(df[v], errors="coerce")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True, errors="coerce").dt.tz_localize(None)
    temps = df[["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"]]
    print("\n=== WEATHER QUALITY CHECKS ===")
    passed = apply_checks(df, "weather", {
        "null_required_field": df[["temperature_2m_max", "temperature_2m_min",
                                   "temperature_2m_mean", "precipitation_sum"]].isna().any(axis=1),
        "duplicate_city_date": df.duplicated(["city", "weather_date"], keep="first"),
        "out_of_range": ((temps < -40) | (temps > 45)).any(axis=1) | (df["precipitation_sum"] < 0)
                        | (df["temperature_2m_min"] > df["temperature_2m_max"])
                        | (df["weather_code"].notna() & ~df["weather_code"].isin(WMO_CODES)),
        "bad_date_or_outside_window": df["weather_date"].isna() | (df["weather_date"] < start) | (df["weather_date"] > end),
    })
    nat = (passed.groupby("weather_date")
           .agg(temp_mean_c=("temperature_2m_mean", "mean"), temp_max_c=("temperature_2m_max", "mean"),
                temp_min_c=("temperature_2m_min", "mean"), precip_mm=("precipitation_sum", "mean"),
                snowfall_cm=("snowfall_sum", "mean"), cities_reporting=("city", "nunique"))
           .reset_index())
    nat["month"] = nat["weather_date"].dt.to_period("M").astype(str)
    nat["temp_anomaly_c"] = nat["temp_mean_c"] - nat.groupby("month")["temp_mean_c"].transform("mean")
    nat["temp_band"] = pd.cut(nat["temp_mean_c"], [-np.inf, 10, 18, np.inf],
                              labels=["cold", "mild", "warm"], right=False).astype(str)
    nat["anomaly_band"] = pd.cut(nat["temp_anomaly_c"], [-np.inf, -2, 2, np.inf],
                                 labels=["colder_than_usual", "typical", "warmer_than_usual"]).astype(str)
    nat["is_rain_day"] = nat["precip_mm"] >= 1.0
    nat = nat.round({c: 2 for c in ["temp_mean_c", "temp_max_c", "temp_min_c", "precip_mm",
                                     "snowfall_cm", "temp_anomaly_c"]})
    return passed, nat


# ------------------------------------------------------------------ H&M -> Silver
def salted(series, salt):
    return series.map(lambda v: None if pd.isna(v) else hashlib.sha256((salt + str(v)).encode()).hexdigest())


def age_band(age):
    if pd.isna(age) or not (15 <= age <= 100):
        return "unknown"
    lo = int(age) // 5 * 5
    return f"{lo}-{lo + 4}"


def silver_articles(raw):
    df = raw.copy()
    df["article_id"] = df["article_id"].str.strip().str.zfill(10)      # conform: 10 digits with leading zero
    df["product_code"] = df["product_code"].str.strip().str.zfill(7)
    int_cols = ["product_type_no", "graphical_appearance_no", "colour_group_code", "perceived_colour_value_id",
                "perceived_colour_master_id", "department_no", "index_group_no", "section_no", "garment_group_no"]
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    print("\n=== ARTICLE QUALITY CHECKS ===")
    return apply_checks(df, "articles", {
        "null_article_id_or_name": df["article_id"].isna() | df["prod_name"].isna(),
        "duplicate_article_id": df.duplicated("article_id", keep="first"),
        "null_category_codes": df[["product_type_no", "department_no", "index_code", "index_group_no"]].isna().any(axis=1),
    })


def silver_customers(raw, salt):
    df = raw.copy()
    df["age_num"] = pd.to_numeric(df["age"], errors="coerce")
    print("\n=== CUSTOMER QUALITY CHECKS ===")
    ok = apply_checks(df, "customers", {
        "null_customer_id": df["customer_id"].isna(),
        "duplicate_customer_id": df.duplicated("customer_id", keep="first"),
    })
    out = pd.DataFrame({
        "customer_key": salted(ok["customer_id"], salt),
        "postal_key": salted(ok["postal_code"], salt),
        "age_band": ok["age_num"].map(age_band),
        "club_member_status": ok["club_member_status"].fillna("UNKNOWN").str.upper(),
        "fashion_news_frequency": ok["fashion_news_frequency"].fillna("UNKNOWN").str.upper(),  # fixes mixed casing
        "fn": pd.to_numeric(ok["FN"], errors="coerce").fillna(0).astype(int),
        "active": pd.to_numeric(ok["Active"], errors="coerce").fillna(0).astype(int),
    })
    log("customers_masked", len(out), "customer_id + postal_code -> salted SHA-256; age -> 5-year band")
    return out, dict(zip(ok["customer_id"], out["customer_key"]))      # key map kept in memory only


def silver_order_lines(raw_tx, key_map, article_ids, start, end):
    df = raw_tx.copy()
    df["article_id"] = df["article_id"].str.strip().str.zfill(10)
    df["order_date"] = pd.to_datetime(df["t_dat"], format="%Y-%m-%d", errors="coerce")
    df["price_num"] = pd.to_numeric(df["price"], errors="coerce")
    df["channel"] = pd.to_numeric(df["sales_channel_id"], errors="coerce").astype("Int64")
    print("\n=== TRANSACTION QUALITY CHECKS ===")
    ok = apply_checks(df, "transactions", {
        "null_required_field": df[["t_dat", "customer_id", "article_id", "price", "sales_channel_id"]].isna().any(axis=1),
        "bad_date_or_outside_window": df["order_date"].isna() | (df["order_date"] < start) | (df["order_date"] > end),
        "price_out_of_range": ~df["price_num"].between(0, 1, inclusive="right"),
        "invalid_channel": ~df["channel"].isin([1, 2]),
        "orphan_customer": ~df["customer_id"].isin(key_map.keys()),
        "orphan_article": ~df["article_id"].isin(article_ids),
    })
    ok["customer_key"] = ok["customer_id"].map(key_map)
    log("revenue_of_passed_transactions", round(float(ok["price_num"].sum()), 10))
    # Order = one customer, one date, one channel (same business rule as Assignment 2).
    # Identical units (same order, article, price) collapse into quantity; different prices stay separate lines.
    lines = (ok.groupby(["customer_key", "order_date", "channel", "article_id", "price"], dropna=False)
               .agg(quantity=("price", "size"), price_num=("price_num", "first"))
               .reset_index())
    orders = (lines[["customer_key", "order_date", "channel"]].drop_duplicates()
              .sort_values(["order_date", "customer_key", "channel"]).reset_index(drop=True))
    orders["order_id"] = np.arange(1, len(orders) + 1)
    lines = lines.merge(orders, on=["customer_key", "order_date", "channel"])
    lines = lines.sort_values(["order_id", "article_id", "price"]).reset_index(drop=True)
    lines["order_item_id"] = np.arange(1, len(lines) + 1)
    lines["revenue"] = lines["quantity"] * lines["price_num"]
    out = lines.rename(columns={"channel": "sales_channel_id"})[
        ["order_item_id", "order_id", "customer_key", "order_date", "sales_channel_id",
         "article_id", "quantity", "price_num", "revenue"]].rename(columns={"price_num": "price"})
    log("orders_built", len(orders))
    log("order_lines_built", len(out))
    return out


# ------------------------------------------------------------------ reconciliation (Bronze in = Silver out + quarantine)
def reconcile(name, bronze_rows, silver_rows, quarantined_rows):
    ok = bronze_rows == silver_rows + quarantined_rows
    log(f"reconcile_{name}", bronze_rows,
        f"bronze={bronze_rows:,} silver={silver_rows:,} quarantined={quarantined_rows:,} -> "
        f"{'MATCH' if ok else 'MISMATCH'}")
    return ok


# ------------------------------------------------------------------ write Silver
DDL = {
    "silver_weather_daily": """(
        city VARCHAR(40) NOT NULL, weather_date DATE NOT NULL, latitude DECIMAL(8,4), longitude DECIMAL(8,4),
        temperature_2m_max DECIMAL(5,2), temperature_2m_min DECIMAL(5,2), temperature_2m_mean DECIMAL(5,2),
        precipitation_sum DECIMAL(6,2), rain_sum DECIMAL(6,2), snowfall_sum DECIMAL(6,2), weather_code SMALLINT,
        ingested_at DATETIME, bronze_key VARCHAR(255), PRIMARY KEY (city, weather_date))""",
    "silver_weather_national": """(
        weather_date DATE PRIMARY KEY, temp_mean_c DECIMAL(5,2), temp_max_c DECIMAL(5,2), temp_min_c DECIMAL(5,2),
        precip_mm DECIMAL(6,2), snowfall_cm DECIMAL(6,2), cities_reporting TINYINT, month CHAR(7),
        temp_anomaly_c DECIMAL(5,2), temp_band VARCHAR(10), anomaly_band VARCHAR(20), is_rain_day BOOLEAN)""",
    "silver_customers_masked": """(
        customer_key CHAR(64) PRIMARY KEY, postal_key CHAR(64), age_band VARCHAR(10),
        club_member_status VARCHAR(20), fashion_news_frequency VARCHAR(20), fn TINYINT, active TINYINT)""",
    "silver_articles": """(
        article_id CHAR(10) PRIMARY KEY, product_code CHAR(7), prod_name VARCHAR(120),
        product_type_no INT, product_type_name VARCHAR(60), product_group_name VARCHAR(60),
        graphical_appearance_no INT, graphical_appearance_name VARCHAR(60),
        colour_group_code INT, colour_group_name VARCHAR(60),
        perceived_colour_value_id INT, perceived_colour_value_name VARCHAR(60),
        perceived_colour_master_id INT, perceived_colour_master_name VARCHAR(60),
        department_no INT, department_name VARCHAR(80), index_code CHAR(1), index_name VARCHAR(60),
        index_group_no INT, index_group_name VARCHAR(60), section_no INT, section_name VARCHAR(80),
        garment_group_no INT, garment_group_name VARCHAR(60), detail_desc TEXT)""",
    "silver_order_lines": """(
        order_item_id INT PRIMARY KEY, order_id INT NOT NULL, customer_key CHAR(64) NOT NULL,
        order_date DATE NOT NULL, sales_channel_id TINYINT NOT NULL, article_id CHAR(10) NOT NULL,
        quantity SMALLINT NOT NULL, price DECIMAL(19,16) NOT NULL, revenue DECIMAL(22,16) NOT NULL,
        UNIQUE KEY uq_line (order_id, article_id, price), KEY ix_lines_date (order_date),
        KEY ix_lines_customer (customer_key), KEY ix_lines_article (article_id))""",
    "silver_order_lines_weather": """(
        order_item_id INT PRIMARY KEY, order_id INT NOT NULL, customer_key CHAR(64) NOT NULL,
        order_date DATE NOT NULL, sales_channel_id TINYINT NOT NULL, article_id CHAR(10) NOT NULL,
        quantity SMALLINT NOT NULL, price DECIMAL(19,16) NOT NULL, revenue DECIMAL(22,16) NOT NULL,
        temp_mean_c DECIMAL(5,2), temp_anomaly_c DECIMAL(5,2), precip_mm DECIMAL(6,2),
        temp_band VARCHAR(10), anomaly_band VARCHAR(20), is_rain_day BOOLEAN, KEY ix_lw_date (order_date))""",
    "silver_quarantine": """(
        id BIGINT AUTO_INCREMENT PRIMARY KEY, run_id CHAR(12), quarantined_at DATETIME,
        source VARCHAR(20), reject_reasons VARCHAR(255), record TEXT)""",
    "silver_run_log": """(
        id BIGINT AUTO_INCREMENT PRIMARY KEY, run_id CHAR(12), run_at DATETIME,
        metric VARCHAR(64), value DOUBLE, detail VARCHAR(255))""",
}
APPEND_ONLY = {"silver_quarantine", "silver_run_log"}       # history kept across runs; everything else is rebuilt


def write(lake, name, df):
    with lake.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {name} {DDL[name]} ENGINE=InnoDB"))
        if name not in APPEND_ONLY:
            conn.execute(text(f"DELETE FROM {name}"))
        if len(df):
            df.to_sql(name, conn, if_exists="append", index=False, chunksize=1000, method="multi")
    print(f"  {name:<28} {len(df):>8,} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--start", default="2020-03-01")
    ap.add_argument("--end", default="2020-09-22")
    args = ap.parse_args()
    start, end = pd.Timestamp(args.start), pd.Timestamp(args.end)

    salt = get_salt()
    s3 = boto3.client("s3", region_name=args.region)
    lake = lake_engine()
    print(f"run {RUN_ID}: s3://{args.bucket}/bronze -> {LAKE_DB} (Silver)\n")

    weather_raw = read_bronze_weather(s3, args.bucket)
    tx_raw = read_bronze_csv(s3, args.bucket, "transactions")
    cust_raw = read_bronze_csv(s3, args.bucket, "customers")
    art_raw = read_bronze_csv(s3, args.bucket, "articles")

    weather, nat = silver_weather(weather_raw, start, end)
    articles = silver_articles(art_raw)
    customers, key_map = silver_customers(cust_raw, salt)
    lines = silver_order_lines(tx_raw, key_map, set(articles["article_id"]), start, end)

    enriched = lines.merge(nat[["weather_date", "temp_mean_c", "temp_anomaly_c", "precip_mm", "temp_band",
                                "anomaly_band", "is_rain_day"]],
                           left_on="order_date", right_on="weather_date", how="left").drop(columns="weather_date")
    print("\n=== ENRICHMENT ===")
    log("order_lines_without_weather", int(enriched["temp_mean_c"].isna().sum()), "should be 0")

    print("\n=== RECONCILIATION: Bronze in = Silver out + quarantined ===")
    q = pd.Series([r["source"] for r in quarantine]).value_counts() if quarantine else pd.Series(dtype=int)
    reconcile("weather_city_days", len(weather_raw), len(weather), int(q.get("weather", 0)))
    reconcile("customers", len(cust_raw), len(customers), int(q.get("customers", 0)))
    reconcile("articles", len(art_raw), len(articles), int(q.get("articles", 0)))
    reconcile("transaction_units", len(tx_raw), int(lines["quantity"].sum()), int(q.get("transactions", 0)))
    passed_rev = next(r["value"] for r in run_log if r["metric"] == "revenue_of_passed_transactions")
    diff = round(passed_rev - float(lines["revenue"].sum()), 10)
    log("revenue_passed_minus_silver", diff, "MATCH" if abs(diff) < 1e-8 else "MISMATCH")

    print(f"\n=== WRITING SILVER to {LAKE_DB} ===")
    write(lake, "silver_weather_daily", weather)
    write(lake, "silver_weather_national", nat)
    write(lake, "silver_customers_masked", customers)
    write(lake, "silver_articles", articles)
    write(lake, "silver_order_lines", lines)
    write(lake, "silver_order_lines_weather", enriched)
    write(lake, "silver_quarantine", pd.DataFrame(quarantine))
    write(lake, "silver_run_log", pd.DataFrame(run_log))


if __name__ == "__main__":
    main()