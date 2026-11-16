"""
Assignment 2 - Part 1: build the H&M sample and the flat (denormalized) table.

Run this on the ORIGINAL Kaggle files (transactions_train.csv, customers.csv, articles.csv).
It reads transactions in chunks, so it works on a laptop.

Outputs (in OUT_DIR):
  transactions_sample.csv   - every purchase by the sampled customers inside the date window
  customers_sample.csv      - those customers
  articles_sample.csv       - articles they bought + a set of unsold articles (the long tail)
  hm_flat.csv               - ONE wide table: transactions joined to customers and articles
"""
import numpy as np
import pandas as pd
from pathlib import Path

# ---------------- settings ----------------
DATA_DIR = Path("raw")          # folder with the 3 Kaggle CSVs
OUT_DIR1 = Path("data/bronze/hm")
OUT_DIR2 = Path("data/silver/hm")
START_DATE = "2020-03-01"            # window start (data ends 2020-09-22)
N_CUSTOMERS = 6000                   # customers to draw; many won't buy in the window
MAX_ROWS = 40000                     # cap on transaction rows (assignment: 5,000-50,000)
N_NEVER_SOLD = 300                   # articles with zero sales in the FULL dataset
N_UNSOLD_IN_WINDOW = 700             # articles not bought by the sample in the window
CHUNK = 2_000_000
SEED = 42
# ------------------------------------------

rng = np.random.default_rng(SEED)
OUT_DIR1.mkdir(parents=True, exist_ok=True)
OUT_DIR2.mkdir(parents=True, exist_ok=True)
ID_COLS = {"article_id": str, "product_code": str}

# 1. Sample customers
customers = pd.read_csv(DATA_DIR / "customers.csv", dtype=str)
sampled_ids = set(rng.choice(customers["customer_id"].to_numpy(), N_CUSTOMERS, replace=False))

# 2. One pass over transactions: keep sampled customers in the window,
#    and remember every article that was EVER sold (to find never-sold articles).
kept, ever_sold = [], set()
reader = pd.read_csv(DATA_DIR / "transactions_train.csv", dtype=str, chunksize=CHUNK)
for i, chunk in enumerate(reader, 1):
    ever_sold.update(chunk["article_id"].unique())
    mask = (chunk["t_dat"] >= START_DATE) & chunk["customer_id"].isin(sampled_ids)
    kept.append(chunk[mask])
    print(f"chunk {i}: kept {mask.sum():,} rows")
tx = pd.concat(kept, ignore_index=True)

# 3. Respect the row cap by dropping whole customers (keeps baskets intact)
if len(tx) > MAX_ROWS:
    per_cust = tx.groupby("customer_id").size().sample(frac=1, random_state=SEED)
    keep = per_cust[per_cust.cumsum() <= MAX_ROWS].index
    tx = tx[tx["customer_id"].isin(keep)]
tx = tx.sort_values(["t_dat", "customer_id"]).reset_index(drop=True)

# 4. Articles: everything bought + long-tail articles with no sales in the sample
articles = pd.read_csv(DATA_DIR / "articles.csv", dtype=ID_COLS)
bought = set(tx["article_id"])
never_sold = articles[~articles["article_id"].isin(ever_sold)]
unsold_window = articles[articles["article_id"].isin(ever_sold) & ~articles["article_id"].isin(bought)]
extra = pd.concat([
    never_sold.sample(min(N_NEVER_SOLD, len(never_sold)), random_state=SEED),
    unsold_window.sample(min(N_UNSOLD_IN_WINDOW, len(unsold_window)), random_state=SEED),
])
art_sample = pd.concat([articles[articles["article_id"].isin(bought)], extra]).drop_duplicates("article_id")
cust_sample = customers[customers["customer_id"].isin(tx["customer_id"])]

# 5. Flat table: one row per purchased unit, with customer + article details repeated
FLAT_COLS = [
    "t_dat", "customer_id", "age", "club_member_status", "fashion_news_frequency", "FN", "Active", "postal_code",
    "sales_channel_id", "article_id", "product_code", "prod_name", "product_type_no", "product_type_name",
    "product_group_name", "graphical_appearance_no", "graphical_appearance_name", "colour_group_code",
    "colour_group_name", "department_no", "department_name", "section_no", "section_name",
    "index_code", "index_name", "index_group_no", "index_group_name", "price",
]
flat = tx.merge(cust_sample, on="customer_id", how="left").merge(art_sample, on="article_id", how="left")[FLAT_COLS]

# 6. Save
tx.to_csv(OUT_DIR1 / "transactions_sample.csv", index=False)
cust_sample.to_csv(OUT_DIR1 / "customers_sample.csv", index=False)
art_sample.to_csv(OUT_DIR1 / "articles_sample.csv", index=False)
flat.to_csv(OUT_DIR2 / "hm_flat.csv", index=False)

# 7. Profile (paste into your report)
print("\n=== PROFILE ===")
print(f"Flat table: {len(flat):,} rows x {flat.shape[1]} columns")
print(f"Date range: {tx['t_dat'].min()} to {tx['t_dat'].max()} ({tx['t_dat'].nunique()} days)")
print(f"Customers: {len(cust_sample):,} | Articles bought: {len(bought):,} | "
      f"Never sold (full data): {len(never_sold):,} total, {min(N_NEVER_SOLD, len(never_sold))} kept | "
      f"Unsold in window kept: {min(N_UNSOLD_IN_WINDOW, len(unsold_window))}")
print(f"Exact duplicate rows (multi-unit purchases): {flat.duplicated().sum():,}")
print("\nNulls per column:\n", flat.isna().sum()[flat.isna().sum() > 0])
print("\nData types:\n", flat.dtypes)
print("\n5-row preview:\n", flat.head().to_string())
