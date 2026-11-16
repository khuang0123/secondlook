"""
Assignment 3, Part 1, Step 2: Bronze layer in Amazon S3.

Uploads files exactly as produced/received (zero transformation) and records audit metadata:
  - raw Open-Meteo responses from Step 1 (each already wrapped with ingested_at + source_name)
  - the three H&M sample files, which keep the original Kaggle columns (rows filtered only, never joined):
      transactions_sample.csv   (purchases by sampled customers, 2020-03-01 to 2020-09-22)
      customers_sample.csv      (those customers)
      articles_sample.csv       (article catalogue, incl. the 1,000 unsold long-tail articles)

Lineage: Kaggle originals -> 00_build_sample.py (sample customers + date window + unsold articles;
rows filtered, columns unchanged) -> three sample CSVs -> Bronze (S3).

Bronze layout (immutable: every run lands under its own ingest_date, nothing is overwritten):
  bronze/open_meteo/archive/city=<city>/window=<YYYY-MM>/ingest_date=<YYYY-MM-DD>/response.json
  bronze/hm_kaggle/transactions/ingest_date=<YYYY-MM-DD>/transactions_sample.csv
  bronze/hm_kaggle/customers/ingest_date=<YYYY-MM-DD>/customers_sample.csv
  bronze/hm_kaggle/articles/ingest_date=<YYYY-MM-DD>/articles_sample.csv
  bronze/_manifests/batch_id=<id>.json          one manifest per run: every object, hash, size, rows

Audit metadata on every object (S3 user metadata): source-name, ingested-at, sha256, batch-id.
CSV files are not edited to add columns; their audit metadata lives in object metadata + the manifest.

Re-running is safe: an object whose key already exists with the same SHA-256 is skipped.

Setup:
  pip install boto3
  aws configure            (or: aws sso login --profile <name>; export AWS_PROFILE=<name>)
Expected project layout (paths are resolved from this file's location, so it runs from any folder):
  Assignment/
    data/bronze/weather/raw/city=<city>/window=<YYYY-MM>.json   (from 01_ingest_weather.py)
    data/bronze/hm/transactions_sample.csv, customers_sample.csv, articles_sample.csv
    script/02_land_bronze_s3.py                                  (this file)
Run:
  python script/02_land_bronze_s3.py --bucket <your-bucket> --create-bucket   (first time only)
  python script/02_land_bronze_s3.py --bucket <your-bucket>
"""
import argparse
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

PROJECT_ROOT = Path(__file__).resolve().parents[1]          # Assignment/ (the parent of script/)
DEFAULT_WEATHER_DIR = PROJECT_ROOT / "data" / "bronze" / "weather" / "raw"
DEFAULT_HM_DIR = PROJECT_ROOT / "data" / "bronze" / "hm"
BATCH_ID = uuid.uuid4().hex[:12]
NOW = datetime.now(timezone.utc).replace(microsecond=0)
INGEST_DATE = NOW.strftime("%Y-%m-%d")


def sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


# ------------------------------------------------------------------ bucket setup (optional)
def create_bucket(s3, bucket, region):
    """Private, encrypted, versioned bucket. Safe to call if it already exists."""
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
        print(f"created bucket {bucket}")
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
        print(f"bucket {bucket} already exists")
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True,
        "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    print("  public access blocked, default encryption on, versioning on")


# ------------------------------------------------------------------ upload one object
def land(s3, bucket, key, body, source_name, ingested_at, content_type, rows=None):
    digest = sha256_bytes(body)
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        if head.get("Metadata", {}).get("sha256") == digest:
            return {"key": key, "sha256": digest, "bytes": len(body), "rows": rows, "status": "skipped (identical)"}
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, Metadata={
        "source-name": source_name, "ingested-at": ingested_at, "sha256": digest, "batch-id": BATCH_ID})
    return {"key": key, "sha256": digest, "bytes": len(body), "rows": rows, "status": "uploaded"}


def land_weather(s3, bucket, weather_dir):
    results = []
    files = sorted(Path(weather_dir).rglob("*.json"))
    if not files:
        print(f"  no JSON files under {weather_dir}; run 01_ingest_weather.py first")
    for path in files:
        body = path.read_bytes()                                  # uploaded byte-for-byte
        doc = json.loads(body)
        city = next((p.split("=", 1)[1] for p in path.parts if p.startswith("city=")), "unknown")
        window = path.stem.split("=", 1)[1] if path.stem.startswith("window=") else path.stem
        ingested_at = doc.get("ingested_at", NOW.isoformat())
        key = (f"bronze/open_meteo/archive/city={city}/window={window}/"
               f"ingest_date={ingested_at[:10]}/response.json")
        days = len(((doc.get("response") or {}).get("daily") or {}).get("time") or [])
        results.append(land(s3, bucket, key, body, doc.get("source_name", "open-meteo-historical-archive"),
                            ingested_at, "application/json", rows=days))
    return results


def land_csv(s3, bucket, path, dataset, source_name):
    path = Path(path)
    if not path.exists():
        print(f"  {path} not found, skipped"); return []
    body = path.read_bytes()
    lines = body.splitlines()
    rows = max(len(lines) - 1, 0)                                 # data rows, excluding the header
    key = f"bronze/hm_kaggle/{dataset}/ingest_date={INGEST_DATE}/{path.name}"
    return [land(s3, bucket, key, body, source_name, NOW.isoformat(), "text/csv", rows=rows)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--create-bucket", action="store_true")
    ap.add_argument("--weather-dir", default=str(DEFAULT_WEATHER_DIR))
    ap.add_argument("--hm-dir", default=str(DEFAULT_HM_DIR),
                    help="folder containing transactions_sample.csv, customers_sample.csv, articles_sample.csv")
    args = ap.parse_args()

    s3 = boto3.client("s3", region_name=args.region)
    if args.create_bucket:
        create_bucket(s3, args.bucket, args.region)

    print(f"batch {BATCH_ID} -> s3://{args.bucket}/bronze/")
    sample = Path(args.hm_dir)
    print(f"reading weather from {args.weather_dir}\nreading H&M samples from {sample}")
    results = land_weather(s3, args.bucket, args.weather_dir)
    for dataset in ("transactions", "customers", "articles"):
        results += land_csv(s3, args.bucket, sample / f"{dataset}_sample.csv", dataset, f"hm-kaggle-sample:{dataset}")

    manifest = {"batch_id": BATCH_ID, "ingested_at": NOW.isoformat(), "bucket": args.bucket, "objects": results}
    s3.put_object(Bucket=args.bucket, Key=f"bronze/_manifests/batch_id={BATCH_ID}.json",
                  Body=json.dumps(manifest, indent=2).encode(), ContentType="application/json")

    up = [r for r in results if r["status"] == "uploaded"]
    weather = [r for r in results if r["key"].startswith("bronze/open_meteo/")]
    print("\n=== BRONZE LANDING SUMMARY ===")
    print(f"  weather responses : {len(weather)} objects, {sum(r['rows'] or 0 for r in weather):,} days")
    for r in results:
        if r["key"].startswith("bronze/hm_kaggle/"):
            print(f"  {r['key'].split('/')[2]:<18}: {r['rows']:,} rows  ({r['status']})")
    print(f"  uploaded {len(up)}, skipped {len(results) - len(up)} identical, "
          f"total {sum(r['bytes'] for r in results) / 1024:,.0f} KB")
    print(f"  manifest          : s3://{args.bucket}/bronze/_manifests/batch_id={BATCH_ID}.json")


if __name__ == "__main__":
    main()