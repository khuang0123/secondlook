"""
Assignment 3, Part 1, Step 1: live API ingestion (Open-Meteo Historical Weather API).

Pulls daily weather for 5 Swedish cities over the H&M sample window and:
  - "paginates" by iterating monthly date windows x cities until every window is retrieved
    (Open-Meteo has no page/offset/cursor parameter; one request returns a whole date range)
  - rate-limits (minimum gap between calls) and retries with exponential backoff + jitter
    on HTTP 429 / 5xx / timeouts / connection errors, honouring Retry-After when present
  - handles bad status codes, error JSON, missing keys, empty and ragged arrays without crashing
  - runs 4 automated quality checks on every record and logs row-level pass/fail counts

Outputs (outputs/bronze/weather/):
  raw/city=<city>/window=<YYYY-MM>.json   raw API response + audit metadata (input to Step 2 Bronze)
  weather_records.csv                     flattened records with qc_pass / qc_fail_reasons columns
  request_log.csv                         one row per request: status, attempts, records, error
  qc_report.json                          pass/fail counts per check

API: https://archive-api.open-meteo.com/v1/archive  (no key; free for non-commercial use)
Run:  python 01_ingest_weather.py            (defaults to 2020-03-01 .. 2020-09-22)
      python 01_ingest_weather.py --start 2020-03-01 --end 2020-09-22
"""
import argparse
import json
import random
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://archive-api.open-meteo.com/v1/archive"
SOURCE_NAME = "open-meteo-historical-archive"
CITIES = {                      # H&M is Sweden-based; customer postal codes are hashed, so we use national coverage
    "Stockholm":  (59.3293, 18.0686),
    "Gothenburg": (57.7089, 11.9746),
    "Malmo":      (55.6050, 13.0038),
    "Uppsala":    (59.8586, 17.6389),
    "Vasteras":   (59.6099, 16.5448),
}
DAILY_VARS = ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
              "precipitation_sum", "rain_sum", "snowfall_sum", "weather_code"]
REQUIRED = ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "precipitation_sum"]
WMO_CODES = {0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 71, 73, 75, 77,
             80, 81, 82, 85, 86, 95, 96, 99}

MIN_INTERVAL_S = 0.5            # rate limit: at most ~2 requests per second
MAX_RETRIES = 5
TIMEOUT_S = 20
OUT = Path("data/bronze/weather")


# ------------------------------------------------------------------ windows ("pages")
def monthly_windows(start: date, end: date):
    """Yield (window_start, window_end) month by month, clipped to [start, end]."""
    cur = start
    while cur <= end:
        nxt = (cur.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield cur, min(nxt - timedelta(days=1), end)
        cur = nxt


# ------------------------------------------------------------------ HTTP with retries
_last_call = 0.0


def throttle():
    global _last_call
    wait = MIN_INTERVAL_S - (time.monotonic() - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()


def fetch(session, params):
    """Return (payload or None, status_code, attempts, error_message)."""
    status, err = None, None
    for attempt in range(1, MAX_RETRIES + 1):
        throttle()
        try:
            resp = session.get(API_URL, params=params, timeout=TIMEOUT_S)
            status = resp.status_code
            if status == 200:
                try:
                    payload = resp.json()
                except ValueError:
                    return None, status, attempt, "invalid JSON body"
                if isinstance(payload, dict) and payload.get("error"):
                    return None, status, attempt, f"API error: {payload.get('reason')}"
                return payload, status, attempt, None
            if status == 400:                                   # bad request: retrying won't help
                try:
                    reason = resp.json().get("reason")
                except ValueError:
                    reason = resp.text[:200]
                return None, status, attempt, f"HTTP 400: {reason}"
            if status == 429 or status >= 500:                  # transient: back off and retry
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else 2 ** attempt
                err = f"HTTP {status}"
            else:
                return None, status, attempt, f"HTTP {status}"
        except requests.exceptions.Timeout:
            err, delay = "timeout", 2 ** attempt
        except requests.exceptions.ConnectionError as e:
            err, delay = f"connection error: {type(e).__name__}", 2 ** attempt
        if attempt < MAX_RETRIES:
            delay = min(delay, 60) + random.uniform(0, 0.5)     # jitter avoids synchronized retries
            print(f"    retry {attempt}/{MAX_RETRIES - 1} after {err}; sleeping {delay:.1f}s")
            time.sleep(delay)
    return None, status, MAX_RETRIES, f"gave up after {MAX_RETRIES} attempts ({err})"


# ------------------------------------------------------------------ parse safely
def flatten(payload, city, lat, lon, w_start, ingested_at):
    """Turn one response into records; tolerate missing keys and ragged arrays."""
    daily = (payload or {}).get("daily")
    if not isinstance(daily, dict) or not daily.get("time"):
        return [], "missing or empty 'daily.time'"
    n = len(daily["time"])
    problems = [v for v in DAILY_VARS if v in daily and len(daily[v] or []) != n]
    missing = [v for v in DAILY_VARS if v not in daily]
    rows = []
    for i, day in enumerate(daily["time"]):
        row = {"city": city, "latitude": lat, "longitude": lon, "date": day,
               "window_start": w_start.isoformat(), "ingested_at": ingested_at, "source_name": SOURCE_NAME}
        for v in DAILY_VARS:
            arr = daily.get(v) or []
            row[v] = arr[i] if i < len(arr) else None
        rows.append(row)
    note = "; ".join(filter(None, [
        f"missing keys {missing}" if missing else "",
        f"length mismatch {problems}" if problems else ""])) or None
    return rows, note


# ------------------------------------------------------------------ quality checks
def run_quality_checks(df, start, end):
    """Flag every row; return the frame with qc columns plus a pass/fail report."""
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index)

    def flag(mask, label):
        for i in df.index[mask]:
            reasons[i].append(label)
        return {"check": label, "passed": int((~mask).sum()), "failed": int(mask.sum())}

    report = []
    # 1. Null check on required measures
    report.append(flag(df[REQUIRED].isna().any(axis=1), "null_required_field"))
    # 2. Duplicate detection on the natural key (city, date)
    report.append(flag(df.duplicated(subset=["city", "date"], keep="first"), "duplicate_city_date"))
    # 3. Range validation on physical values
    t = df[["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"]]
    bad_range = (((t < -40) | (t > 45)).any(axis=1)
                 | (df["precipitation_sum"] < 0)
                 | (df["temperature_2m_min"] > df["temperature_2m_max"])
                 | (df["weather_code"].notna() & ~df["weather_code"].isin(WMO_CODES)))
    report.append(flag(bad_range.fillna(False), "out_of_range"))
    # 4. Format validation: date parses and falls inside the requested window
    d = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    report.append(flag(d.isna() | (d < pd.Timestamp(start)) | (d > pd.Timestamp(end)), "bad_date_format_or_range"))

    df = df.copy()
    df["qc_fail_reasons"] = reasons.map(lambda r: ";".join(r) if r else "")
    df["qc_pass"] = df["qc_fail_reasons"] == ""
    return df, report


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2020-03-01")
    ap.add_argument("--end", default="2020-09-22")
    args = ap.parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)

    (OUT / "raw").mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "secondlook-coursework/1.0"
    records, log = [], []
    windows = list(monthly_windows(start, end))
    print(f"{len(CITIES)} cities x {len(windows)} monthly windows = {len(CITIES) * len(windows)} requests")

    for city, (lat, lon) in CITIES.items():
        for w_start, w_end in windows:
            params = {"latitude": lat, "longitude": lon, "start_date": w_start.isoformat(),
                      "end_date": w_end.isoformat(), "daily": ",".join(DAILY_VARS),
                      "timezone": "Europe/Stockholm"}
            ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            payload, status, attempts, error = fetch(session, params)
            rows, note = flatten(payload, city, lat, lon, w_start, ingested_at) if payload else ([], None)
            if payload is not None:
                raw_path = OUT / "raw" / f"city={city}" / f"window={w_start:%Y-%m}.json"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(json.dumps({
                    "ingested_at": ingested_at, "source_name": SOURCE_NAME, "request_url": API_URL,
                    "params": params, "status_code": status, "response": payload}))
            records.extend(rows)
            log.append({"city": city, "window_start": w_start.isoformat(), "window_end": w_end.isoformat(),
                        "status": status, "attempts": attempts, "records": len(rows),
                        "error": error or note or ""})
            print(f"  {city:<10} {w_start:%Y-%m}  status={status} attempts={attempts} "
                  f"records={len(rows)}{'  ERROR: ' + (error or note) if (error or note) else ''}")

    pd.DataFrame(log).to_csv(OUT / "request_log.csv", index=False)
    if not records:
        print("No records retrieved; see request_log.csv"); return

    df, report = run_quality_checks(pd.DataFrame(records), start, end)
    df.to_csv(OUT / "weather_records.csv", index=False)
    summary = {
        "run_finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "requests_total": len(log),
        "requests_ok": sum(1 for r in log if r["records"] > 0),
        "requests_failed": sum(1 for r in log if r["records"] == 0),
        "retries": sum(r["attempts"] - 1 for r in log),
        "records_total": len(df),
        "records_passed_all_checks": int(df["qc_pass"].sum()),
        "records_failed_any_check": int((~df["qc_pass"]).sum()),
        "checks": report,
    }
    (OUT / "qc_report.json").write_text(json.dumps(summary, indent=2))

    print("\n=== QUALITY CHECKS (row level) ===")
    for r in report:
        print(f"  {r['check']:<26} passed={r['passed']:>5}  failed={r['failed']:>4}")
    print(f"\nrecords: {summary['records_total']:,}  passed all checks: {summary['records_passed_all_checks']:,}  "
          f"failed: {summary['records_failed_any_check']:,}")
    print(f"requests: {summary['requests_ok']}/{summary['requests_total']} ok, {summary['retries']} retries")
    if summary["records_total"] < 500:
        print("WARNING: fewer than 500 records; add cities or widen the date range.")


if __name__ == "__main__":
    main()
