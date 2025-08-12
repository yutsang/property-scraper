import os
import sys
import json
from datetime import datetime, timedelta, date
from typing import Optional, Tuple

import pandas as pd
import requests


def compute_week_date_range(today: Optional[date] = None) -> Tuple[date, date]:
    if today is None:
        today = datetime.now().date()
    wd = today.weekday()  # 0=Mon .. 6=Sun
    if 0 <= wd <= 3:
        last_monday = today - timedelta(days=wd + 7)
        last_sunday = last_monday + timedelta(days=6)
        return last_monday, last_sunday
    else:
        monday = today - timedelta(days=wd)
        sunday = monday + timedelta(days=6)
        return monday, sunday


def fetch_midland_ici_transactions(date_min: date, date_max: date, max_page_size: int = 20000) -> pd.DataFrame:
    base_url = "https://www.midlandici.com.hk/ics/property/transaction/json"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.midlandici.com.hk",
        "Referer": "https://www.midlandici.com.hk/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    }

    all_rows = []
    cursor = 1
    while True:
        resp = requests.get(
            base_url,
            headers=headers,
            params={
                "ics_type": "",
                "date_min": date_min.strftime("%Y-%m-%d"),
                "date_max": date_max.strftime("%Y-%m-%d"),
                "lang": "english",
                "page_size": max_page_size,
                "cursor": cursor,
                "order": "tx_date-desc",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("transactions") or []
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < max_page_size:
            break
        cursor += 1

    df = pd.DataFrame(all_rows)
    return df


def to_int_or_none(x):
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x)
        digits = "".join(ch for ch in s if ch.isdigit())
        return int(digits) if digits else None
    except Exception:
        return None


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Midland ICI transactions for weekly date range via requests")
    parser.add_argument("--min-area", type=int, default=4000, help="Minimum area (sqft) to keep")
    parser.add_argument("--out", default=None, help="Optional output CSV path. Defaults to midland_ici_filtered_YYYYMMDD_YYYYMMDD.csv in project root")
    args = parser.parse_args()

    start_date, end_date = compute_week_date_range()
    df = fetch_midland_ici_transactions(start_date, end_date)

    if df.empty:
        print(json.dumps({"summary": "no_data", "start_date": str(start_date), "end_date": str(end_date)}))
        return

    # Try common area fields that appear in ICI JSON (e.g., saleableArea, grossArea, area)
    area_col = None
    for candidate in ["saleableArea", "grossArea", "sa", "area", "areaSqft"]:
        if candidate in df.columns:
            area_col = candidate
            break

    if area_col is None:
        # Try to derive from nested fields if needed
        # Fallback: create a None column
        df["areaSqft"] = None
        area_col = "areaSqft"

    df["area_sqft"] = df[area_col].apply(to_int_or_none)
    filtered = df[df["area_sqft"].fillna(0) >= args.min_area].copy()
    filtered.sort_values(by=["area_sqft"], ascending=False, inplace=True)

    out_path = (
        args.out
        if args.out
        else os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            f"midland_ici_filtered_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
        )
    )
    filtered.to_csv(out_path, index=False)

    print(
        json.dumps(
            {
                "summary": "ok",
                "start_date": str(start_date),
                "end_date": str(end_date),
                "count": len(filtered),
                "output": out_path,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()


