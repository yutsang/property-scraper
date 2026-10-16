#!/usr/bin/env python3
"""Standalone, resumable full-history pull of Midland residential transactions.

Does NOT touch the Kedro pipeline or existing datasets: reads conf/local for the
endpoint/token and the estate parquet (read-only) for the sub-district list;
writes only to a fresh output directory.

Why this shape:
- The pipeline hard-codes `tx_date="1year"`, capping updates to 1 year. The site's
  own frontend sends NO `tx_date`, which returns the full available history back
  to ~2014. So this pull simply omits `tx_date`.
- No `tx_type` returns sales+lease combined (verified: no-type count == S+L).
- Per sub-district counts are a few thousand, well under any offset ceiling, so
  plain page-based pagination (limit=1000) reaches the end.

Usage:
  python scripts/midland_res_api_pull.py                 # all 130 sub-districts
  python scripts/midland_res_api_pull.py --districts 130ND10001
Re-run to resume; completed sub-districts are skipped.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path

import httpx
import pandas as pd
import yaml

OUT = "data/midland_res_api_full"
ESTATE_PARQUET = "data/01_raw/midland_res_estates.parquet"
PAGE_LIMIT = 1000
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("midland_res_pull")


class RateLimited(Exception):
    pass


class Puller:
    def __init__(self, out_dir: str, delay: float):
        self.out = Path(out_dir)
        (self.out / "rows").mkdir(parents=True, exist_ok=True)
        self.delay = delay
        site = yaml.safe_load(open("conf/local/parameters.yml"))["webscraper"]["source_b_res"]["site"]
        self.url = site["transaction_search_url"]
        self.headers = site["transaction_headers"]
        self.client = httpx.Client(timeout=40)
        self.ckpt = self.out / "checkpoint.json"
        self.done = json.loads(self.ckpt.read_text()) if self.ckpt.exists() else {}
        if self.done:
            logger.info("resuming: %d sub-districts already complete", len(self.done))

    def _save_ckpt(self):
        tmp = self.ckpt.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.done, ensure_ascii=False, indent=2))
        os.replace(tmp, self.ckpt)

    def _get(self, params: dict) -> dict:
        backoff = 30.0
        for _ in range(8):
            try:
                r = self.client.get(self.url, headers=self.headers, params=params)
            except httpx.HTTPError as exc:
                logger.warning("network error (%s); backing off %.0fs", exc, backoff)
                time.sleep(backoff); backoff = min(backoff * 2, 600); continue
            if r.status_code == 200:
                time.sleep(self.delay)
                return r.json()
            if r.status_code == 429 or r.status_code >= 500:
                logger.warning("HTTP %d; backing off %.0fs", r.status_code, backoff)
                time.sleep(backoff); backoff = min(backoff * 2, 600); continue
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:150]}")
        raise RateLimited("too many consecutive failures — likely blocked")

    def _base(self, dist: str, page: int) -> dict:
        # No tx_date (full history to ~2014); no tx_type (sales + lease together).
        return {"ad": "true", "chart": "true", "lang": "en", "currency": "HKD",
                "unit": "feet", "search_behavior": "normal", "limit": PAGE_LIMIT,
                "page": page, "intsmdist_ids": dist}

    def pull_district(self, dist: str) -> list:
        first = self._get(self._base(dist, 1))
        total = int(first.get("count", 0))
        rows = list(first.get("result", []))
        pages = (total + PAGE_LIMIT - 1) // PAGE_LIMIT
        for page in range(2, pages + 1):
            batch = self._get(self._base(dist, page)).get("result", [])
            if not batch:
                break
            rows.extend(batch)
        return rows, total

    def _write(self, dist: str, rows: list):
        shard = self.out / "rows" / f"{dist}.jsonl"
        tmp = shard.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        os.replace(tmp, shard)

    def run(self, districts: list[str]):
        pending = [d for d in districts if d not in self.done]
        logger.info("%d sub-districts to pull (%d done)", len(pending), len(self.done))
        for i, dist in enumerate(pending, 1):
            try:
                rows, total = self.pull_district(dist)
            except RateLimited as exc:
                logger.error("STOPPING (resumable): %s — re-run to resume", exc)
                self._save_ckpt(); raise SystemExit(2)
            self._write(dist, rows)
            cov = len(rows) / total if total else 1.0
            self.done[dist] = {"rows": len(rows), "api_count": total, "coverage": round(cov, 4)}
            self._save_ckpt()
            flag = "" if cov >= 0.995 else f"  <-- shortfall {cov:.1%}"
            logger.info("[%d/%d] %s: %d/%d rows%s", i, len(pending), dist, len(rows), total, flag)
        total_rows = sum(v["rows"] for v in self.done.values())
        logger.info("DONE. %d sub-districts, %d transactions.", len(self.done), total_rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--delay", type=float, default=0.4)
    ap.add_argument("--districts", nargs="*")
    args = ap.parse_args()
    districts = args.districts or sorted(
        pd.read_parquet(ESTATE_PARQUET)["int_sm_district_id"].dropna().astype(str).unique()
    )
    Puller(args.out, args.delay).run(districts)


if __name__ == "__main__":
    main()
