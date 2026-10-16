#!/usr/bin/env python3
"""Standalone, resumable full-history pull of Midland ICI (commercial) transactions.

Does NOT touch the Kedro pipeline or existing datasets: reads only conf/local for
the endpoint/token, writes only to a fresh output directory.

Why this shape:
- The ICI API (`data.midlandici.com.hk/search/v1/transaction`) defaults to a
  ~5-year rolling window (floors at 2021). Passing `dateFrom`/`dateTo` (yyyy-mm-dd)
  unlocks full history back to 2000 — ~400k rows vs ~82k currently held.
- Page-based pagination breaks at deep offsets, so history is sliced by YEAR;
  each year is small (<=27 pages at limit=1000) and pages cleanly. Date windows
  are disjoint, so no cross-window dedup is needed.
- The response is a list of sections; the SL section is sales+lease combined.

Usage:
  python scripts/midland_ici_api_pull.py            # full pull 2000..now, resumable
  python scripts/midland_ici_api_pull.py --years 2015 2016
Re-run to resume; completed years are skipped.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import date
from pathlib import Path

import httpx
import yaml

OUT = "data/midland_ici_api_full"
PAGE_LIMIT = 1000
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("midland_ici_pull")


class RateLimited(Exception):
    pass


def _cfg():
    c = yaml.safe_load(open("conf/local/parameters.yml"))["webscraper"]["source_b_commercial"]
    return c


class Puller:
    def __init__(self, out_dir: str, delay: float, lang: str = "zh-hk"):
        self.out = Path(out_dir)
        (self.out / "rows").mkdir(parents=True, exist_ok=True)
        self.delay = delay
        self.lang = lang
        cfg = _cfg()
        self.url = cfg["transaction_url"]
        self.headers = cfg["transaction_headers_api"]
        self.client = httpx.Client(timeout=40, follow_redirects=True)
        self.client.get(cfg["transaction_landing_url"], headers=self.headers)  # session cookies
        self.ckpt = self.out / "checkpoint.json"
        self.done = json.loads(self.ckpt.read_text()) if self.ckpt.exists() else {}
        if self.done:
            logger.info("resuming: %d years already complete", len(self.done))

    def _save_ckpt(self):
        tmp = self.ckpt.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.done, ensure_ascii=False, indent=2))
        os.replace(tmp, self.ckpt)

    def _get(self, params: dict) -> list:
        backoff = 30.0
        for _ in range(8):
            try:
                r = self.client.get(self.url, headers=self.headers, params=params)
            except httpx.HTTPError as exc:
                logger.warning("network error (%s); backing off %.0fs", exc, backoff)
                time.sleep(backoff); backoff = min(backoff * 2, 600); continue
            if r.status_code == 200:
                time.sleep(self.delay)
                try:
                    return r.json()
                except Exception:
                    logger.warning("non-JSON 200 (offset too deep?); backing off %.0fs", backoff)
                    time.sleep(backoff); backoff = min(backoff * 2, 600); continue
            if r.status_code == 429 or r.status_code >= 500:
                logger.warning("HTTP %d; backing off %.0fs", r.status_code, backoff)
                time.sleep(backoff); backoff = min(backoff * 2, 600); continue
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:150]}")
        raise RateLimited("too many consecutive failures — likely blocked")

    @staticmethod
    def _sl(sections: list) -> dict | None:
        return next((s for s in sections if s.get("type") == "SL"), None)

    def _base(self, year: int, page: int) -> dict:
        return {"currency": "HKD", "lang": self.lang, "sort": "txDate-desc", "unit": "feet",
                "txType": "SL", "limit": PAGE_LIMIT, "page": page,
                "dateFrom": f"{year}-01-01", "dateTo": f"{year}-12-31"}

    def pull_year(self, year: int) -> list:
        first = self._sl(self._get(self._base(year, 1)))
        if not first:
            return []
        total = int(first.get("count", 0))
        rows = list(first.get("results", []))
        pages = (total + PAGE_LIMIT - 1) // PAGE_LIMIT
        for page in range(2, pages + 1):
            sec = self._sl(self._get(self._base(year, page)))
            batch = sec.get("results", []) if sec else []
            if not batch:
                break
            rows.extend(batch)
        return rows

    def _write(self, year: int, rows: list):
        shard = self.out / "rows" / f"{year}.jsonl"
        tmp = shard.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        os.replace(tmp, shard)

    def run(self, years: list[int]):
        pending = [y for y in years if str(y) not in self.done]
        logger.info("%d years to pull (%d done)", len(pending), len(self.done))
        for i, year in enumerate(pending, 1):
            try:
                rows = self.pull_year(year)
            except RateLimited as exc:
                logger.error("STOPPING (resumable): %s — re-run to resume", exc)
                self._save_ckpt(); raise SystemExit(2)
            self._write(year, rows)
            self.done[str(year)] = {"rows": len(rows)}
            self._save_ckpt()
            logger.info("[%d/%d] %d: %d rows", i, len(pending), year, len(rows))
        total = sum(v["rows"] for v in self.done.values())
        logger.info("DONE. %d years, %d transactions.", len(self.done), total)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None)
    ap.add_argument("--delay", type=float, default=0.4)
    ap.add_argument("--years", nargs="*", type=int)
    ap.add_argument("--lang", default="zh-hk")
    args = ap.parse_args()
    years = args.years or list(range(2000, date.today().year + 1))
    out = args.out or (OUT if args.lang == "zh-hk" else f"{OUT}_{args.lang.replace('-', '_')}")
    Puller(out, args.delay, args.lang).run(years)


if __name__ == "__main__":
    main()
