#!/usr/bin/env python3
"""Standalone, resumable DEEP-history pull of Midland residential transactions,
per building.

The district search endpoint (`/search/v2/transactions`) is windowed to ~2013+.
The per-building endpoint (`/info/v1/transactions/buildings/{id}`) carries full
history back to ~1996. This pulls every building's full history, superseding the
shallower district pull.

Does NOT touch the Kedro pipeline or existing data: reads conf/local for the
token and the on-disk transaction parquet (read-only) for the building list;
writes only to a fresh output directory. Resumable per sub-district.

Usage:
  python scripts/midland_res_buildings_pull.py
  python scripts/midland_res_buildings_pull.py --districts 130ND10001
Re-run to resume; completed sub-districts are skipped.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections import defaultdict
from pathlib import Path

import httpx
import pandas as pd
import yaml

OUT = "data/midland_res_buildings_full"
TRANS_PARQUET = "data/01_raw/midland_res_trans_lv_0.parquet"
BASE = "https://data.midland.com.hk/info/v1/transactions/buildings/"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("midland_res_bldg_pull")


class RateLimited(Exception):
    pass


class Puller:
    def __init__(self, out_dir: str, delay: float):
        self.out = Path(out_dir)
        (self.out / "rows").mkdir(parents=True, exist_ok=True)
        self.delay = delay
        self.headers = yaml.safe_load(open("conf/local/parameters.yml"))[
            "webscraper"]["source_b_res"]["site"]["transaction_headers"]
        self.client = httpx.Client(timeout=40)
        self.ckpt = self.out / "checkpoint.json"
        self.done = json.loads(self.ckpt.read_text()) if self.ckpt.exists() else {}
        if self.done:
            logger.info("resuming: %d sub-districts done", len(self.done))

    def _save_ckpt(self):
        tmp = self.ckpt.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.done, ensure_ascii=False, indent=2))
        os.replace(tmp, self.ckpt)

    def _get(self, bid: str) -> dict | None:
        backoff = 30.0
        for _ in range(8):
            try:
                r = self.client.get(BASE + bid, headers=self.headers,
                                    params={"lang": "en", "unit": "feet", "currency": "HKD"})
            except httpx.HTTPError as exc:
                logger.warning("network error (%s); backing off %.0fs", exc, backoff)
                time.sleep(backoff); backoff = min(backoff * 2, 600); continue
            if r.status_code == 200:
                time.sleep(self.delay)
                try:
                    return r.json()
                except Exception:
                    return None
            if r.status_code == 404:
                time.sleep(self.delay)
                return None  # building has no record
            if r.status_code == 429 or r.status_code >= 500:
                logger.warning("HTTP %d; backing off %.0fs", r.status_code, backoff)
                time.sleep(backoff); backoff = min(backoff * 2, 600); continue
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:150]}")
        raise RateLimited("too many consecutive failures — likely blocked")

    @staticmethod
    def _flatten(bid: str, payload: dict) -> list[dict]:
        """One building -> units -> transactions; emit one row per transaction,
        carrying the building id and the unit's stable fields."""
        rows = []
        for unit in payload.get("data", []):
            unit_fields = {k: unit.get(k) for k in
                           ("unit_id", "unit_type", "floor", "floor_seq", "flat",
                            "flat_name", "area", "net_area")}
            for tx in unit.get("transactions", []):
                rows.append({"building_id": bid, **unit_fields, **tx})
        return rows

    def pull_district(self, dist: str, bids: list[str]) -> tuple[list, int, str]:
        rows: list[dict] = []
        oldest = "9999"
        for bid in bids:
            payload = self._get(bid)
            if payload:
                br = self._flatten(bid, payload)
                rows.extend(br)
                for r in br:
                    d = str(r.get("tx_date", ""))[:10]
                    if d and d < oldest:
                        oldest = d
        return rows, len(bids), (oldest if oldest != "9999" else "-")

    def _write(self, dist: str, rows: list):
        shard = self.out / "rows" / f"{dist}.jsonl"
        tmp = shard.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        os.replace(tmp, shard)

    def run(self, by_dist: dict[str, list[str]]):
        districts = [d for d in sorted(by_dist) if d not in self.done]
        logger.info("%d sub-districts to pull (%d done)", len(districts), len(self.done))
        for i, dist in enumerate(districts, 1):
            try:
                rows, nb, oldest = self.pull_district(dist, by_dist[dist])
            except RateLimited as exc:
                logger.error("STOPPING (resumable): %s — re-run to resume", exc)
                self._save_ckpt(); raise SystemExit(2)
            self._write(dist, rows)
            self.done[dist] = {"buildings": nb, "rows": len(rows), "oldest": oldest}
            self._save_ckpt()
            logger.info("[%d/%d] %s: %d buildings, %d rows, back to %s",
                        i, len(districts), dist, nb, len(rows), oldest)
        total = sum(v["rows"] for v in self.done.values())
        logger.info("DONE. %d sub-districts, %d transactions.", len(self.done), total)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--delay", type=float, default=0.35)
    ap.add_argument("--districts", nargs="*")
    args = ap.parse_args()

    t = pd.read_parquet(TRANS_PARQUET, columns=["building_id", "int_sm_district_id"])
    t = t.dropna().astype(str).drop_duplicates()
    by_dist: dict[str, list[str]] = defaultdict(list)
    for bid, dist in zip(t["building_id"], t["int_sm_district_id"]):
        by_dist[dist].append(bid)
    if args.districts:
        by_dist = {d: by_dist[d] for d in args.districts if d in by_dist}
    Puller(args.out, args.delay).run(by_dist)


if __name__ == "__main__":
    main()
