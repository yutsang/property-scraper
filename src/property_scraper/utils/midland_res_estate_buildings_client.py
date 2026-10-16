"""Resumable client for Midland residential building enumeration per estate.

Shared by the standalone CLI (scripts/midland_res_estate_buildings_pull.py) and
the Kedro node (pipelines/building_master/buildings.py) so both use one tested
implementation.

Known limitation (by design, not a bug)
----------------------------------------
`data.midland.com.hk/search/v2/filters?filter_type=building&est_ids=<id>&tx_type=S`
(and the same with `tx_type=L`) needs no auth beyond the site's normal headers
(verified live) and returns a flat list of building dicts with a stable id
(`B000000831`-style), name, and estate/phase/district hierarchy embedded in
each row. **This is NOT a guaranteed-complete building list** — verified live
on City One Shatin: the site shows 52 blocks, but tx_type=S union tx_type=L
only returns 27 (52%). Treat this as "more blocks than we can reverse-engineer
from transaction history alone", not "all blocks".
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import httpx
import yaml

API = "https://data.midland.com.hk/search/v2/filters"
DEFAULT_OUT = "data/midland_res_estate_buildings_full"

logger = logging.getLogger(__name__)


class RateLimited(Exception):
    pass


def _headers() -> dict:
    return yaml.safe_load(open("conf/local/parameters.yml"))[
        "webscraper"]["source_b_res"]["site"]["transaction_headers"]


class MidlandEstateBuildingsPuller:
    def __init__(self, out_dir: str = DEFAULT_OUT, delay: float = 0.3):
        self.out = Path(out_dir)
        (self.out / "rows").mkdir(parents=True, exist_ok=True)
        self.delay = delay
        self.client = httpx.Client(timeout=30, headers=_headers())
        self.checkpoint_path = self.out / "checkpoint.json"
        self.done: dict[str, dict] = {}
        if self.checkpoint_path.exists():
            self.done = json.loads(self.checkpoint_path.read_text())
            logger.info("resuming: %d estates already complete", len(self.done))

    def _save_checkpoint(self) -> None:
        tmp = self.checkpoint_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.done, ensure_ascii=False, indent=2))
        os.replace(tmp, self.checkpoint_path)

    def _get(self, estate_id: str, tx_type: str) -> list:
        backoff = 20.0
        for _ in range(6):
            try:
                r = self.client.get(API, params={
                    "filter_type": "building", "est_ids": estate_id, "tx_type": tx_type,
                })
            except httpx.HTTPError as exc:
                logger.warning("network error (%s); backing off %.0fs", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue
            if r.status_code == 200:
                time.sleep(self.delay)
                try:
                    payload = r.json()
                except Exception:
                    return []
                for section in payload:
                    if section.get("type") == "building":
                        return section.get("filter") or []
                return []
            if r.status_code == 400:
                logger.warning("HTTP 400 for est_id=%s (malformed id?) — skipping", estate_id)
                return []
            if r.status_code == 429 or r.status_code >= 500:
                logger.warning("HTTP %d for est_id=%s; backing off %.0fs", r.status_code, estate_id, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue
            raise RuntimeError(f"HTTP {r.status_code} for est_id={estate_id}: {r.text[:150]}")
        raise RateLimited("too many consecutive failures — likely blocked")

    def collect_estate(self, estate_id: str) -> list[dict]:
        by_building_id: dict[str, dict] = {}
        for tx_type in ("S", "L"):
            for row in self._get(estate_id, tx_type):
                bid = row.get("id")
                if bid and bid not in by_building_id:
                    by_building_id[bid] = row
        return list(by_building_id.values())

    def _write(self, estate_id: str, rows: list[dict]) -> None:
        safe_name = estate_id.replace("/", "_")
        shard = self.out / "rows" / f"{safe_name}.jsonl"
        tmp = shard.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(tmp, shard)

    def run(self, estate_ids: list[str]) -> None:
        pending = [e for e in estate_ids if e not in self.done]
        logger.info("%d estates to pull (%d already done)", len(pending), len(self.done))
        if not pending:
            logger.info("nothing to do.")
            return
        for i, estate_id in enumerate(pending, 1):
            try:
                rows = self.collect_estate(estate_id)
            except RateLimited as exc:
                logger.error("STOPPING (resumable): %s", exc)
                self._save_checkpoint()
                raise
            self._write(estate_id, rows)
            self.done[estate_id] = {"buildings": len(rows)}
            if i % 200 == 0 or i == len(pending):
                self._save_checkpoint()
            if i % 500 == 0:
                total = sum(v["buildings"] for v in self.done.values())
                logger.info("[%d/%d] %d estates done, %d buildings collected so far", i, len(pending), len(self.done), total)
        self._save_checkpoint()
        total = sum(v["buildings"] for v in self.done.values())
        logger.info("DONE. %d estates, %d buildings collected.", len(self.done), total)

    def load_all_rows(self) -> list[dict]:
        """Read every collected building row back from the rows/ shards."""
        rows: list[dict] = []
        for shard in sorted((self.out / "rows").glob("*.jsonl")):
            with shard.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
        return rows
