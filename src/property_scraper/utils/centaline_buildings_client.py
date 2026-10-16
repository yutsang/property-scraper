"""Resumable client for Centaline residential block/building enumeration.

Shared by the standalone CLI (scripts/centaline_buildings_pull.py) and the
Kedro node (pipelines/building_master/buildings.py) so both use one tested
implementation.

Why this shape
--------------
- `data/01_raw/centaline_estate_lv_1.parquet` already carries a "2-" (phase)
  typeCode as its own row for every estate — verified live: all 237 "3-"
  (estate-level) rows have phaseCount > 0, and their buildingCount always
  equals the sum of their child "2-" rows. So the phase level never needs
  discovering; only the phase -> block level does.
- `estate/api/Transaction/ConsumptionTableEstateMenu?typeCode=<code>` needs no
  auth (verified live, plain GET, 200 with real data). It returns
  `{"hasSubLevel": bool, "menuItems": [{"name", "typeCode"}, ...]}` — when
  `hasSubLevel` is true the child codes need the same call again (not observed
  live from a real "2-" code, but handled generically rather than assumed
  away); when false, `menuItems` are the real block names (e.g. "1座").
- Progress is checkpointed per phase typeCode, so a crash loses at most the
  phase in flight, and a re-run with an already-complete checkpoint returns
  instantly (no API calls) unless new phase codes appear upstream.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import httpx

API = "https://hk.centanet.com/estate/api/Transaction/ConsumptionTableEstateMenu"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
DEFAULT_OUT = "data/centaline_buildings_full"

logger = logging.getLogger(__name__)


class RateLimited(Exception):
    """Raised when the source looks like it is blocking us."""


class CentalineBuildingsPuller:
    def __init__(self, out_dir: str = DEFAULT_OUT, delay: float = 0.3):
        self.out = Path(out_dir)
        (self.out / "rows").mkdir(parents=True, exist_ok=True)
        self.delay = delay
        self.client = httpx.Client(timeout=30, headers={"user-agent": UA})
        self.checkpoint_path = self.out / "checkpoint.json"
        self.done: dict[str, dict] = {}
        if self.checkpoint_path.exists():
            self.done = json.loads(self.checkpoint_path.read_text())
            logger.info("resuming: %d phase codes already complete", len(self.done))

    def _save_checkpoint(self) -> None:
        tmp = self.checkpoint_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.done, ensure_ascii=False, indent=2))
        os.replace(tmp, self.checkpoint_path)

    def _menu(self, type_code: str) -> dict:
        backoff = 20.0
        for _ in range(6):
            try:
                r = self.client.get(API, params={"typeCode": type_code})
            except httpx.HTTPError as exc:
                logger.warning("network error (%s); backing off %.0fs", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue
            if r.status_code == 200:
                time.sleep(self.delay)
                try:
                    return r.json()
                except Exception:
                    logger.warning("non-JSON 200 for %s; backing off %.0fs", type_code, backoff)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 300)
                    continue
            if r.status_code == 429 or r.status_code >= 500:
                logger.warning("HTTP %d for %s; backing off %.0fs", r.status_code, type_code, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue
            raise RuntimeError(f"HTTP {r.status_code} for {type_code}: {r.text[:150]}")
        raise RateLimited("too many consecutive failures — likely blocked")

    def collect_blocks(self, phase_code: str) -> list[dict]:
        """Resolve one phase code down to leaf blocks, recursing on hasSubLevel."""
        blocks: list[dict] = []
        stack = [phase_code]
        seen = {phase_code}
        while stack:
            code = stack.pop()
            menu = self._menu(code)
            items = menu.get("menuItems") or []
            if menu.get("hasSubLevel"):
                for item in items:
                    child = item.get("typeCode")
                    if child and child not in seen:
                        seen.add(child)
                        stack.append(child)
            else:
                for item in items:
                    blocks.append({
                        "phase_code": phase_code,
                        "block_name": item.get("name"),
                        "block_type_code": item.get("typeCode"),
                    })
        return blocks

    def _write(self, phase_code: str, blocks: list[dict]) -> None:
        safe_name = phase_code.replace("/", "_")
        shard = self.out / "rows" / f"{safe_name}.jsonl"
        tmp = shard.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for row in blocks:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(tmp, shard)

    def run(self, phase_codes: list[str]) -> None:
        pending = [c for c in phase_codes if c not in self.done]
        logger.info("%d phase codes to pull (%d already done)", len(pending), len(self.done))
        if not pending:
            logger.info("nothing to do.")
            return
        for i, code in enumerate(pending, 1):
            try:
                blocks = self.collect_blocks(code)
            except RateLimited as exc:
                logger.error("STOPPING (resumable): %s", exc)
                self._save_checkpoint()
                raise
            self._write(code, blocks)
            self.done[code] = {"blocks": len(blocks)}
            if i % 200 == 0 or i == len(pending):
                self._save_checkpoint()
            if i % 500 == 0:
                total = sum(v["blocks"] for v in self.done.values())
                logger.info("[%d/%d] %d phases done, %d blocks collected so far", i, len(pending), len(self.done), total)
        self._save_checkpoint()
        total = sum(v["blocks"] for v in self.done.values())
        logger.info("DONE. %d phase codes, %d blocks collected.", len(self.done), total)

    def load_all_rows(self) -> list[dict]:
        """Read every collected block row back from the rows/ shards."""
        rows: list[dict] = []
        for shard in sorted((self.out / "rows").glob("*.jsonl")):
            with shard.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
        return rows
