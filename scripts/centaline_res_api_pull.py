#!/usr/bin/env python3
"""Standalone, resumable puller for the full Centaline residential transaction
history via the public JSON API.

This does NOT touch the Kedro pipeline or any existing dataset. It reads the raw
transaction parquet read-only (only to seed building codes) and writes only to a
fresh output directory.

Why it is shaped this way
-------------------------
- The API caps `size` at 100 and `offset` at ~10,000, so any query returning
  more than ~10k rows cannot be paged to the end. Areas are segmented
  recursively into estate/building `typeCode`s (discovered from the API's own
  responses, seeded from the existing parquet) until every leaf is under the
  ceiling.
- Omitting the `day` parameter returns full history rather than the 3-year
  rolling window the site's UI uses.
- The token is an anonymous JWT the public frontend mints for every visitor;
  it is grabbed once from a headless browser and refreshed on 401.
- Progress is checkpointed at area granularity, so an IP block or crash loses at
  most the area in flight. Re-running resumes from the checkpoint.

Usage
-----
  python scripts/centaline_res_api_pull.py                 # full pull, resumable
  python scripts/centaline_res_api_pull.py --areas HMA051 HMA179   # subset
  python scripts/centaline_res_api_pull.py --delay 0.8     # gentler pacing
  python scripts/centaline_res_api_pull.py --max-calls 500 # stop after N calls
Re-run the same command to resume; completed areas are skipped.

Note: list --areas codes literally (space-separated on the command line). Do not
pass them via an unquoted shell variable in zsh — zsh does not word-split it, so
`--areas $CODES` arrives as one giant area name.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from pathlib import Path

import httpx

API = "https://hk.centanet.com/findproperty/api/Transaction/Search"
TOKEN_PAGE = ("https://hk.centanet.com/findproperty/en/list/transaction/"
              "anderson_19-HMA051")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

PAGE_SIZE = 100
OFFSET_CEILING = 9900          # last usable offset; count <= 10000 is fully pageable
REACHABLE = OFFSET_CEILING + PAGE_SIZE
DEFAULT_OUT = "data/centaline_res_api_full"
AREA_CODE_CSV = "data/01_raw/Centanet_Res_Area_Code.csv"
SEED_PARQUET = "data/01_raw/centaline_res_trans_lv_0.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("centaline_pull")


class RateLimited(Exception):
    """Raised when the source looks like it is blocking us (bans / sustained 5xx)."""


class Puller:
    def __init__(self, out_dir: str, delay: float, max_calls: int | None):
        self.out = Path(out_dir)
        (self.out / "rows").mkdir(parents=True, exist_ok=True)
        self.delay = delay
        self.max_calls = max_calls
        self.calls = 0
        self.token: str | None = None
        self.client = httpx.Client(timeout=40)
        self.checkpoint_path = self.out / "checkpoint.json"
        self.done: dict[str, dict] = {}
        self._load_checkpoint()
        self.seed_codes = self._load_seed_codes()

    # ---- token -----------------------------------------------------------
    def refresh_token(self) -> None:
        """Grab the anonymous bearer token the public frontend uses."""
        from playwright.sync_api import sync_playwright

        logger.info("acquiring anonymous token via headless browser...")
        holder = {"t": None}
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=UA)
            page = ctx.new_page()

            def grab(req):
                if "/findproperty/api/" in req.url and "authorization" in req.headers:
                    holder["t"] = req.headers["authorization"]

            page.on("request", grab)
            page.goto(f"{TOKEN_PAGE}?q=session_{self.calls}_{int(self.delay*1000)}",
                      wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(4000)
            browser.close()
        if not holder["t"]:
            raise RateLimited("could not obtain a token (site may be blocking)")
        self.token = holder["t"]
        logger.info("token acquired")

    def _headers(self) -> dict:
        return {"authorization": self.token, "content-type": "application/json",
                "referer": TOKEN_PAGE, "user-agent": UA}

    # ---- checkpoint ------------------------------------------------------
    def _load_checkpoint(self) -> None:
        if self.checkpoint_path.exists():
            self.done = json.loads(self.checkpoint_path.read_text())
            logger.info("resuming: %d areas already complete", len(self.done))

    def _save_checkpoint(self) -> None:
        tmp = self.checkpoint_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.done, ensure_ascii=False, indent=2))
        os.replace(tmp, self.checkpoint_path)

    def _load_seed_codes(self) -> dict[str, set[str]]:
        """Read-only: known building codes per area, to seed segmentation."""
        seeds: dict[str, set[str]] = {}
        try:
            import pandas as pd

            df = pd.read_parquet(SEED_PARQUET, columns=["area_code", "building_code"])
            for area, grp in df.dropna().groupby("area_code"):
                codes = {str(c) for c in grp["building_code"].unique() if str(c).strip()}
                seeds[str(area)] = codes
            logger.info("seeded building codes for %d areas from existing parquet", len(seeds))
        except Exception as exc:  # seeding is optional
            logger.warning("no seed codes (%s); relying on dynamic discovery only", exc)
        return seeds

    # ---- one API call with retry/backoff/token-refresh -------------------
    def _post(self, body: dict) -> dict:
        backoff = 30.0
        for attempt in range(8):
            if self.max_calls is not None and self.calls >= self.max_calls:
                raise RateLimited(f"reached --max-calls={self.max_calls}")
            self.calls += 1
            try:
                r = self.client.post(API, headers=self._headers(), json=body)
            except httpx.HTTPError as exc:
                logger.warning("network error (%s); backing off %.0fs", exc, backoff)
                time.sleep(backoff); backoff = min(backoff * 2, 600); continue
            if r.status_code == 200:
                time.sleep(self.delay + random.uniform(0, self.delay))
                return r.json()
            if r.status_code in (401, 403):
                logger.warning("HTTP %d — refreshing token", r.status_code)
                self.refresh_token(); continue
            if r.status_code == 429 or r.status_code >= 500:
                logger.warning("HTTP %d (attempt %d) — backing off %.0fs",
                               r.status_code, attempt + 1, backoff)
                time.sleep(backoff); backoff = min(backoff * 2, 600); continue
            # 4xx other than auth: a bad request for this body; surface it.
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        raise RateLimited("too many consecutive failures — likely blocked")

    def _body(self, type_code: str, hmas: list[str], size: int, offset: int,
              order: str = "Descending") -> dict:
        return {"postType": "Both", "sort": "InsOrRegDate", "order": order,
                "size": size, "offset": offset, "pageSource": "search",
                "bigPhotoMode": False, "hmas": hmas, "mtrs": [],
                "primarySchoolNets": [], "markets": [], "universities": [],
                "typeCodes": [type_code], "unitPlanGroupFilter": {}}

    def _count(self, type_code: str, hmas: list[str]) -> int:
        return int(self._post(self._body(type_code, hmas, 1, 0)).get("count", 0))

    def _page_segment(self, type_code: str, hmas: list[str], sink: dict,
                      order: str = "Descending") -> set[str]:
        """Page the reachable portion of a segment into `sink` (id->row); return
        the distinct child typeCodes seen, for further segmentation."""
        discovered: set[str] = set()
        offset = 0
        while offset <= OFFSET_CEILING:
            data = self._post(self._body(type_code, hmas, PAGE_SIZE, offset, order)).get("data", [])
            if not data:
                break
            for row in data:
                rid = str(row.get("id"))
                sink[rid] = row
                code = row.get("typeCode")
                if code:
                    discovered.add(str(code))
            if len(data) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        return discovered

    # ---- collect one whole area -----------------------------------------
    def collect_area(self, hma: str) -> tuple[dict, int]:
        sink: dict[str, dict] = {}
        area_code = f"19-{hma}"
        area_count = self._count(area_code, [hma])  # the only count call per area

        # Small area: page it straight through.
        if area_count <= REACHABLE:
            self._page_segment(area_code, [hma], sink, "Descending")
            return sink, area_count

        # Large area: page every constituent code in full. Paging a code from
        # offset 0 to exhaustion returns ALL of its rows regardless of the offset
        # ceiling, because a single estate/building is well under it. Codes come
        # from the area's newest page (recency + discovery) and from the known
        # building codes seeded off the existing parquet (which cover estates
        # that transacted only in the middle of the period). No per-code count
        # call — we just page until a short page ends it.
        # Harvest codes from both ends of the period. An estate active only in
        # the middle years appears in neither the newest 10k nor the seed, so the
        # oldest-10k pass is what pulls it into the segment list.
        discovered = self._page_segment(area_code, [hma], sink, "Descending")
        discovered |= self._page_segment(area_code, [hma], sink, "Ascending")
        todo = sorted(discovered | self.seed_codes.get(hma, set()))
        processed: set[str] = {area_code}
        while todo:
            # Once we hold as many rows as the area reported, the remaining seed
            # codes are all already captured — stop rather than re-page them.
            if len(sink) >= area_count:
                break
            code = todo.pop()
            if code in processed:
                continue
            processed.add(code)
            children = self._page_segment(code, [], sink, "Descending")
            # A giant estate ("3-" code) exceeds the ceiling; its building-level
            # children (harvested from its own rows) segment it further.
            for child in children:
                if child not in processed:
                    todo.append(child)
        return sink, area_count

    def _write_area(self, hma: str, sink: dict) -> None:
        shard = self.out / "rows" / f"{hma}.jsonl"
        tmp = shard.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for row in sink.values():
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(tmp, shard)

    # ---- driver ----------------------------------------------------------
    def run(self, areas: list[str]) -> None:
        pending = [a for a in areas if a not in self.done]
        logger.info("%d areas to pull (%d already done)", len(pending), len(self.done))
        if not pending:
            logger.info("nothing to do.")
            return
        if self.token is None:
            self.refresh_token()
        for i, hma in enumerate(pending, 1):
            try:
                sink, area_count = self.collect_area(hma)
            except RateLimited as exc:
                logger.error("STOPPING (resumable): %s", exc)
                self._save_checkpoint()
                logger.error("re-run the same command to resume from here.")
                sys.exit(2)
            self._write_area(hma, sink)
            coverage = len(sink) / area_count if area_count else 1.0
            self.done[hma] = {"collected": len(sink), "api_count": area_count,
                              "coverage": round(coverage, 4), "calls_so_far": self.calls}
            self._save_checkpoint()
            flag = "" if coverage >= 0.995 else f"  <-- shortfall {coverage:.1%}"
            logger.info("[%d/%d] %s: %d/%d rows (%.1f%%)  total_calls=%d%s",
                        i, len(pending), hma, len(sink), area_count,
                        coverage * 100, self.calls, flag)
        total = sum(v["collected"] for v in self.done.values())
        logger.info("DONE. %d areas, %d transactions collected, %d API calls.",
                    len(self.done), total, self.calls)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--delay", type=float, default=0.5, help="base delay between calls (s)")
    ap.add_argument("--areas", nargs="*", help="specific HMA codes (default: all)")
    ap.add_argument("--max-calls", type=int, default=None, help="stop after N calls (testing)")
    args = ap.parse_args()

    if args.areas:
        areas = args.areas
    else:
        import pandas as pd

        areas = pd.read_csv(AREA_CODE_CSV)["Code"].dropna().astype(str).tolist()

    puller = Puller(args.out, args.delay, args.max_calls)
    puller.run(areas)


if __name__ == "__main__":
    main()
