# property-scraper

Kedro project that scrapes Hong Kong property transactions from two agencies and
publishes consolidated Excel reports.

## Source naming

`source_a` = Centaline, `source_b` = Midland.

The split came from an anonymisation attempt: internal identifiers stay generic,
user-facing output uses the real names (`SOURCE_DISPLAY_LABELS` in
`data_process/nodes.py`). That policy no longer holds — the published reports name
both agencies in their sheet tabs, `README.md` carries the mapping outright, and
the repo is public. Read the two names as interchangeable, and use the real names
in anything new.

Data files lag the pipeline names on purpose: `source_a_res` reads and writes
`centaline_res_trans_lv_0.parquet`, `source_b_commercial` writes
`midland_ici_building_details.parquet`. Renaming them would orphan existing data.

## `kedro run` scrapes everything

`__default__` is all pipelines combined, so a bare `kedro run` launches all four
scrapers against live sites — hours of traffic. Always name one:
`kedro run --pipeline=data_process`.

## Published reports are committed on purpose

`data/03_primary/RE_*.xlsx` (~141 MB) are pipeline *outputs* that are deliberately
tracked — the repo is public and sharing the reports is the point. Running
`data_process` rewrites them, so a dirty working tree after a run is expected.
Leave them tracked.

Everything else under `data/` is gitignored (`data/**`), so adding a new report
file needs `git add -f`.

## `conf/local/` is required to scrape

Real API endpoints and request headers live in `conf/local/parameters.yml`, which
is gitignored. Without it the four scraper pipelines cannot run. `data_process`
and `building_master` work off existing data files and run fine without it.

## building_master

Reconciles buildings across the two agencies — they name the same building
differently — by fuzzy matching, emitting a canonical master, a source-to-canonical
bridge, and a review queue for uncertain matches. Thresholds live in
`parameters.yml` under `building_master_matching` / `building_master_review`.

Human decisions are meant to feed back through a loop: the pipeline writes
`data/08_reporting/building_master_override_template.xlsx`, a human fills it in,
the result is saved to `conf/base/curated/building_master_reviewed_overrides.csv`,
and `resolve_canonical_buildings` reads it on the next run to pin those matches.

That loop is currently broken, verified by running it end to end:

- The template emits `proposed_canonical_building_id` / `decision`; the reader
  (`matching.py:681-685`) requires `canonical_building_id` / `review_status`.
  Saving the template as-is raises `ValueError: reviewed_overrides missing
  required columns`.
- Merging two records requires filling the *same* canonical id on both of their
  rows. Filling one row alone silently produces a one-member canonical building
  marked `approved`.
- Only the literal `approved` is honoured. `rejected` is read and discarded, so a
  human verdict of "these are genuinely different buildings" cannot be recorded
  and the pair returns to the queue every run. Approved pairs also stay in the
  queue.
- 13.1% of `source_record_id`s are content fingerprints derived partly from the
  modal transaction address, and they cover 48.5% of the review queue. One new
  transaction can flip the fingerprint. When an override's id no longer resolves,
  `matching.py:693-694` skips the row silently — the human decision is lost with
  no warning.

The CSV is header-only; no overrides have ever been recorded, which is why none
of this surfaced before.

## Source C is retired

`conf/base/parameters.yml` has a `buildings:` block for a third source
(`source_c`), `conf/base/parameters/buildings.yml` holds its full config, and
`utils/web_scraping.py` still branches on `source_c.com`. None of it is wired to a
pipeline — no `buildings` pipeline exists in the registry, and nothing references
`params:geocoding` / `params:node_tracking` / `params:buildings`.

`building_master` covers what that config aimed at (matching, name cleaning,
source priority, manual review), so read building-consolidation settings from
`parameters.yml` under `building_master_*`.

## Conventions

Scraping parameters (delays, retries, concurrency, thresholds, export year ranges)
belong in `conf/base/parameters.yml`, not as literals in node functions.

Source-specific scraping and parsing stays in its own `source_*` pipeline;
cross-source harmonisation belongs in `data_process`, cross-source building
identity in `building_master`.
