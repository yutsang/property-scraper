# Source A Residential – Rescraping Guide

This document describes the full flow for source_a_res pipeline rescraping: how district names/codes are obtained, how transactions and estates are scraped, and how to ensure totals match the website.

---

## 1. District Name and Code Sources

### 1.1 Area Codes Flow

| Step | Source | Output |
|------|--------|--------|
| 1 | **Base CSV** `data/01_raw/Source A_Res_Area_Code.csv` | Contains: `Region`, `District`, `Subdistrict`, `Code` |
| 2 | **Sitemap** `https://hk.centanet.com/sitemap.xml` | Child sitemaps: `sitemap_TC_HMA_buy.xml`, `sitemap_TC_HMA_rent.xml`, `sitemap_TC_HMA_transaction.xml` |
| 3 | **Node** `update_area_codes_from_sitemap` | Parses URLs like `/list/transaction/pok-fu-lam_19-HMA155` → `(subdistrict_slug, code)` |
| 4 | **Merge** | New `(Subdistrict, Code)` from sitemap merged into existing CSV. Region/District inferred (default: `district=subdistrict`, `region=Hong Kong Island`) |

**Important:**
- The sitemap only provides `(slug, code)` pairs. Region and District come from the existing CSV or are defaulted.
- Slug → display name: `pok-fu-lam` → `Pok Fu Lam` via `slug_to_subdistrict()`.
- To get the canonical list: run `update_area_codes_from_sitemap` first; it adds any new districts from sitemap to the CSV.
- The CSV is the source of truth for `Region`, `District`, `Subdistrict`, `Code` used by scrapers.

### 1.2 Rescraping Parameters

For a full rescraping run, set in `conf/base/parameters.yml`:

```yaml
source_a_res:
  full_rerun: true
  transaction_full_rerun: true
  estate_full_rerun: true
```

Or pass at runtime: `kedro run --params source_a_res.full_rerun=true`

---

## 2. Transaction Scraping

### 2.1 Data Sources (HTML + JS)

| Source | Path | Fields |
|--------|------|--------|
| **JavaScript** | `window.__NUXT__.state.transaction.transactionList.data[]` | date, region, district, subdistrict, building_code, estate_code, price, area, g_area, n_area, etc. |
| **HTML Table** | `tr.cv-structured-list-item` | date, address, price, area, ft_price (visible on list page) |

### 2.2 Merge Strategy

- **Primary**: JS (`__NUXT__`) has full metadata (building codes, dates, region, district).
- **Fallback**: HTML supplies `area`, `price`, `ft_price` when JS has `None`.
- **Merge by index**: Record `i` from JS is merged with record `i` from HTML (both from same page, same order).
- If only one source has data, that source is used.

### 2.3 Total Count Validation

- **Website**: Shows "Sold: X Leased: Y" → total = X + Y per district.
- **__NUXT__**: May have `transactionList.total` (check structure).
- **Scraper**: Paginates until control_date or max_pages.
- **Validation**: After scraping each district, compare our count vs website total. Log `WARNING` if mismatch.

### 2.4 Potential Mismatch Causes

1. **Index misalignment**: HTML and JS rows in different order → wrong fallbacks.
2. **Pagination cutoff**: `max_pages_per_area` or `control_date` stops before all records.
3. **__NUXT__ not ready**: Page saved before JS hydrated → empty or partial data.
4. **Different filters**: Website may show filtered results; we scrape default sort.

---

## 3. Estate Scraping

### 3.1 Data Sources (HTML + JS)

| Source | Path | Fields |
|--------|------|--------|
| **HTML** | `a.property-text.flex.def-property-box` | Name, Address, Blocks, Units, UnitRate, MoM, ForSale, ForRent, Link |
| **__NUXT__** | `state.estate` or `state.property` (if present) | May have estate list – check at runtime |

### 3.2 Current Behavior

- **Phase 1 (Playwright probe)**: Load district page 1 with Playwright, parse page-1 estate count plus any visible / JS total, and skip if both match stored metadata.
- **Phase 2 (Playwright)**: Full scrape with pagination for districts that changed.
- **website_total** used for skip logic; scraped count should match after full pagination.

### 3.3 Total Count Validation

- **Website**: Shows "X Estate(s)" or similar.
- **Scraper**: Sum of all estates from paginated pages.
- **Validation**: After scraping a district, `len(district_estates)` should equal `website_total` (or we should log mismatch).

### 3.4 Potential Mismatch Causes

1. **website_total parsing**: Wrong selector or format → incorrect total.
2. **Pagination**: "Next" button not clicked or disabled early.
3. **Lazy load**: Estates load on scroll; we may miss some if not scrolling.
4. **__NUXT__ not used**: If estate list exists in JS and differs from HTML, we only use HTML.

---

## 4. Rescraping Checklist

- [ ] Set `full_rerun`, `transaction_full_rerun`, `estate_full_rerun` if doing full rescrape.
- [ ] Run `update_area_codes_from_sitemap` to refresh district list from sitemap.
- [ ] Verify `Source A_Res_Area_Code.csv` has correct Region/District/Subdistrict/Code.
- [ ] Run transaction scraper; check logs for per-district count mismatches.
- [ ] If the local transaction parquet is corrupt or 0-byte, restore a backup before retrying.
- [ ] Run estate scraper; check logs for per-district count mismatches.
- [ ] Compare global totals: transactions and estates vs website banner/footer totals.

---

## 5. Pipeline Order

```
check_api_health → update_area_codes → estate_listing_scraper → estate_detail_scraper
                                         ↘ transaction_data_scraper → transaction_processor
                                                                              ↘
                                                                    estate_data_enricher
```

To rescrape only transactions and estates (skip health/area update):

```bash
kedro run --from-nodes estate_listing_scraper --to-nodes estate_data_enricher --params source_a_res.transaction_full_rerun=true source_a_res.estate_full_rerun=true
```

To do a full rescraping run (all nodes, fresh data):

```bash
kedro run --params source_a_res.full_rerun=true source_a_res.transaction_full_rerun=true source_a_res.estate_full_rerun=true
```

### Count mismatch investigation

- **Transaction mismatch**: Check logs for `⚠️ Transaction count exceeds website` or `scraped=X website_total=Y`. If scraped > website_total, data may be duplicated. If scraped < website_total after pagination, check `max_pages_per_area`, network timeouts, or pagination logic.
- **Estate mismatch**: Check logs for `⚠️ Estate count mismatch`. If scraped ≠ website_total, verify `website_total` selectors in Phase 1, pagination in Phase 2, or lazy-loaded content.
