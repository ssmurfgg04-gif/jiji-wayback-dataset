# v2 Real Data Extraction Architecture (2026-08-14)

The v2 pipeline mines **real** African marketplace listings from two archival
sources: the Internet Archive Wayback Machine and the Common Crawl WARC
archive. It replaces the v1 synthetic generator with actual captured data.

## Data Flow

```
                   +---------------------------+
                   |  jiji domains (7 total)    |
                   |  ke ng tz ug za et gh      |
                   +-------------+-------------+
                                 |
              +------------------+------------------+
              |                                     |
   +----------v-----------+              +----------v-----------+
   | PATH A: Common Crawl |              | PATH B: Wayback      |
   | WARC archive         |              | Machine CDX + API    |
   +----------------------+              +----------------------+
              |                                     |
   cc_index_collect.py                    wayback_miner.py
   (index.jsonl via index API)            (CDX pagination, 4-6 workers)
              |                                     |
   cc_fetch.py                            listings.tsv
   (resumeable S3 segment fetches)        (6,236 rows, rich fields)
              |
   cc_bodies/*.gz (gzipped HTML/JSON)
              |
   cc_meta_build.py -> cc_meta.jsonl (body-hash -> url/ts/country)
              |
   cc_parse.py -> cc_rows.tsv (17,372 rows)
              |
   +----------v----------+
   | PATH C: path_c_merge.py            |
   |  - dedup by GUID                   |
   |  - currency normalize to KES       |
   |  - temporal features: days_listed, |
   |    price_delta, price_pct_change,  |
   |    relative_price                   |
   |  - export CSV + Parquet             |
   +-------------------------------------+
```

## PATH A: Common Crawl

1. `cc_index_collect.py` queries the Common Crawl index API
   (`https://index.commoncrawl.org/CC-MAIN-*/index`) for all 7 domains.
   Verified crawls: CC-MAIN-2022-49, 2023-14, 2023-40, 2024-10, 2024-33.
   Completed index sets: `jiji.co.ke` (321k) and `jiji.ng` (402k).
   The remaining 5 domains are rate-limited (~720 req/day rule) and are
   queued in `cc_index/` for background completion.
2. `cc_fetch.py` downloads the WARC segments referenced by the index from
   AWS S3 Open Data buckets (no rate limits), gzip-compressing each body to
   `cc_bodies/<hash>.gz`. The engine is **resumeable**: committed bodies are
   tracked in a journal, and progress survives restarts.
3. `cc_meta_build.py` builds a cached map `bodyhash -> [url, capture_ts,
   country]` from the index journals (currently 723,309 keys).
4. `cc_parse.py` parses HTML category pages for listing cards: price, title,
   GUID (from href), capture date, country. Rows are appended to
   `cc_rows.tsv`, keyed by body-hash so re-runs only process new bodies.

## PATH B: Wayback Machine

`wayback_miner.py`:

- CDX API pagination per domain (`pageSize=150000`, `showNumPages=true`)
- 4-6 worker threads, 1.0s base delay, Retry-After-aware rate limiting
- Fetches listing JSON via the archived API endpoints
  (`api_web/v1/listing/...`) with filename-verification to avoid duplicate
  captures
- Extracts rich seller data: seller_id/name, phone, category, view/favourite
  counts, feedback, rating, boost badge, image URLs
- Writes `listings.tsv` (6,236 rows across all 7 domains)

## PATH C: Post-Processing

`path_c_merge.py`:

1. **Canonical merge** - union of path A + B rows keyed by GUID; wayback
   rich fields preferred where both exist
2. **Currency normalization** - parse local price, tag with ISO currency
   (`KES/NGN/TZS/UGX/ZAR/ETB/GHS`), convert to KES baseline via
   `CURRENCY_TO_KES` (approximate rates)
3. **Temporal features** - `first_seen`/`last_seen`, `capture_count`,
   `days_listed`, `price_first/last/min/max`, `price_delta`,
   `price_pct_change`, `relative_price`
4. **Content features** - `has_images`, `image_url_count`
5. **Export** - CSV (UTF-8) + Parquet (pyarrow) + `pipeline_metadata_*.json`

## Current Results (2026-08-14 snapshot)

| Metric | Value |
|--------|-------|
| Merged unique GUIDs | 21,283 |
| Wayback rows | 6,236 |
| Common Crawl rows | 17,372 |
| With price | 21,227 |
| Multi-capture listings | 16,020 |
| Country mix | ke 17,267 / ng 1,735 / gh 2,224 / tz 57 |

## Updating the Dataset

The CC fetch engine runs in the background and can be resumed at any time
(the WARC journal prevents re-fetching). To refresh:

```bash
python scripts/cc_parse.py        # parse newly committed bodies
python scripts/path_c_merge.py    # rebuild CSV + Parquet
```

## Environment Notes

- Python 3.12, requests, beautifulsoup4, pyarrow, numpy
- The working directory contains a helper module named `inspect.py`
  (a Wayback network helper) that shadows the stdlib `inspect`; all scripts
  pin the stdlib first via `sys.path.insert(0, os.path.dirname(os.__file__))`
  before importing numpy/pyarrow.