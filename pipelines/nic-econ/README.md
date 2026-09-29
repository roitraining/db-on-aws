# NIC + Economic Enrichment Pipeline

A medallion pipeline (bronze → silver → gold) that enriches the FFIEC NIC institution data with
FRED, BLS LAUS, Census PEP/ACS, FDIC deposits, NOAA weather, NFL results, and card-spending data,
rolled up to county / state / MSA views for an AI/BI dashboard.

Built to run on **serverless compute in a workspace with restricted egress** — the same environment
this course's labs use. All API-sourced data ships pre-fetched in `data/`; only GitHub-hosted
sources (nflverse, Opportunity Insights) are downloaded live, because GitHub is reachable from
serverless where most of the internet is not.

## Run order

| Step | Notebook | Notes |
|---|---|---|
| 1 | `notebooks/01_setup` | catalog `nic_econ`, schemas, landing volume |
| 2 | `notebooks/01b_stage_from_repo` | copies `data/` into the landing volume (never overwrites) |
| 3 | `notebooks/03c_bronze_staged` | loads all bronze tables; NIC zips come from the `training_nic` volume staged by Lab 0, or upload your own to `landing/nic_zips/` |
| 4 | `notebooks/04_silver` | dims + facts |
| 5 | `notebooks/05_gold` | county/state/MSA metrics, macro, weather × spending, Bears × spending |
| 6 | `notebooks/06_dashboard_datasets.sql` | AI/BI dashboard dataset queries |

Run each as a serverless job task or interactively. Widgets: `start_year` (default 2015 — the
staged data covers **2024+**, so pass `start_year=2024`), `contact_email`.

## What's pinned in `data/`

| Folder | Contents | Fetched |
|---|---|---|
| `ref/` | Census state.txt, county gazetteer 2024, OMB CBSA delineation 2023 (as CSV) | 2026-09-28 |
| `fred/` | 314 series (8 national + 6 per state), observations 2024-01 → fetch date | 2026-09-28 |
| `census/` | PEP county totals, 2020 + 2024 vintages | 2026-09-28 |
| `bls/` | LAUS county unemployment, monthly, 2024+ (gzipped) | 2026-09-28 |
| `fdic/` | Summary of Deposits 2024–2026, all branches (gzipped) | 2026-09-28 |

**Not shipped** (size): NOAA GHCN daily weather (~65 MB/year gzipped). Fetch locally with
`scripts/fetch_rest_local.py`-style streaming filter and upload to `landing/noaa/` — the loader
picks up `noaa_ghcn_<year>.csv.gz` automatically. Without it, weather tables are empty and
everything else still works.

**Census ACS** requires an API key (mandatory since 2025): get one at
api.census.gov/data/key_signup.html, fetch `census_acs5.csv`, drop it in `landing/census/`.
Without it, county metrics lack income/poverty columns (population comes from PEP).

## Known environment gotchas (verified 2026-09-28)

- Serverless egress in the class trial workspace allows **GitHub raw only**; public S3 buckets get
  connection-reset and most DNS fails. Hence the repo-as-data-channel pattern.
- The 2026 NIC extract uses `county_cd`, not `cnty_cd` — already fixed in `00_config`.
- NIC RELATIONSHIPS / TRANSFORMATIONS files aren't in the pinned set; the loader creates empty
  fallback tables so silver/gold run (institution events come out empty).
- Temp tables note for instructors: `CREATE TEMP TABLE` works on serverless SQL warehouses
  (Databricks SQL, late 2025) — DELETE unsupported on them.
