# Lab 10 Extended — Quarantine, Change Data Feed, Incremental Day Demo

The instructor's **extended** version of the Lab 10 medallion pipeline, deployed and
live-verified 2026-10-01. The plain solution matching the lab guide is
[`../lab-10-pipeline`](../lab-10-pipeline) — start there if you are following Lab 10.
This bundle adds what the guide does not cover:

| Addition | What it shows |
|---|---|
| `branches_quarantine` | The opposite of silver: rows **violating** the rules, built from the same `RULES` dict with the predicate inverted (12 on the initial load) |
| Change Data Feed | `delta.enableChangeDataFeed` on silver + quarantine; query with `table_changes('<table>', 2)` |
| `silver_changes_feed` | **In-pipeline CDF consumer** — a pipeline dataset streaming silver's own change feed into an append-only audit table, visible as a node in the graph. Same-pipeline CDF reads are undocumented but verified working (serverless, 2026-10-01) |
| Job (`resources/medallion_job.job.yml`) | The Lab 11 DAG: pipeline → event-log → If/else gate → promote (true) / halt (false). `promote_gold` **publishes** — a pipeline-created MV cannot be `REFRESH`ed from any job task (both failure modes verified) |
| `make_day_files.py` | Generates `branches_day2/3/4.csv` incremental files (clean rows + planted violations, named so the room can see them) and uploads them to `/Volumes/<catalog>/raw/landing/incremental_days/` |

**Demo driver notebook:** [`../../../demos/land_incremental_day.py`](../../../demos/land_incremental_day.py)
— BEFORE counts → land a day file → Start the pipeline → AFTER + inspection cells
(expectations per run, change feed per commit, update pre/post images).

## Instructor-workspace defaults (edit before deploying elsewhere)

This is the deployment as it runs in the instructor workspace, captured with
`bundle generate` + `deployment bind`:

- Target catalog defaults to **`eng_labtest`** (`variables.target_catalog`); the job
  notebooks and demo notebook reference `eng_labtest.work.*` literally.
- `targets.*.workspace.host` points at the instructor workspace.
- `make_day_files.py` reads `~/.databrickscfg` using `$DATABRICKS_CONFIG_PROFILE`
  (default `DEFAULT`).

## Verified figures (initial load + day 2)

bronze 175,914 → 176,219 · silver 175,907 → 176,209 · quarantine 12 → 17 ·
gold TX 11,199 → 11,501 · `silver_changes_feed`: commit 2 = 175,907 inserts,
commit 3 = 302 inserts. Expectations per run: `valid_key` 7 then 3 failed,
`plausible_city` 5 then 2. (Attendee environments without the legacy demo files
start ~2,000 lower: bronze 173,914.)

## Known boundaries (all live-verified)

- A pipeline-created materialized view can only be refreshed **by its pipeline** —
  not from a serverless notebook task (`MV_NOT_ENABLED_ON_SERVERLESS_GENERIC_COMPUTE`)
  nor a SQL-warehouse task ("Cannot REFRESH a Materialized View created from DLT").
- CDF is **not supported on tables with row filters or column masks** (docs).
- `table_changes(..., 0)` fails before the CDF enable version — start from the enable
  commit (here, 2).
- Out-of-band DML on a pipeline streaming table works and lands in its change feed,
  but a **full refresh recomputes the table and reverts it**.
