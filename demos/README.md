# Demos

Class demonstration notebooks. The instructor runs these live — and the serverless demos run in
**your own account** too, so pull this repo as a Git folder and run along, or re-run them afterwards.

## Advanced Data Engineering (Days 3–4) — `de_module_*`

Two per module, from the module narratives. Tested 2026-09-29 in the trial workspace; notebooks
whose last cell fails do so **on purpose** (the failure is the lesson). M4/M5 demos drive the
real `bundles/solutions/lab-10-pipeline` and `lab-11-job` artifacts — deploy those first.

- `de_module_1_demo_1_external_location.sql` — credential-is-not-SQL, external location, managed-vs-external drop contrast. Final SELECT errors on purpose. External-location cells need the instructor workspace's storage credential.
- `de_module_1_demo_2_grant_a_peer.sql` — three privileges from the platform-owner side; `peer` widget.
- `de_module_2_demo_1_lazy_dag.py` — lazy eval; the accepted-but-broken transformation; last cell fails on purpose. Spark UI DAG step needs classic compute.
- `de_module_2_demo_2_find_the_straggler.py` — builds guaranteed 60/40 skew (2M rows), Summary Metrics max-vs-median walk. Classic cluster for the UI.
- `de_module_3_demo_1_txn_log_time_travel.sql` — DESCRIBE HISTORY, VERSION AS OF 0, fresh versions each run, retention framing.
- `de_module_3_demo_2_small_files.py` — 20 tiny appends → numFiles before/after OPTIMIZE → CLUSTER BY.
- `de_module_4_demo_1_autoloader_raw.py` — raw cloudFiles with schemaLocation+checkpoint, new-file-processed-once, `_rescued_data`, then the declarative contrast. Self-contained landing dir.
- `de_module_4_demo_2_expectations_cdc.py` — warn/drop/fail against the running Lab 10 pipeline; SCD2 evidence from AUTO CDC FROM SNAPSHOT.
- `de_module_5_demo_1_task_values_gate.py` — task values (Python-only), If/else operands, both branches via the threshold variable; presenter script for the Lab 11 job.
- `de_module_5_demo_2_event_log_run_as.py` — event_log() as owner, the Run As failure, SET OWNER + repair-run.
- `de_module_6_demo_1_manifest_to_deploy.py` — terminal script: validate/deploy/run the Lab 10 bundle; CLI must be v1.x (0.2xx fails downloading Terraform).
- `de_module_6_demo_2_adopt_running_resource.py` — `bundle generate job` + `deployment bind`, syntax verified on CLI v1.18; GitLab CI sketch (`jesseroi/db-on-aws`).
- `serverless_profile_vs_spark_ui.md` — **verified capability matrix**: what the serverless query profile covers (DAG, shuffle, task counts, spill) vs what genuinely needs the classic Spark UI (per-task skew, stragglers, executors).

## Day 1 (serverless — run along in your own account)

- `module_1_lazy_evaluation.py` — read → transform → action, then the typo cell that succeeds and the innocent cell that fails. **The last cell fails on purpose.**
- `module_2_translating_a_real_query.sql` — TOP/LIMIT, backticks, LENGTH (the 120-char seed), ISNULL→COALESCE+NULLIF, the silently-wrong DATEDIFF argument order, then `:start_date`/`:end_date` parameter widgets.
- `module_3_four_checks.sql` — the four-check UAT framework against the real migrated data: 62,080 vs 61,699, the charter-250 anti-join, paired aggregates, and the naive-vs-normalised row comparison with the LENGTH() reveal. Does not name the total defect count — the lab is the discovery.
- `module_3_time_travel.sql` — DESCRIBE HISTORY, VERSION AS OF 0 (the 62,080 reveal: the gap happened *inside* the table's lifetime), TIMESTAMP AS OF, and the 7-day retention limits.
- `module_3_python_primer.py` - a first taste of Python for the Day 1 close: variables, f-strings, lists/loops, a function, the PySpark bridge, and the one-loop-many-tables payoff. Ends with a table of free learning resources (all links verified).
- `delta_under_the_hood.sql` - the files and the log without filesystem access: DESCRIBE DETAIL, the hidden `_metadata` column listing the real Parquet files, DESCRIBE HISTORY, then an UPDATE on a scratch copy to watch new files appear and the old version stay readable.

## Day 2 (serverless unless noted)

- `module_4_demo_1_etl_path.py` — the 9-minute ETL arc: read → transform → aggregate → the same
  thing in `%sql` (the "same engine" reassurance) → `saveAsTable` → prove it's real. Serverless.
- `module_4_demo_2_why_slow.py` — the 7-minute Spark UI walk: one visible aggregation over the
  2M-row perf tables, stages, shuffle read/write, task count, and the one-sentence takeaway.
  **Classic cluster required** — start it before the session.
- `module_5_demo_1_grant_fail.sql` — the correct grant that fails: SELECT granted and visible,
  access denied anyway, fixed by USE CATALOG + USE SCHEMA with SELECT untouched. Needs a second
  identity. Replace `<colleague>` before class.
- `module_5_demo_2_publish_compare_alert.sql` — view vs materialized view honestly compared,
  REFRESH via serverless pipeline, then the alert flow including the you-cannot-reuse-a-saved-query
  frustration, shown deliberately.
- `module_5_demo_3_row_filter.sql` — BONUS (not in the outline): row filter + column mask on
  the nic_econ enrichment data — same table, per-user truths; ties grants (Demo 1) to the
  Module 6 credential decision. Needs the second identity; replace `<colleague>` before class.
- `module_6_demo_1_dashboard.sql` — click path for the live dashboard build: two charts, one
  dataset, a filter scoped to both, the deliberately mis-scoped filter, the fix, cross-filtering.
- `module_6_demo_2_genie.sql` — Genie right and wrong: the well-formed question, the
  underspecified one, the silently-resolved ambiguity, and the read-the-SQL verification habit.
  Needs a curated Genie space; rehearse both questions.
- `spark_ui_follow_along.md` — the five-artifact Spark UI follow-along (partitions, shuffle,
  skew, spill, OOM). Formerly Lab 4 Part 5; removed from the lab 2026-09-28 because the Spark UI
  is a DE topic — in the DA class it is demo-only. Pairs with `spark_ui_showcase.py`.

## Classic-cluster / Advanced

- `spark_ui_showcase.py` — partition sizes, shuffle, skew, spill, and OOM, each producing one artifact in the Spark UI. Requires a **classic cluster** (the per-task Spark UI view does not exist on serverless — see `serverless_profile_vs_spark_ui.md` — and Free Edition cannot create classic compute), so in class this runs on the instructor's shared cluster. The final cell fails on purpose.
- `delta_log_old_school.py` - **Advanced course (Day 3, pairs with Lab 9 Delta internals)** - the actual `_delta_log` JSON files, listed and read, via a hive_metastore table on the DBFS root (UC tables never expose theirs). Creates and updates its own scratch table, then ties the raw files back to DESCRIBE HISTORY. Follow-on talking point: in a customer-managed AWS deployment the same files are browsable in the S3 console, because you own the bucket - UC controls the front door, your AWS account controls the building.
- `spark_ui_showcase_demo_script.md` — step-by-step Spark UI navigation for demoing the above.
