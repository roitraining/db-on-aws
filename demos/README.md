# Demos

Instructor demonstration notebooks. These are shown live in class; they are here so you can re-read them afterwards. In class workspaces they live at `/Shared/db-on-aws/`.

## Day 1 (run on serverless in the instructor's account)

- `module_1_lazy_evaluation.py` — read → transform → action, then the typo cell that succeeds and the innocent cell that fails. **The last cell fails on purpose.**
- `module_2_translating_a_real_query.sql` — TOP/LIMIT, backticks, LENGTH (the 120-char seed), ISNULL→COALESCE+NULLIF, the silently-wrong DATEDIFF argument order, then `:start_date`/`:end_date` parameter widgets.
- `module_3_four_checks.sql` — the four-check UAT framework against the real migrated data: 62,080 vs 61,699, the charter-250 anti-join, paired aggregates, and the naive-vs-normalised row comparison with the LENGTH() reveal. Does not name the total defect count — the lab is the discovery.
- `module_3_time_travel.sql` — DESCRIBE HISTORY, VERSION AS OF 0 (the 62,080 reveal: the gap happened *inside* the table's lifetime), TIMESTAMP AS OF, and the 7-day retention limits.

## Day 2+ (requires a classic cluster)

- `spark_ui_showcase.py` — partition sizes, shuffle, skew, spill, and OOM, each producing one artifact in the Spark UI. Requires a **classic cluster** (the Spark UI does not exist on serverless, and Free Edition cannot create classic compute), so in class this runs on the instructor's shared cluster. The final cell fails on purpose.
- `spark_ui_showcase_demo_script.md` — step-by-step Spark UI navigation for demoing the above.
