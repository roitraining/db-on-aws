# Lab 10_ALT: The Medallion Pipeline — No-Pipelines Edition

**Course:** Databricks on AWS: Advanced Data Engineering
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

Same lab, different machinery. When pipeline compute is not available, every guarantee Lab 10 builds — incremental ingestion, quality gating, layered cleaning, slowly changing dimensions — can be built from the primitives underneath: raw Auto Loader streaming, `CREATE TABLE AS`, and `MERGE`. You will build the whole Medallion flow in **one runnable notebook**, see exactly what the declarative framework was doing for you, and end with an artifact Lab 11 can orchestrate directly. Every step here runs on **serverless notebook compute, including Free Edition** (verified 2026-10-01).

---

## Prerequisites

- [ ] Labs 7–9 completed — you have `eng_<id>` and its `work` schema
- [ ] Serverless notebook compute — **no pipeline compute, no classic cluster needed**
- [ ] `training_nic.raw.landing` volume with a `branches/` subdirectory
- [ ] Intro Lab 4 completed (DataFrame basics assumed)

---

## Objectives

- Seed a private landing directory and ingest it with raw Auto Loader and a checkpoint
- Implement the three expectation actions by hand: **drop** (Silver), **capture** (Quarantine), **warn** (a quality log)
- Build Gold as a plain table
- Build an SCD Type 2 table with an explicit `MERGE` — the machinery `AUTO CDC` automates
- Land a new file and prove Bronze is incremental while the downstream layers recompute
- Record quality metrics to a table an orchestrator can read
- Publish Gold-only access

---

## Part 1: One Notebook, Your Own Landing

### Task 1: Set Up

1. **Create the notebook**

    In the left sidebar click **Workspace**, navigate to **Users → your.email**, click
    **Create → Notebook**. Name it `alt_medallion`, set the default language to **Python**,
    and attach **serverless** compute using the compute selector at the top right.

2. **Configure and seed a private landing directory**

    Everything lands in a volume **you own**, so you control exactly when files arrive.
    Paste into the first cell and run it:

    ```python
    import os, shutil

    CATALOG = "eng_<id>"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.alt")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.alt.scratch")

    SRC = "/Volumes/training_nic/raw/landing/branches"
    BASE = f"/Volumes/{CATALOG}/alt/scratch/lab10_alt"
    LANDING = f"{BASE}/landing"
    CHECKPOINT = f"{BASE}/checkpoint"
    SCHEMA_LOC = f"{BASE}/schema"

    os.makedirs(LANDING, exist_ok=True)
    src_files = sorted(f for f in os.listdir(SRC) if f.endswith(".csv"))
    if not os.listdir(LANDING):                      # seed only once
        for f in src_files[:2]:
            shutil.copyfile(f"{SRC}/{f}", f"{LANDING}/{f}")
    print("landing holds:", sorted(os.listdir(LANDING)))
    ```
    <!-- source: facts_extracted.md §4 -->

    > **Expected Result:** Two CSV files listed. The `if not os.listdir` guard means
    > re-running this cell never re-seeds — the notebook stays safe to **Run all**.

---

## Part 2: Bronze — Raw Auto Loader

### Task 2: Incremental Ingest with a Checkpoint

3. **Write the Bronze pass**

    This is Lab 10's Task 2, unchanged — Auto Loader is Structured Streaming and needs no
    pipeline. Next cell:

    ```python
    def bronze_pass():
        stream = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "csv")
                  .option("cloudFiles.schemaLocation", SCHEMA_LOC)
                  .option("header", "true")
                  .load(LANDING))
        (stream.writeStream
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .toTable(f"{CATALOG}.alt.branches_bronze")
            .awaitTermination())

    bronze_pass()
    print("bronze:", spark.table(f"{CATALOG}.alt.branches_bronze").count())
    ```
    <!-- source: facts_extracted.md §4 -->

    > **Expected Result:** **50,000** rows — the two seeded files. (In the shared class
    > workspace the first two files are the small ones: **1,000**.) Write your number down.

4. **Run the cell a second time**

    > **Expected Result:** The count does not move. The checkpoint recorded both files;
    > exactly-once is yours without a pipeline, because it never came from the pipeline —
    > it comes from the checkpoint.
    <!-- source: facts_extracted.md §4 -->

---

## Part 3: The Three Expectation Actions, By Hand

### Task 3: Drop, Capture, Warn

5. **Declare the rules once**

    In Lab 10 these were `@dp.expect` decorators. Here they are one dict — and the whole
    point of this part is that each *action* is just a query shape:

    ```python
    RULES = {
        "valid_key": "ID_RSSD IS NOT NULL",                    # action: DROP
        "plausible_city": "CITY IS NULL OR LENGTH(CITY) > 1",  # action: WARN
    }
    CONFORMED = f"""
        SELECT `#ID_RSSD` AS ID_RSSD,
               TRIM(NM_LGL) AS NM_LGL,
               NULLIF(CITY, '') AS CITY,
               * EXCEPT (`#ID_RSSD`, NM_LGL, CITY)
        FROM {CATALOG}.alt.branches_bronze"""
    ```
    <!-- source: facts_extracted.md §5 -->

6. **Silver — the DROP action is a WHERE clause**

    ```python
    spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.alt.branches_silver AS
                  SELECT * FROM ({CONFORMED}) WHERE {RULES['valid_key']}""")
    print("silver:", spark.table(f"{CATALOG}.alt.branches_silver").count())
    ```
    <!-- source: facts_extracted.md §5 -->

7. **Quarantine — the CAPTURE side, same predicates inverted**

    ```python
    quarantine_pred = " OR ".join(f"NOT({p})" for p in RULES.values())
    spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.alt.branches_quarantine AS
                  SELECT * FROM ({CONFORMED}) WHERE {quarantine_pred}""")
    print("quarantine:", spark.table(f"{CATALOG}.alt.branches_quarantine").count())
    ```
    <!-- source: facts_extracted.md §5 -->

8. **Quality log — the WARN action is a count you keep**

    A pipeline records these numbers in its event log. You record them in a table — which
    is *better* for Lab 11, because any job task can read a table:

    ```python
    bronze_n = spark.table(f"{CATALOG}.alt.branches_bronze").count()
    silver_n = spark.table(f"{CATALOG}.alt.branches_silver").count()
    warned_n = spark.sql(f"""SELECT COUNT(*) c FROM ({CONFORMED})
                             WHERE {RULES['valid_key']}
                               AND NOT ({RULES['plausible_city']})""").first()["c"]
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.alt.quality_log
                  (run_at TIMESTAMP, rule STRING, action STRING, violations BIGINT)""")
    spark.sql(f"""INSERT INTO {CATALOG}.alt.quality_log VALUES
                  (current_timestamp(), 'valid_key', 'drop', {bronze_n - silver_n}),
                  (current_timestamp(), 'plausible_city', 'warn', {warned_n})""")
    display(spark.sql(f"SELECT * FROM {CATALOG}.alt.quality_log ORDER BY run_at DESC"))
    ```
    <!-- source: facts_extracted.md §5 -->

    > **Expected Result:** Both rules log **0** violations — the seeded files are clean, and
    > zero is the correct reading on clean data, exactly like the empty Data quality panel
    > would be. Part 4 makes these numbers move.

9. **Gold — a plain table, on purpose**

    ```python
    spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.alt.branch_summary_gold AS
                  SELECT STATE_ABBR_NM, COUNT(*) AS branch_count
                  FROM {CATALOG}.alt.branches_silver GROUP BY STATE_ABBR_NM""")
    print("gold rows:", spark.table(f"{CATALOG}.alt.branch_summary_gold").count())
    ```
    <!-- source: facts_extracted.md §5 -->

    > **Key Insight:** Lab 10's Gold was a materialized view only its pipeline could refresh.
    > A plain table rebuilt by `CREATE OR REPLACE` has no such owner — any job task can
    > rebuild it, which Lab 11 will exploit.

---

## Part 4: Prove It Is Incremental

### Task 4: Land a File, Rerun the Build

10. **Write a new arrival with planted violations**

    You own the landing directory, so make the data misbehave on purpose — 5 clean rows,
    3 with no key, 2 with a one-character city:

    ```python
    with open(f"{LANDING}/{src_files[0]}") as fh:
        header_cols = fh.readline().strip().split(",")
    def mkrow(key, name, city, state):
        r = ["" for _ in header_cols]
        r[0] = key
        r[header_cols.index("NM_LGL")] = name
        r[header_cols.index("CITY")] = city
        r[header_cols.index("STATE_ABBR_NM")] = state
        return ",".join(r)
    rows  = [mkrow(str(97000000+i), f"ALT DAY BRANCH {i}", "SPRINGFIELD", "IL") for i in range(1, 6)]
    rows += [mkrow("", f"ALT BAD BRANCH {i} (NO KEY)", "SPRINGFIELD", "IL") for i in range(1, 4)]
    rows += [mkrow(str(97900000+i), f"ALT BAD BRANCH {i} (SHORT CITY)", "X", "IL") for i in range(1, 3)]
    with open(f"{LANDING}/branches_zz_alt_day.csv", "w") as fh:
        fh.write(",".join(header_cols) + "\n")
        fh.write("\n".join(rows) + "\n")
    print("landed: 10 rows (5 clean, 3 no-key, 2 short-city)")
    ```
    <!-- source: facts_extracted.md §9 -->

11. **Re-run the whole notebook**

    Click **Run all**. The seed cell skips (guarded), Bronze ingests only the new file,
    the layers rebuild.

    > **Expected Result:** Bronze **+10** (one new file), Silver **+7** (the 3 no-key rows
    > dropped), Quarantine **5**, and the quality log's newest rows read
    > `valid_key | drop | 3` and `plausible_city | warn | 2`. Gold's `IL` count rose by 7.

    > **What Just Happened?** Bronze moved by exactly the new file because the checkpoint is
    > doing what the pipeline did in Lab 10. Silver, Quarantine, and Gold recomputed in full
    > — that is the honest trade of this edition: you keep incremental *ingestion* for free,
    > and you pay with full *recompute* downstream. The declarative framework's real product
    > is making that second part incremental too.
    <!-- source: facts_extracted.md §5 -->

---

## Part 5: SCD Type 2 With Your Own Hands

### Task 5: The MERGE that AUTO CDC Writes For You

12. **Initial load with validity columns**

    ```python
    spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.alt.institutions_scd AS
                  SELECT *, current_timestamp() AS __START_AT,
                         CAST(NULL AS TIMESTAMP) AS __END_AT
                  FROM training_nic.legacy_onprem.institutions""")
    print("scd:", spark.table(f"{CATALOG}.alt.institutions_scd").count())
    ```
    <!-- source: facts_extracted.md §6 -->

    > **Expected Result:** **62,080** rows, every one current (`__END_AT` NULL). The two
    > dunder columns are the same ones `AUTO CDC ... STORED AS SCD TYPE 2` maintains.

13. **Make a changed snapshot**

    The SQL Server stand-in has no change feed, so change detection is snapshot comparison —
    simulate the next day's snapshot with three renamed institutions:

    ```python
    spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.alt.snapshot_v2 AS
                  SELECT * FROM training_nic.legacy_onprem.institutions""")
    keys = [r[0] for r in spark.sql(f"""SELECT `#ID_RSSD` FROM {CATALOG}.alt.snapshot_v2
                                        WHERE `#ID_RSSD` IS NOT NULL
                                        ORDER BY `#ID_RSSD` LIMIT 3""").collect()]
    key_list = ",".join(str(k) for k in keys)
    spark.sql(f"""UPDATE {CATALOG}.alt.snapshot_v2
                  SET NM_LGL = CONCAT(TRIM(NM_LGL), ' (RENAMED)')
                  WHERE `#ID_RSSD` IN ({key_list})""")
    ```
    <!-- source: facts_extracted.md §6 -->

14. **Close the changed rows, insert their new versions**

    ```python
    spark.sql(f"""MERGE INTO {CATALOG}.alt.institutions_scd t
                  USING {CATALOG}.alt.snapshot_v2 s
                  ON t.`#ID_RSSD` = s.`#ID_RSSD`
                     AND t.__END_AT IS NULL AND t.NM_LGL <> s.NM_LGL
                  WHEN MATCHED THEN UPDATE SET __END_AT = current_timestamp()""")
    spark.sql(f"""INSERT INTO {CATALOG}.alt.institutions_scd
                  SELECT s.*, current_timestamp(), CAST(NULL AS TIMESTAMP)
                  FROM {CATALOG}.alt.snapshot_v2 s
                  LEFT JOIN (SELECT `#ID_RSSD` FROM {CATALOG}.alt.institutions_scd
                             WHERE __END_AT IS NULL) t
                    ON t.`#ID_RSSD` = s.`#ID_RSSD`
                  WHERE t.`#ID_RSSD` IS NULL""")
    display(spark.sql(f"""SELECT `#ID_RSSD`, NM_LGL, __START_AT, __END_AT
                          FROM {CATALOG}.alt.institutions_scd
                          WHERE `#ID_RSSD` IN ({key_list})
                          ORDER BY `#ID_RSSD`, __START_AT"""))
    ```
    <!-- source: facts_extracted.md §6 -->

    > **Expected Result:** **62,083** total rows; each of the three keys appears **twice** —
    > the old name with `__END_AT` stamped, the `(RENAMED)` version current. History kept,
    > current state queryable with `WHERE __END_AT IS NULL`.

    > **Key Insight:** Count what you just hand-wrote: a join condition, a change predicate,
    > a close, an anti-join insert — and you only handled *renames*. `AUTO CDC FROM SNAPSHOT`
    > is one function call that handles all columns, deletes, and ordering. The point of
    > doing it manually once is knowing precisely what you are delegating.
    <!-- source: facts_extracted.md §6 -->

---

## Part 6: Publish Gold Only

### Task 6: Same Governance, Any Compute

15. **Grant read access to Gold and nothing else**

    In the **SQL editor** (or a `%sql` cell):

    ```sql
    GRANT USE CATALOG ON CATALOG eng_<id> TO `account users`;
    GRANT USE SCHEMA  ON SCHEMA  eng_<id>.alt TO `account users`;
    GRANT SELECT      ON TABLE   eng_<id>.alt.branch_summary_gold TO `account users`;
    SHOW GRANTS ON TABLE eng_<id>.alt.branches_bronze;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Expected Result:** Bronze shows no reader grant. Consumers see conformed,
    > quality-gated Gold and never the raw landing — the governance story is identical to
    > Lab 10's, because Unity Catalog never cared how the tables were built.

---

## Hand-Off to Lab 11

Lab 11 proceeds **as written**, with exactly two substitutions:

| Lab 11 says | You do instead |
|---|---|
| Task type **Pipeline**, pointed at your Lab 10 pipeline | Task type **Notebook**, pointed at your **`alt_medallion`** notebook — a **Run all** of it is one complete, idempotent build |
| Read the violation count from `event_log(TABLE(...))` | Read it from your quality log: `SELECT violations FROM eng_<id>.alt.quality_log WHERE rule = 'valid_key' ORDER BY run_at DESC LIMIT 1` |

Everything else — Task Values, the If/else gate, branch routing, notifications,
repair-and-rerun, Run As — is a Jobs feature and needs no pipeline. One bonus: the Gold
promotion task can simply rebuild the plain Gold table, because no pipeline owns it.

---

## Stretch Task

1. Make Silver incremental too: convert step 6 into a `MERGE` keyed on `ID_RSSD` and measure what fraction of the rebuild you saved.
2. Extend the SCD `MERGE` to also detect changed `CITY`, not just `NM_LGL`. How does the change predicate scale with column count — and what does that tell you about `AUTO CDC`'s value?
3. Add a `deleted` branch: remove a row from `snapshot_v2` and close its SCD record without inserting a successor.

---

## Checkpoint: Verify Your Progress

- [ ] I seeded a private landing directory I own
- [ ] I ingested with raw Auto Loader and an explicit checkpoint
- [ ] A second pass ingested nothing — and I can say why
- [ ] I implemented DROP as Silver's WHERE clause
- [ ] I implemented CAPTURE as Quarantine's inverted predicate
- [ ] I implemented WARN as rows in my quality log
- [ ] My first quality-log entries were zeros, and I can explain why that is correct
- [ ] I landed a 10-row file and Bronze moved by exactly 10
- [ ] Silver moved by 7, Quarantine holds 5, and the log reads drop 3 / warn 2
- [ ] I built the SCD table and closed 3 rows with my own MERGE
- [ ] Each changed key shows two versions, one closed and one current
- [ ] Readers hold SELECT on Gold and nothing on Bronze

---

## Troubleshooting Reference

> **Key Insight:** With no pipeline in the picture, there are only three moving parts:
> the checkpoint (ingestion), your SQL (transformation), and the catalog (access).

| Issue | Symptom | Solution |
|---|---|---|
| Rerun duplicates Bronze rows | Count doubles | Checkpoint path changed or was deleted; the table and checkpoint must live and die together. |
| Seed cell re-copies files | Landing grows on rerun | The `if not os.listdir(LANDING)` guard is missing. |
| `PARSE_SYNTAX_ERROR` near `#` | Error naming `#ID_RSSD` | Backtick it in SQL; in the conformed view it is already renamed to `ID_RSSD`. |
| Quality log counts never change | Same numbers each run | You re-ran only the log cell. The counts are computed from the tables — **Run all**. |
| SCD MERGE closes 0 rows | `closed = 0` | `snapshot_v2` was not updated, or you re-ran step 14 after the versions already matched. Re-run from step 12. |
| SCD table grows every full rerun | More than +3 | Expected — step 12 is `CREATE OR REPLACE`, so a full rerun resets it first. If you skipped step 12, duplicate current rows appear. |
| Volume write denied | Permission error on `/Volumes/eng_<id>/...` | Create the volume in **your** catalog (step 2) — you own `eng_<id>`; you do not own `training_nic`. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Serverless notebook | Billed while attached | Detach when finished. |
| Full downstream recompute | Every Run all rebuilds Silver/Quarantine/Gold | At training scale, pennies; at production scale, the argument for pipelines. |
| Table copies | SCD + snapshot are two full copies of the source | Drop `snapshot_v2` at course end. |

**Cleanup:** Keep everything in `eng_<id>.alt` — Lab 11 orchestrates this notebook and reads the quality log.

---

## Knowledge Check

1. Where does exactly-once ingestion actually come from, and what did the pipeline add on top of it?
2. Express each of the three expectation actions as a query shape.
3. Your first quality-log entries were zeros. Correct or a bug — and how do you know?
4. Bronze moved by 10 but Silver was rebuilt in full. What did the declarative framework give you that this edition does not?
5. Why is Gold a plain table here, and what does that unlock for Lab 11?
6. Name the four pieces of SCD-2 logic you hand-wrote that `AUTO CDC FROM SNAPSHOT` generates.
7. A colleague says "no pipelines means no governance." Use Part 6 to answer them.

Answers are held in the Knowledge Check Bank.

---

## Next Steps

Lab 11, as written, with the two substitutions from the Hand-Off table: your `alt_medallion`
notebook becomes the job's first task, and your quality log becomes what the gate reads.

---

## Resources

- Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/
- Delta Lake MERGE / table history: https://docs.databricks.com/aws/en/delta/history
- Unity Catalog privileges: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
- AUTO CDC APIs (what Part 5 automates): https://docs.databricks.com/aws/en/ldp/cdc

---

*Lab 10_ALT Complete*
