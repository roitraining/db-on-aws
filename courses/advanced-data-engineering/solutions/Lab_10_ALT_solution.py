# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 10_ALT — Solution Notebook
# MAGIC
# MAGIC **The complete medallion build, no pipelines.** Set the `catalog` widget to **your**
# MAGIC catalog (`eng_<id>` from Lab 7), attach **serverless** compute, then **Run all**.
# MAGIC Safe to re-run end to end — the seed cell is guarded and Bronze is checkpointed.
# MAGIC
# MAGIC Live-verified on Free Edition and the class workspace, 2026-10-01.
# MAGIC Guide: `courses/advanced-data-engineering/labs/Lab_10_ALT_Guide.md`

# COMMAND ----------

# MAGIC %md ## Part 1 — Config and a private landing directory you own

# COMMAND ----------

import os, shutil

dbutils.widgets.text("catalog", "eng_<id>", "Your catalog from Lab 7")
CATALOG = dbutils.widgets.get("catalog")
assert CATALOG != "eng_<id>", "Set the catalog widget (top of the notebook) to YOUR eng_<id> catalog first"

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

# COMMAND ----------

# MAGIC %md ## Part 2 — Bronze: raw Auto Loader with a checkpoint
# MAGIC Run this cell twice: the second pass ingests nothing. Exactly-once comes from the
# MAGIC checkpoint, not from a pipeline.

# COMMAND ----------

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
# Expected: 50,000 on an attendee account (two seeded part files); 1,000 in the class workspace

# COMMAND ----------

# MAGIC %md ## Part 3 — The three expectation actions, by hand
# MAGIC DROP is Silver's WHERE · CAPTURE is Quarantine's inverted predicate · WARN is a logged count.

# COMMAND ----------

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

# Silver — rows passing the DROP rule
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.alt.branches_silver AS
              SELECT * FROM ({CONFORMED}) WHERE {RULES['valid_key']}""")

# Quarantine — rows violating ANY rule
quarantine_pred = " OR ".join(f"NOT({p})" for p in RULES.values())
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.alt.branches_quarantine AS
              SELECT * FROM ({CONFORMED}) WHERE {quarantine_pred}""")

print("silver:", spark.table(f"{CATALOG}.alt.branches_silver").count())
print("quarantine:", spark.table(f"{CATALOG}.alt.branches_quarantine").count())

# COMMAND ----------

# Quality log — what a pipeline would write to its event log, in a table any job task can read
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
display(spark.sql(f"SELECT * FROM {CATALOG}.alt.quality_log ORDER BY run_at DESC LIMIT 6"))
# First run on clean data: both rules log 0 — and 0 is the CORRECT reading on clean data.

# COMMAND ----------

# Gold — a plain table on purpose: no pipeline owns it, so any job task can rebuild it (Lab 11)
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.alt.branch_summary_gold AS
              SELECT STATE_ABBR_NM, COUNT(*) AS branch_count
              FROM {CATALOG}.alt.branches_silver GROUP BY STATE_ABBR_NM""")
print("gold rows:", spark.table(f"{CATALOG}.alt.branch_summary_gold").count())

# COMMAND ----------

# MAGIC %md ## Part 4 — Prove it is incremental: land a bad file, Run all again
# MAGIC After running this cell once, **Run all**. Expected deltas: Bronze **+10**,
# MAGIC Silver **+7**, Quarantine **5**, newest log rows `drop 3` / `warn 2`.
# MAGIC (Re-running this cell later is harmless — Auto Loader tracks the file by name.)

# COMMAND ----------

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
print("landed: 10 rows (5 clean, 3 no-key, 2 short-city) — now Run all")

# COMMAND ----------

# MAGIC %md ## Part 5 — SCD Type 2 with an explicit MERGE (what AUTO CDC automates)

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.alt.institutions_scd AS
              SELECT *, current_timestamp() AS __START_AT,
                     CAST(NULL AS TIMESTAMP) AS __END_AT
              FROM training_nic.legacy_onprem.institutions""")
print("scd initial:", spark.table(f"{CATALOG}.alt.institutions_scd").count())  # 62,080

# COMMAND ----------

# A changed "next-day" snapshot: three institutions renamed
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.alt.snapshot_v2 AS
              SELECT * FROM training_nic.legacy_onprem.institutions""")
keys = [r[0] for r in spark.sql(f"""SELECT `#ID_RSSD` FROM {CATALOG}.alt.snapshot_v2
                                    WHERE `#ID_RSSD` IS NOT NULL
                                    ORDER BY `#ID_RSSD` LIMIT 3""").collect()]
key_list = ",".join(str(k) for k in keys)
spark.sql(f"""UPDATE {CATALOG}.alt.snapshot_v2
              SET NM_LGL = CONCAT(TRIM(NM_LGL), ' (RENAMED)')
              WHERE `#ID_RSSD` IN ({key_list})""")
print("renamed keys:", key_list)

# COMMAND ----------

# Close the changed rows, insert their new versions
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
# Expected: 62,083 total; each key twice — old name closed, (RENAMED) current

# COMMAND ----------

# MAGIC %md ## Part 6 — Publish Gold only (edit `eng_<id>` before running, or skip and use the SQL editor)

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.alt TO `account users`")
spark.sql(f"GRANT SELECT ON TABLE {CATALOG}.alt.branch_summary_gold TO `account users`")
display(spark.sql(f"SHOW GRANTS ON TABLE {CATALOG}.alt.branches_bronze"))
# Expected: Bronze shows no reader grant — consumers see Gold only

# COMMAND ----------

# MAGIC %md ## Hand-off to Lab 11
# MAGIC Run Lab 11 **as written**, with two substitutions:
# MAGIC 1. Job Task 1: type **Notebook**, pointed at **this notebook** (a Run all = one full build).
# MAGIC 2. The quality read:
# MAGIC    ```sql
# MAGIC    SELECT violations FROM <catalog>.alt.quality_log
# MAGIC    WHERE rule = 'valid_key' ORDER BY run_at DESC LIMIT 1
# MAGIC    ```
# MAGIC    emitted as the `dropped_records` task value. Everything else — the If/else gate,
# MAGIC    branches, notifications, repair-and-rerun, Run As — is a Jobs feature and runs unchanged.
