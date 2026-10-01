# Databricks notebook source
# MAGIC %md
# MAGIC # DE Module 4 · Demo 1: Auto Loader, Raw Then Declarative
# MAGIC
# MAGIC **Nine minutes.** *"Auto Loader is Structured Streaming. This is the honest version."*
# MAGIC The declarative streaming table is not magic — it is this checkpointed stream with the
# MAGIC boilerplate removed. Show raw first, declarative second.
# MAGIC
# MAGIC Self-contained: uses its own landing dir + checkpoint in a scratch volume, so the shared
# MAGIC `landing/branches` files (and Lab 10's expected row counts) are never disturbed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0 — a private landing directory with two of the NIC branch files

# COMMAND ----------

import os, shutil

spark.sql("CREATE SCHEMA IF NOT EXISTS training_nic.eng_demo")
spark.sql("CREATE VOLUME IF NOT EXISTS training_nic.eng_demo.scratch")

SRC = "/Volumes/training_nic/raw/landing/branches"
BASE = "/Volumes/training_nic/eng_demo/scratch/autoloader_demo"
LANDING = f"{BASE}/landing"
CHECKPOINT = f"{BASE}/checkpoint"
SCHEMA_LOC = f"{BASE}/schema"

# fresh start on each demo run — checkpoint AND target table together, or a rerun double-ingests
shutil.rmtree(BASE, ignore_errors=True)
os.makedirs(LANDING, exist_ok=True)
spark.sql("DROP TABLE IF EXISTS training_nic.eng_demo.branches_bronze_raw")

src_files = sorted(f for f in os.listdir(SRC) if f.endswith(".csv"))
for f in src_files[:2]:
    shutil.copyfile(f"{SRC}/{f}", f"{LANDING}/{f}")
print("landed:", sorted(os.listdir(LANDING)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — the raw Structured Streaming form
# MAGIC `cloudFiles.format` and `cloudFiles.schemaLocation` are **required** standalone;
# MAGIC `checkpointLocation` is what makes exactly-once true.

# COMMAND ----------

def run_autoloader_pass():
    stream = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("cloudFiles.schemaLocation", SCHEMA_LOC)
              .option("cloudFiles.inferColumnTypes", "true")       # typed schema: bad values become rescues, not silent strings
              .option("cloudFiles.schemaEvolutionMode", "rescue")  # unexpected columns rescue instead of failing the stream
              .option("header", "true")
              .load(LANDING))
    q = (stream.writeStream
         .option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True)
         .toTable("training_nic.eng_demo.branches_bronze_raw"))
    q.awaitTermination()

run_autoloader_pass()
n1 = spark.table("training_nic.eng_demo.branches_bronze_raw").count()
print(f"after first pass: {n1:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — land a new file. The stream picks up ONLY the new file.
# MAGIC *"New file, processed once. That is the checkpoint doing its job."*

# COMMAND ----------

new_file = src_files[2]
shutil.copyfile(f"{SRC}/{new_file}", f"{LANDING}/{new_file}")
print("new file landed:", new_file)

run_autoloader_pass()
n2 = spark.table("training_nic.eng_demo.branches_bronze_raw").count()
print(f"after second pass: {n2:,} rows  (+{n2-n1:,} — the new file only, nothing reprocessed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — the rescued data column. *"Nobody enabled this. Look at it anyway."*
# MAGIC On by default. **Expect ZERO rescued rows here** — the branch files parse cleanly, and
# MAGIC an empty net is the correct result on clean data. Say that out loud. Then break it.

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("training_nic.eng_demo.branches_bronze_raw")
print("columns include:", [c for c in df.columns if "rescued" in c.lower()])
print("rescued rows on clean data:", df.where(F.col("_rescued_data").isNotNull()).count())  # 0 — correct

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3b — now make the net catch something. Land a file carrying a bad date and a
# MAGIC column nobody agreed to. The row is **not dropped** — the unparseable pieces land in
# MAGIC `_rescued_data` as JSON, with the source file path as the receipt. (Verified 2026-10-01.)

# COMMAND ----------

with open(f"{LANDING}/{src_files[0]}") as fh:
    header_cols = fh.readline().strip().split(",")
bad = ["" for _ in header_cols]
bad[0] = "99999999"
bad[header_cols.index("NM_LGL")] = "RESCUE DEMO BRANCH"
bad[header_cols.index("D_DT_START")] = "NOT_A_DATE"
with open(f"{LANDING}/branches_zz_rescue_demo.csv", "w") as fh:
    fh.write(",".join(header_cols) + ",EXTRA_COL\n")
    fh.write(",".join(bad) + ",SURPRISE_EXTRA_VALUE\n")
print("malformed file landed")

run_autoloader_pass()
rescued = (spark.table("training_nic.eng_demo.branches_bronze_raw")
           .where(F.col("_rescued_data").isNotNull()))
print("rescued rows:", rescued.count())   # 1
display(rescued.select("#ID_RSSD", "NM_LGL", "D_DT_START", "_rescued_data"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 — now the declarative form. *"Same engine, less plumbing."*
# MAGIC
# MAGIC The same ingest as a **streaming table** in a Lakeflow pipeline — this is the Bronze table
# MAGIC from the Lab 10 pipeline (`bundles/solutions/lab-10-pipeline/src/medallion.py`):
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC @dp.table(name="branches_bronze")
# MAGIC def branches_bronze():
# MAGIC     return (spark.readStream
# MAGIC             .format("cloudFiles")
# MAGIC             .option("cloudFiles.format", "csv")
# MAGIC             .option("header", "true")
# MAGIC             .load("/Volumes/training_nic/raw/landing/branches"))
# MAGIC ```
# MAGIC
# MAGIC Spot the differences: **no schemaLocation, no checkpoint, no trigger, no writeStream** —
# MAGIC the pipeline manages all of it. That is what you traded direct control for.
# MAGIC → Demo 2 runs this pipeline and adds the quality gates.
