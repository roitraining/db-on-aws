# Databricks notebook source
# MAGIC %md
# MAGIC # DE Module 4 · Demo 2: Expectations and AUTO CDC
# MAGIC
# MAGIC **Nine minutes.** *"Three actions, one syntax. Warn counts, drop removes, fail stops."*
# MAGIC
# MAGIC **The live artifact is the Lab 10 solution pipeline** — deploy it before class:
# MAGIC ```
# MAGIC cd bundles/solutions/lab-10-pipeline
# MAGIC databricks bundle deploy -t dev && databricks bundle run medallion_pipeline
# MAGIC ```
# MAGIC It writes `training_nic.lab10_solution.*`. This notebook is the presenter's script plus the
# MAGIC verification queries to run against the finished update.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — Warn: invalid rows written, and counted
# MAGIC In the pipeline source (`src/medallion.py`, Silver):
# MAGIC ```python
# MAGIC @dp.expect("city_present", "CITY IS NOT NULL")          # WARN — write, but count
# MAGIC @dp.expect_or_drop("valid_key", "ID_RSSD IS NOT NULL")  # DROP — remove before write
# MAGIC ```
# MAGIC Open the pipeline UI → the Silver table's **Data quality** panel shows the per-expectation
# MAGIC counts. That count is the number Module 5's quality gate will read.

# COMMAND ----------

# The observable result: rows that WARN let through (city missing but present in Silver)
from pyspark.sql import functions as F

silver = spark.table("training_nic.lab10_solution.branches_silver")
print("silver rows:", silver.count())
print("rows with CITY null (warn let them through, counted):",
      silver.where(F.col("CITY").isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — Drop: `valid_key` removes rows before the write
# MAGIC Compare Bronze in vs Silver out — the difference is what `expect_or_drop` removed.
# MAGIC (On the staged data `ID_RSSD IS NOT NULL` drops nothing — the four bad keys are non-numeric,
# MAGIC not NULL. **That is the teaching moment:** the predicate and the action are separate choices,
# MAGIC and a badly chosen predicate silently protects nothing.)

# COMMAND ----------

bronze_n = spark.table("training_nic.lab10_solution.branches_bronze").count()
silver_n = silver.count()
print(f"bronze: {bronze_n:,}   silver: {silver_n:,}   dropped: {bronze_n - silver_n:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — Fail: change the expectation and let the pipeline halt (live edit)
# MAGIC In `medallion.py`, swap onto Silver:
# MAGIC ```python
# MAGIC @dp.expect_or_fail("key_is_numeric", "ID_RSSD RLIKE '^[0-9]+$'")
# MAGIC ```
# MAGIC Re-run the update — it **stops** on the four non-numeric keys (`X499`, `X998`, …).
# MAGIC Show the failed update in the pipeline UI, then revert. *"Warn counts, drop removes,
# MAGIC fail stops."*

# COMMAND ----------

# The four rows that would stop a Fail expectation:
display(spark.table("training_nic.lab10_solution.branches_silver")
        .where(~F.col("ID_RSSD").rlike("^[0-9]+$")).select("ID_RSSD", "NM_LGL", "CITY"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 — AUTO CDC FROM SNAPSHOT: SCD Type 2 without a change feed
# MAGIC The source is a SQL Server stand-in that never had CDC enabled — snapshot comparison is the
# MAGIC migration-honest variant, and it is **Python-only**. Legacy name: `apply_changes_from_snapshot()`.
# MAGIC ```python
# MAGIC dp.create_streaming_table("institutions_scd")
# MAGIC dp.create_auto_cdc_from_snapshot_flow(
# MAGIC     target="institutions_scd",
# MAGIC     source="training_nic.legacy_onprem.institutions",
# MAGIC     keys=["#ID_RSSD"],
# MAGIC     stored_as_scd_type=2)
# MAGIC ```

# COMMAND ----------

# The SCD2 evidence: __START_AT / __END_AT columns Delta maintains for you
scd = spark.table("training_nic.lab10_solution.institutions_scd")
print("SCD rows:", scd.count())
print("SCD columns:", [c for c in scd.columns if c.startswith("__")])
display(scd.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 — say the edition constraint out loud
# MAGIC **AUTO CDC needs serverless Lakeflow pipelines or Pro/Advanced edition** — it is not
# MAGIC supported on plain Apache Spark Declarative Pipelines. On the wrong edition the failure is a
# MAGIC missing-feature error at update time, not a syntax error.
# MAGIC
# MAGIC **Transition:** *"The pipeline cleans and gates. Next it needs to run on a schedule, under an
# MAGIC identity that is not a person, and refuse to publish bad data."*
