# Databricks notebook source
# MAGIC %md
# MAGIC # DE Module 5 · Demo 1: Task Values and the Quality Gate
# MAGIC
# MAGIC **Nine minutes.** *"The pipeline refuses to publish bad data. That is the expert tier."*
# MAGIC
# MAGIC **The live artifact is the Lab 11 solution job** — deploy it before class:
# MAGIC ```
# MAGIC cd bundles/solutions/lab-11-job
# MAGIC databricks bundle deploy -t dev && databricks bundle run medallion_job
# MAGIC ```
# MAGIC This notebook is the presenter script; the job's own graph on screen is the demo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — the task value, set from Python only
# MAGIC Open the job's `read_event_log` task and show the line:
# MAGIC ```python
# MAGIC dbutils.jobs.taskValues.set(key="dropped_records", value=int(dropped))
# MAGIC ```
# MAGIC *"A SQL task could not do this — task values are set from Python only."* A SQL rewrite still
# MAGIC deploys and still runs; the gate just silently never sees a value. (Cap: 48 KiB per value.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — the If/else task reading it
# MAGIC In the job graph, open `quality_gate`:
# MAGIC ```
# MAGIC {{tasks.read_event_log.values.dropped_records}}  LESS_THAN_OR_EQUAL  100
# MAGIC ```
# MAGIC **Keep both operands numbers** — the notebook casts to `int` because a string operand makes
# MAGIC the comparison lexicographic ("9" > "100"), and the gate takes the wrong branch for no
# MAGIC visible reason. (The outline says "Condition Task"; the product now says "If/else task" —
# MAGIC use both names once.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — branch routing
# MAGIC `promote_gold` depends on the gate with **outcome: "true"**; `halt_and_alert` on
# MAGIC **outcome: "false"**. Omit the outcome and a task depends on the gate *running*, not on
# MAGIC which way it went. The untaken branch reports **EXCLUDED**, not failed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 — run it both ways
# MAGIC 1. **Clean run:** `databricks bundle run medallion_job` → gate true → `promote_gold` runs,
# MAGIC    `halt_and_alert` EXCLUDED. Show the Gold table timestamp.
# MAGIC 2. **Inject violations:** re-deploy with the threshold below the observed count —
# MAGIC    `databricks bundle deploy -t dev --var="quality_threshold=0"` (any drop now trips it)
# MAGIC    — re-run → gate false → Gold is held, `halt_and_alert` runs instead.
# MAGIC
# MAGIC *"The pipeline refuses to publish bad data."* Then restore the threshold.

# COMMAND ----------

# Evidence for the room: what the gate actually read on the last run.
# RUN THIS ON A SQL WAREHOUSE (or a Shared-mode cluster) — an Assigned (single-user)
# cluster raises EVENT_LOG_REQUIRES_SHARED_COMPUTE. Verified 2026-09-30: reads 0 on
# the current solution deployment (expect_or_drop drops nothing on the staged data).
from pyspark.sql import functions as F
df = spark.sql("""
    SELECT SUM(CAST(details:flow_progress.data_quality.dropped_records AS BIGINT)) AS dropped_records
    FROM event_log(TABLE(training_nic.lab10_solution.branches_silver))
    WHERE event_type = 'flow_progress'
""")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transition:** *"The gate needs its number from somewhere. That somewhere is the pipeline's
# MAGIC own event log — and reading it is where identity bites."* → Demo 2.
