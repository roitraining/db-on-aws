# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4 · Demo 2: Why Was That Slow?
# MAGIC
# MAGIC **Seven minutes. Classic cluster required** — the Spark UI does not exist on serverless.
# MAGIC Start the cluster before the session (~6 min cold start).
# MAGIC
# MAGIC Goal is ONE sentence an engineer can act on — *"it shuffled N rows on the join"* — not tuning.
# MAGIC Do **not** go deeper; Day 3 covers tuning properly for engineers.
# MAGIC
# MAGIC For the fuller five-artifact walk (partitions, shuffle, skew, spill, OOM), use
# MAGIC [`spark_ui_showcase.py`](spark_ui_showcase.py) with [`spark_ui_follow_along.md`](spark_ui_follow_along.md).
# MAGIC This notebook is the short in-module version.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — run an aggregation that takes a visible few seconds
# MAGIC The 2M-row perf tables built by setup make slow *visible*.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT i.CHTR_TYPE_CD,
# MAGIC        COUNT(*)          AS institution_count,
# MAGIC        SUM(f.TOT_ASSETS) AS total_assets
# MAGIC FROM training_nic.perf.institutions_large AS i
# MAGIC JOIN training_nic.perf.financials_large  AS f
# MAGIC   ON i.`#ID_RSSD` = f.`#ID_RSSD`
# MAGIC GROUP BY i.CHTR_TYPE_CD;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps 2–5 — walk the Spark UI (on screen)
# MAGIC
# MAGIC 1. Open the **Spark UI** from the cluster (Compute → your cluster → Spark UI). Find the job you just ran.
# MAGIC 2. **Stages tab** — count the stages. *"Each boundary is a shuffle. Your data crossed the
# MAGIC    cluster that many times."*
# MAGIC 3. Open the largest stage — show **Shuffle Read** and **Shuffle Write**.
# MAGIC 4. Show the **task count** for that stage — that is the partition count.
# MAGIC
# MAGIC Narrow vs wide, once, concretely: `filter` and `withColumn` stay inside a stage;
# MAGIC `groupBy` and `join` force a shuffle.
# MAGIC
# MAGIC > Panel labels shift between runtime versions — navigate by what the numbers *mean*,
# MAGIC > not a memorised layout.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contrast — a narrow query, no shuffle
# MAGIC Run it, find it in the UI: one stage, no shuffle columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `#ID_RSSD`, STATE_ABBR_NM
# MAGIC FROM training_nic.perf.institutions_large
# MAGIC WHERE STATE_ABBR_NM = 'WA'
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 — frame the takeaway
# MAGIC *"You are not tuning this. You are producing one sentence an engineer can act on."*
# MAGIC
# MAGIC **Transition:** *"You can now prepare data, save it, and say something useful about why it
# MAGIC was slow. The remaining question is how anyone else gets to it."* → Module 5.
