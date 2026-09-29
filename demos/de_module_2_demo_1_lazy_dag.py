# Databricks notebook source
# MAGIC %md
# MAGIC # DE Module 2 · Demo 1: Lazy Evaluation and the DAG
# MAGIC
# MAGIC **Eight minutes.** Run cells one at a time — the *timing* is the demonstration.
# MAGIC **The last cell fails on purpose.** That failure is the lesson.
# MAGIC
# MAGIC **Compute:** run on the **classic cluster** (Demo 2 needs its Spark UI anyway). Everything
# MAGIC except the Spark UI step also works on serverless.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — read a table. Instant. *"Nothing has been read."*

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("training_nic.migrated.institutions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — a filter and a calculated column. Still instant. Still a plan.

# COMMAND ----------

chain = (df.filter(F.col("STATE_ABBR_NM") == "CA")
           .withColumn("NM_CLEAN", F.trim(F.col("NM_LGL"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — `count()`. Now the pause and the progress bar. *"First execution."*

# COMMAND ----------

chain.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 — a bad column reference. **The cell succeeds.**
# MAGIC Ask the room: is this a working pipeline?

# COMMAND ----------

broken = chain.withColumn("ASSET_BAND", F.col("TOTAL_ASSETS") > 1000000)  # no such column
print("Transformation accepted. No error. Nothing has run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 (instructor, classic cluster) — open the **Spark UI** and show the DAG for the
# MAGIC `count()` job: the plan Spark built from the lazy chain. Point at narrow vs wide — the
# MAGIC filter stayed inside a stage; a groupBy or join would open a new one.
# MAGIC
# MAGIC ### Step 5 — an action on the broken chain. *Now* it fails, naming the column.
# MAGIC *"The cell that fails is rarely the wrong cell. Read upward."*
# MAGIC **This cell fails on purpose — that is the end of the demo.**

# COMMAND ----------

broken.count()
