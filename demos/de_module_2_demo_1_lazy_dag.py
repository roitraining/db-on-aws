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
# MAGIC ### Step 4 — a bad column reference. **The cell fails immediately.**
# MAGIC Databricks analyzes eagerly now: name and type errors surface at the *transformation*,
# MAGIC complete with a did-you-mean list. (Verified on DBR 19.6, 2026-09-30 — older material
# MAGIC claimed this error waits for the action. It no longer does.)

# COMMAND ----------

try:
    chain.withColumn("ASSET_BAND", F.col("TOTAL_ASSETS") > 1000000)   # no such column
except Exception as e:
    print(str(e)[:300])   # UNRESOLVED_COLUMN — with suggestions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 — a *data* error. The transformation is accepted — and even `count()` passes.
# MAGIC Analysis can check names and types. It cannot check the data.

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("int")
def as_number(name):
    return int(name)          # legal names are not numbers — fails only when data flows through

broken = chain.withColumn("ID_NUM", as_number(F.col("NM_LGL")))
print("Transformation accepted. No error. Nothing has run.")

# COMMAND ----------

# count() does not need ID_NUM, so Spark never computes it — lazy means "only what the action needs":
print("count() succeeds:", broken.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 (instructor, classic cluster) — open the **Spark UI** and show the DAG for the
# MAGIC `count()` job: the plan Spark built from the lazy chain. Point at narrow vs wide — the
# MAGIC filter stayed inside a stage; a groupBy or join would open a new one.
# MAGIC
# MAGIC ### Step 7 — an action that READS the new column. *Now* it fails — naming the data value,
# MAGIC three cells after the mistake. *"The cell that fails is rarely the wrong cell. Read upward."*
# MAGIC **This cell fails on purpose — that is the end of the demo.**

# COMMAND ----------

broken.select("NM_LGL", "ID_NUM").show(5)
