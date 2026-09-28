# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1 Demo: Lazy Evaluation, Made Visible
# MAGIC
# MAGIC Run each cell **separately** — the timing is the demonstration. Serverless compute.
# MAGIC
# MAGIC **The last cell fails on purpose.** That is the lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1 — read a table
# MAGIC Before running, ask the room: *"is this about to read 61,000 rows?"* Let someone answer.

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("training_nic.migrated.institutions")

# COMMAND ----------

# MAGIC %md
# MAGIC Instant. Nothing was read — Spark built a *plan*, not a result.
# MAGIC
# MAGIC ### Cell 2 — filter and a calculated column

# COMMAND ----------

ca = (df.filter(F.col("STATE_ABBR_NM") == "CA")
        .withColumn("start_year", F.year("D_DT_START")))

# COMMAND ----------

# MAGIC %md
# MAGIC Still instant. Two more steps on the plan; still nothing has run.
# MAGIC
# MAGIC ### Cell 3 — the first action

# COMMAND ----------

ca.count()

# COMMAND ----------

# MAGIC %md
# MAGIC **That** pause was Spark actually working. Actions execute; transformations describe.
# MAGIC
# MAGIC ### Cell 4 — a typo in a filter (MN, not NM — this column does not exist)

# COMMAND ----------

bad = df.filter(F.col("STATE_ABBR_MN") == "CA")

# COMMAND ----------

# MAGIC %md
# MAGIC The cell **succeeded** — and it filtered on a column that does not exist. Let that sit.
# MAGIC
# MAGIC ### Cell 5 — the innocent cell that fails
# MAGIC The error names `STATE_ABBR_MN` — the bug lives in the cell above, which reported success.
# MAGIC When an action fails, read the **whole chain** above it, not the line that errored.
# MAGIC Close the loop: deferral is the *benefit* — Spark sees the whole plan before running it, so it can optimise the whole plan.

# COMMAND ----------

bad.count()
