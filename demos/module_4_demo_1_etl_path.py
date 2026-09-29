# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4 · Demo 1: Building the ETL Path
# MAGIC
# MAGIC **Nine minutes.** Serverless is fine for this demo (classic cluster only matters for Demo 2's
# MAGIC Spark UI — if you plan to run that next, start the cluster *before* the session; cold start ~6 min).
# MAGIC
# MAGIC The arc: read → transform → aggregate → same thing in SQL → write it back → prove it's real.
# MAGIC Say the two reassurances out loud: *Python is not replacing SQL*, and *neither is faster — same engine*.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — read the table. Instant.
# MAGIC Remind the room: **nothing has been read yet.** This is a plan, not data.

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("training_nic.migrated.institutions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — add a filter and a calculated column. Still instant.

# COMMAND ----------

cleaned = (df
           .filter(F.col("STATE_ABBR_NM") == "CA")
           .withColumn("NM_LGL_CLEAN", F.trim(F.col("NM_LGL")))
           .withColumn("start_month", F.trunc(F.col("D_DT_START").cast("date"), "month")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — display it. *"That is the first execution."*

# COMMAND ----------

display(cleaned.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 — aggregate and display the summary

# COMMAND ----------

summary = (cleaned
           .groupBy("CHTR_TYPE_CD", "start_month")
           .agg(F.count("*").alias("institution_count"),
                F.countDistinct("CITY").alias("distinct_cities"))
           .orderBy("start_month", "CHTR_TYPE_CD"))

display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 — the same result in SQL. Identical output.
# MAGIC *"Same engine. Pick whichever is clearer for the step you are on."* This is the reassurance
# MAGIC the room needs — do not rush it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CHTR_TYPE_CD,
# MAGIC        TRUNC(CAST(D_DT_START AS DATE), 'MM') AS start_month,
# MAGIC        COUNT(*)              AS institution_count,
# MAGIC        COUNT(DISTINCT CITY)  AS distinct_cities
# MAGIC FROM training_nic.migrated.institutions
# MAGIC WHERE STATE_ABBR_NM = 'CA'
# MAGIC GROUP BY CHTR_TYPE_CD, TRUNC(CAST(D_DT_START AS DATE), 'MM')
# MAGIC ORDER BY start_month, CHTR_TYPE_CD;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 — write it out. Mention `mode("overwrite")` explicitly:
# MAGIC re-running **replaces** rather than duplicates — that is what makes the notebook repeatable.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS training_nic.analyst")

(summary.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("training_nic.analyst.demo_institution_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 — prove it is a real, shareable table. This is the bridge to Module 5.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM training_nic.analyst.demo_institution_summary
# MAGIC ORDER BY start_month DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC **Transition:** *"That ran in a few seconds. When it takes five minutes instead, you need to
# MAGIC know where to look."* → Demo 2.
