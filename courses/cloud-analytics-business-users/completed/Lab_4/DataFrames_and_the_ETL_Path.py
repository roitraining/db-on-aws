# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4 - Spark DataFrames and the ETL Path

# COMMAND ----------

df = spark.table("training_nic.migrated.institutions")

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

print(df.columns)

# COMMAND ----------

from pyspark.sql import functions as F

filtered = df.filter(F.col("STATE_ABBR_NM") == "CA")
display(filtered.limit(10))

# COMMAND ----------

cleaned = (filtered
           .withColumn("NM_LGL_CLEAN", F.trim(F.col("NM_LGL")))
           .withColumn("start_month", F.trunc(F.col("D_DT_START").cast("date"), "month")))
display(cleaned.select("NM_LGL", "NM_LGL_CLEAN", "D_DT_START", "start_month").limit(10))

# COMMAND ----------

slim = cleaned.select("`#ID_RSSD`", "NM_LGL_CLEAN", "CITY",
                      "STATE_ABBR_NM", "CHTR_TYPE_CD", "start_month")
display(slim.limit(10))

# COMMAND ----------

slim.explain(mode="formatted")

# COMMAND ----------

summary = (slim
           .groupBy("CHTR_TYPE_CD", "start_month")
           .agg(F.count("*").alias("institution_count"),
                F.countDistinct("CITY").alias("distinct_cities"))
           .orderBy("start_month", "CHTR_TYPE_CD"))
display(summary)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS training_nic.analyst")

(summary.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("training_nic.analyst.institution_summary"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM training_nic.analyst.institution_summary
# MAGIC ORDER BY start_month DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC **Language choice per stage** (fill in): read / clean / aggregate in PySpark because ...; verify in SQL because ...

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

# MAGIC %sql
# MAGIC SELECT `#ID_RSSD`, STATE_ABBR_NM
# MAGIC FROM training_nic.perf.institutions_large
# MAGIC WHERE STATE_ABBR_NM = 'WA'
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC **Performance sentence** (fill in): the aggregation shuffled N rows / M bytes across the Exchange and took S seconds; the narrow filter+LIMIT read a fraction of the bytes with no Exchange.