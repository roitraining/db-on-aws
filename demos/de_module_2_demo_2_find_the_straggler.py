# Databricks notebook source
# MAGIC %md
# MAGIC # DE Module 2 · Demo 2: Find the Straggler
# MAGIC
# MAGIC **Seven minutes. Classic cluster required** — this demo reads the **per-task distribution** (Summary Metrics, max vs median), which only the classic Spark UI exposes. Serverless's query profile covers the DAG, shuffle volume, task counts and spill — see `demos/serverless_profile_vs_spark_ui.md` for the verified boundary.
# MAGIC The deliverable is one true sentence an engineer can act on, not a live tuning session.
# MAGIC
# MAGIC This notebook *builds* the skew deliberately (60% of rows on one key), so the straggler is
# MAGIC guaranteed rather than hoped for. Confirm Spark UI panel labels on the training cluster
# MAGIC beforehand — they drift by runtime.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0 — build a skewed table (~2M rows, 60% sharing one key)

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS training_nic.eng_demo")

n = 2_000_000
skewed = (spark.range(n)
          # 60% of rows land on key 'MEGACORP'; the rest spread over 10,000 keys
          .withColumn("org_key",
                      F.when(F.rand(seed=7) < 0.6, F.lit("MEGACORP"))
                       .otherwise(F.concat(F.lit("org_"), (F.col("id") % 10000).cast("string"))))
          .withColumn("amount", (F.rand(seed=11) * 1000).cast("decimal(10,2)")))
skewed.write.mode("overwrite").saveAsTable("training_nic.eng_demo.skewed_transactions")
print(spark.table("training_nic.eng_demo.skewed_transactions").count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — the aggregation that takes a visible few seconds

# COMMAND ----------

agg = (spark.table("training_nic.eng_demo.skewed_transactions")
       .groupBy("org_key")
       .agg(F.count("*").alias("txns"),
            F.countDistinct("amount").alias("distinct_amounts"))  # countDistinct defeats partial aggregation
       .orderBy(F.desc("txns")))
display(agg.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps 2–4 — walk the Spark UI (on screen, classic cluster)
# MAGIC
# MAGIC 1. **Spark UI → Jobs** — find the job for the cell above.
# MAGIC 2. **Stages tab** — count the stages. *"Each boundary is a shuffle."*
# MAGIC 3. Largest stage → **Shuffle Read / Shuffle Write** — name the volume.
# MAGIC 4. **Summary Metrics** → compare **Max** task time/input against the **Median**:
# MAGIC    one task carries ~60% of the rows and runs many times longer than its peers.
# MAGIC    *"That is skew, not volume."* Skew is read from the **distribution**, not the total moved.
# MAGIC
# MAGIC ### Step 5 — frame the deliverable
# MAGIC *"You are not tuning this live. You are producing one true sentence: the groupBy shuffled
# MAGIC N MB and one task ran k× the median on key MEGACORP."*

# COMMAND ----------

# The evidence in numbers (for the sentence): row distribution across keys
display(spark.table("training_nic.eng_demo.skewed_transactions")
        .groupBy("org_key").count().orderBy(F.desc("count")).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Transition:** *"You can read what Spark did and name why it was slow. This afternoon we go
# MAGIC one layer down, into how Delta stores the result."*
