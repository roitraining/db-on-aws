# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12 reference pipeline
# MAGIC
# MAGIC Deliberately minimal. Lab 12 is about the manifest, not the pipeline — the full medallion
# MAGIC implementation lives in `bundles/solutions/lab-10-pipeline`.

# COMMAND ----------

from pyspark import pipelines as dp


@dp.table(name="institutions_bronze")
def institutions_bronze():
    return spark.readStream.table("training_nic.legacy_onprem.institutions")
