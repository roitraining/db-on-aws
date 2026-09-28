# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: The Delta Log, Old School — the Actual Files
# MAGIC
# MAGIC Unity Catalog governs table storage away, so `training_nic` tables never show their
# MAGIC `_delta_log`. But a **hive_metastore** table on the DBFS root is the pre-UC world —
# MAGIC and there the log is just files you can list and read.
# MAGIC
# MAGIC **Requires a classic cluster** (serverless cannot touch the DBFS root) — in class this
# MAGIC runs on the instructor's shared cluster. Safe to re-run: it rebuilds its own scratch table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 — build a small table and change it (two versions)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS hive_metastore.default.delta_log_demo")
spark.sql("CREATE TABLE hive_metastore.default.delta_log_demo AS "
          "SELECT id, id % 5 AS bucket FROM range(1000)")
spark.sql("UPDATE hive_metastore.default.delta_log_demo SET bucket = 99 WHERE bucket = 0")

loc = spark.sql("DESCRIBE DETAIL hive_metastore.default.delta_log_demo").collect()[0]["location"]
print("the table lives at:", loc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 — the table's folder: Parquet files plus `_delta_log`
# MAGIC This is the slide diagram, for real.

# COMMAND ----------

display(dbutils.fs.ls(loc))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 — inside `_delta_log`: one JSON per version

# COMMAND ----------

display(dbutils.fs.ls(loc + "/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 — read version 0: the CREATE
# MAGIC Scroll the output: `commitInfo` (who/when/what), `metaData` (the schema), and `add`
# MAGIC actions naming the Parquet files this version consists of.

# COMMAND ----------

print(dbutils.fs.head(loc + "/_delta_log/00000000000000000000.json", 1500))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 — read version 1: the UPDATE
# MAGIC `commitInfo` records the operation and its predicate — then `remove` actions retire the
# MAGIC old files and `add` actions bring in the rewritten ones. **Delta never edits a Parquet
# MAGIC file in place.**

# COMMAND ----------

print(dbutils.fs.head(loc + "/_delta_log/00000000000000000001.json", 1500))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6 — and DESCRIBE HISTORY is exactly these files, rendered
# MAGIC One row per JSON. On Unity Catalog tables you only ever get this view — the files
# MAGIC themselves are deliberately unreachable, because a client that can touch the log can
# MAGIC corrupt the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY hive_metastore.default.delta_log_demo;
