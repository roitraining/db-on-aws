-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DE Module 3 · Demo 1: The Transaction Log and Time Travel
-- MAGIC
-- MAGIC **Eight minutes.** *"The log is the table, not the files."* Versions must be created **today**
-- MAGIC so time travel works inside the retention window — this notebook creates them fresh on each run.
-- MAGIC
-- MAGIC Storage note: on a UC managed table the S3 path isn't browsable, so the files+log view comes
-- MAGIC from `DESCRIBE DETAIL` and the hidden `_metadata` column (same evidence, governed path). In a
-- MAGIC customer-managed deployment you'd show the same thing in the S3 console — you own the bucket.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 0 — version 0: the initial load from the source extract

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS training_nic.eng_demo;

CREATE OR REPLACE TABLE training_nic.eng_demo.institutions_versioned
AS SELECT * FROM training_nic.migrated.institutions;

SELECT COUNT(*) AS v0_rows FROM training_nic.eng_demo.institutions_versioned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 1 — the files and the log. Parquet files + `_delta_log`; the log is the table.

-- COMMAND ----------

DESCRIBE DETAIL training_nic.eng_demo.institutions_versioned;

-- COMMAND ----------

-- The actual Parquet files behind the table, via the hidden _metadata column:
SELECT DISTINCT _metadata.file_path
FROM training_nic.eng_demo.institutions_versioned
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 2 — DESCRIBE HISTORY. Walk version, timestamp, operation.

-- COMMAND ----------

DESCRIBE HISTORY training_nic.eng_demo.institutions_versioned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 3 — an incremental load. A new version appears.

-- COMMAND ----------

INSERT INTO training_nic.eng_demo.institutions_versioned
SELECT * FROM training_nic.migrated.institutions LIMIT 500;

-- COMMAND ----------

DESCRIBE HISTORY training_nic.eng_demo.institutions_versioned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 4 — the table as it was at load. *"Delta kept it; I did not."*

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM training_nic.eng_demo.institutions_versioned VERSION AS OF 0) AS rows_at_v0,
  (SELECT COUNT(*) FROM training_nic.eng_demo.institutions_versioned)                 AS rows_now;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 5 — say the retention limit out loud
-- MAGIC
-- MAGIC Time travel is bounded by the deleted-file retention window — **7 days by default**. The
-- MAGIC consequence for a real cutover: a "compare against the pre-migration state" promise is only
-- MAGIC good for a week unless retention is raised *before* the cutover. Versioning is a property of
-- MAGIC the table — no backup job, no snapshot table — but it is not an archive.
-- MAGIC
-- MAGIC **Transition:** *"Those incremental loads just wrote more files. Enough of them, and reads
-- MAGIC slow down."* → Demo 2.
