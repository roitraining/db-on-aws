-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Demo: Delta Under the Hood — the Files and the Log
-- MAGIC
-- MAGIC Shows that a Delta table is Parquet files plus a transaction log, without needing
-- MAGIC filesystem access (Unity Catalog governs the files away — the old DBFS browser
-- MAGIC doesn't apply to managed tables). Serverless compute.
-- MAGIC
-- MAGIC Flow: see the files → see the log → make a change → watch both change.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1 — the table's file-level facts
-- MAGIC Point at three columns: **location** (a real cloud-storage path), **numFiles**, **sizeInBytes**.
-- MAGIC One .mdf on one server became many Parquet files in object storage.

-- COMMAND ----------

DESCRIBE DETAIL training_nic.migrated.institutions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2 — the actual files
-- MAGIC Every row you read carries hidden `_metadata` about the file it came from.
-- MAGIC These are the real Parquet files behind the table — name, size, timestamp.

-- COMMAND ----------

SELECT _metadata.file_name                                   AS file_name,
       ROUND(MAX(_metadata.file_size) / 1024 / 1024, 2)      AS size_mb,
       MAX(_metadata.file_modification_time)                 AS modified,
       COUNT(*)                                              AS rows_in_file
FROM   training_nic.migrated.institutions
GROUP  BY 1
ORDER  BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3 — the transaction log
-- MAGIC One row per log entry: version, timestamp, operation. **The log is the table** —
-- MAGIC it records what each version contained.

-- COMMAND ----------

DESCRIBE HISTORY training_nic.migrated.institutions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4 — watch both change: make a copy and modify it
-- MAGIC Never modify the table you are auditing — so we work on our own copy.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS training_nic.analyst;
-- drop first so version 0 is always this CREATE (CREATE OR REPLACE would keep counting)
DROP TABLE IF EXISTS training_nic.analyst.delta_demo;
CREATE TABLE training_nic.analyst.delta_demo AS
SELECT * FROM training_nic.migrated.institutions
WHERE  STATE_ABBR_NM = 'CA';

-- COMMAND ----------

-- our copy: clean the padded names (a deliberate cleanup, in OUR table, not the source)
UPDATE training_nic.analyst.delta_demo
SET    NM_LGL = TRIM(NM_LGL);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5 — the log recorded it
-- MAGIC The UPDATE is a new version. Expand **operationMetrics**: rows touched, files added,
-- MAGIC files removed. Delta never edits a Parquet file in place — it writes new files and
-- MAGIC the log says which files ARE the current version.

-- COMMAND ----------

DESCRIBE HISTORY training_nic.analyst.delta_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6 — and the files changed
-- MAGIC Compare the modification times to step 2 — these files were just written. The
-- MAGIC pre-UPDATE files still exist in storage (that is what time travel reads); they are
-- MAGIC only physically removed later by `VACUUM`.

-- COMMAND ----------

SELECT _metadata.file_name                                   AS file_name,
       ROUND(MAX(_metadata.file_size) / 1024 / 1024, 2)      AS size_mb,
       MAX(_metadata.file_modification_time)                 AS modified,
       COUNT(*)                                              AS rows_in_file
FROM   training_nic.analyst.delta_demo
GROUP  BY 1
ORDER  BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7 — proof the old version still exists

-- COMMAND ----------

SELECT 'current (trimmed)' AS version, MAX(LENGTH(NM_LGL)) AS max_name_len FROM training_nic.analyst.delta_demo
UNION ALL
SELECT 'before the UPDATE', MAX(LENGTH(NM_LGL)) FROM training_nic.analyst.delta_demo VERSION AS OF 0;
