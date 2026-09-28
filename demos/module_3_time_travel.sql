-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Module 3 Demo: Time Travel as a Validation Tool
-- MAGIC
-- MAGIC Six minutes. Serverless compute.
-- MAGIC
-- MAGIC **Setup check:** this table needs history created recently — data files older than the
-- MAGIC 7-day retention may be blocked (DBR 18+ blocks them outright). The environment build
-- MAGIC stages versions v0–v8, so a fresh setup run guarantees the demo. Confirm with step 1
-- MAGIC before class.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1 — the table keeps its own history
-- MAGIC Walk the columns out loud: **version, timestamp, operation**. Nobody took a backup —
-- MAGIC versioning is a property of the table.

-- COMMAND ----------

DESCRIBE HISTORY training_nic.migrated.institutions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Point at the **DELETE** row in the operation column and say nothing else about it yet.
-- MAGIC
-- MAGIC ### 2 — query the table as it existed at load

-- COMMAND ----------

SELECT 'current' AS version, COUNT(*) AS rows FROM training_nic.migrated.institutions
UNION ALL
SELECT 'VERSION AS OF 0', COUNT(*) FROM training_nic.migrated.institutions VERSION AS OF 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **62,080 at version 0 — the on-premises count.** The 381-row gap from this afternoon's
-- MAGIC check 1 did not happen in transit. It happened *inside this table's own lifetime*, at
-- MAGIC the DELETE sitting right there in the history. Time travel just told you **where** a
-- MAGIC defect happened, not only that it exists.
-- MAGIC
-- MAGIC ### 3 — by timestamp instead of version
-- MAGIC Copy a real timestamp from the step-1 output into the query below (any time after the
-- MAGIC earliest version works).

-- COMMAND ----------

-- paste a timestamp from DESCRIBE HISTORY between the quotes:
-- SELECT COUNT(*) FROM training_nic.migrated.institutions TIMESTAMP AS OF '<paste-here>';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4 — the limits (say these before anyone plans around the feature)
-- MAGIC - History **metadata**: 30 days by default (`logRetentionDuration`)
-- MAGIC - Data **files**: 7 days by default — **this is the binding limit**
-- MAGIC - DBR 18.0+ blocks time travel older than the file retention outright
-- MAGIC - Need a longer comparison window? That is an engineering conversation to have
-- MAGIC   **before** cutover, not after.
