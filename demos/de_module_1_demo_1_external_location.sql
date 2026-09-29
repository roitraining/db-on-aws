-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DE Module 1 · Demo 1: External Location and the Managed / External Contrast
-- MAGIC
-- MAGIC **Eight minutes.** The memorable fact: **the storage credential is not SQL** — it wraps an
-- MAGIC IAM role, and the External ID is what makes AWS trust us back. Every SQL Server engineer
-- MAGIC expects `CREATE ... CREDENTIAL` to work; it does not.
-- MAGIC
-- MAGIC **Setup (before class):** a storage credential over the training bucket already created in
-- MAGIC **Catalog Explorer → External data → Credentials** (show the IAM Role ARN + External ID on
-- MAGIC screen as step 1). This notebook takes over at the SQL boundary.
-- MAGIC
-- MAGIC **Environment note:** Free Edition / trial workspaces have no storage credential, so the
-- MAGIC external-location cells only run in the instructor's paid workspace. The managed-table drop
-- MAGIC contrast (steps 4–6) runs anywhere.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 2 — the external location IS SQL. Two objects, two privileges, on purpose.
-- MAGIC (Replace the credential name with yours: `SHOW STORAGE CREDENTIALS` lists them.)

-- COMMAND ----------

SHOW STORAGE CREDENTIALS;

-- COMMAND ----------

-- Instructor workspace only — requires the pre-created credential:
-- CREATE EXTERNAL LOCATION IF NOT EXISTS training_ext_loc
--   URL 's3://roi-databricks-demo-data/uc-external-demo/'
--   WITH (STORAGE CREDENTIAL <credential_name>)
--   COMMENT 'Module 1 demo: external location over the training bucket';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 3 — an external table at a path inside the location

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS training_nic.eng_demo;

-- COMMAND ----------

-- Instructor workspace only (needs the external location above):
-- CREATE OR REPLACE TABLE training_nic.eng_demo.institutions_external
-- LOCATION 's3://roi-databricks-demo-data/uc-external-demo/institutions_external'
-- AS SELECT * FROM training_nic.migrated.institutions LIMIT 1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 4 — a managed table from the same query. No path — Unity Catalog owns the files.

-- COMMAND ----------

CREATE OR REPLACE TABLE training_nic.eng_demo.institutions_managed
AS SELECT * FROM training_nic.migrated.institutions LIMIT 1000;

-- COMMAND ----------

-- Where did Unity Catalog put it? DESCRIBE DETAIL shows the managed location you never chose.
DESCRIBE DETAIL training_nic.eng_demo.institutions_managed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 5 — the drop contrast. *"That difference is the whole reason both exist."*
-- MAGIC
-- MAGIC - `DROP` the **managed** table → Unity Catalog deletes the data with it.
-- MAGIC - `DROP` the **external** table → the catalog entry goes; **the files remain at the S3
-- MAGIC   path** (show the path in the S3 console, or re-CREATE the table on the same LOCATION and
-- MAGIC   the data is still there).
-- MAGIC
-- MAGIC Managed vs external is a **lifecycle** decision, not a performance one.

-- COMMAND ----------

DROP TABLE training_nic.eng_demo.institutions_managed;

-- COMMAND ----------

-- Proof the managed data is gone with the table:
SELECT COUNT(*) AS should_error FROM training_nic.eng_demo.institutions_managed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ☝️ **That error is the demo.** The managed table's data went with it.
-- MAGIC In the instructor workspace, now `DROP` the external table and show the S3 files surviving.
-- MAGIC
-- MAGIC **Transition:** *"You can now stand up governed storage. Next we look at what actually runs
-- MAGIC when you query it — because the execution model is where SQL Server habits mislead you."*
