-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Migration Validation Runbook
-- MAGIC 
-- MAGIC **Findings (fill in as you work):** for every failed check note which check surfaced it, how many rows it affects, what the affected rows share, and the suspected cause. Ruled-out artifacts and the cutover recommendation go here too.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS training_nic.analyst;

CREATE TABLE IF NOT EXISTS training_nic.analyst.validation_runs (
  run_ts       TIMESTAMP,
  check_name   STRING,
  cloud_value  DECIMAL(18,2),
  source_value DECIMAL(18,2),
  passed       BOOLEAN,
  note         STRING);

DECLARE OR REPLACE VARIABLE run_started TIMESTAMP DEFAULT current_timestamp();

-- COMMAND ----------

USE CATALOG training_nic;
USE SCHEMA migrated;
SELECT COUNT(*) AS cloud_rows FROM institutions;

-- COMMAND ----------

SELECT COUNT(*) AS source_rows FROM training_nic.legacy_onprem.institutions;

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM training_nic.migrated.institutions)      AS cloud_rows,
  (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions) AS source_rows;

-- COMMAND ----------

INSERT INTO training_nic.analyst.validation_runs
SELECT run_started, 'check_1_row_count',
       (SELECT COUNT(*) FROM training_nic.migrated.institutions),
       (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions),
       (SELECT COUNT(*) FROM training_nic.migrated.institutions) =
       (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions),
       'total row counts, cloud vs source';

-- COMMAND ----------

SELECT s.`#ID_RSSD`
FROM training_nic.legacy_onprem.institutions AS s
LEFT ANTI JOIN training_nic.migrated.institutions AS c
  ON s.`#ID_RSSD` = c.`#ID_RSSD`;

-- COMMAND ----------

SELECT c.`#ID_RSSD`
FROM training_nic.migrated.institutions AS c
LEFT ANTI JOIN training_nic.legacy_onprem.institutions AS s
  ON c.`#ID_RSSD` = s.`#ID_RSSD`;

-- COMMAND ----------

SELECT s.CHTR_TYPE_CD, COUNT(*) AS missing_count
FROM training_nic.legacy_onprem.institutions AS s
LEFT ANTI JOIN training_nic.migrated.institutions AS c
  ON s.`#ID_RSSD` = c.`#ID_RSSD`
GROUP BY s.CHTR_TYPE_CD
ORDER BY missing_count DESC;

-- COMMAND ----------

INSERT INTO training_nic.analyst.validation_runs
SELECT run_started, 'check_2_key_parity',
       (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions s
          LEFT ANTI JOIN training_nic.migrated.institutions c ON s.`#ID_RSSD` = c.`#ID_RSSD`),
       (SELECT COUNT(*) FROM training_nic.migrated.institutions c
          LEFT ANTI JOIN training_nic.legacy_onprem.institutions s ON c.`#ID_RSSD` = s.`#ID_RSSD`),
       (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions s
          LEFT ANTI JOIN training_nic.migrated.institutions c ON s.`#ID_RSSD` = c.`#ID_RSSD`) = 0
       AND
       (SELECT COUNT(*) FROM training_nic.migrated.institutions c
          LEFT ANTI JOIN training_nic.legacy_onprem.institutions s ON c.`#ID_RSSD` = s.`#ID_RSSD`) = 0,
       'cloud_value = keys missing from cloud; source_value = keys the cloud invented';

-- COMMAND ----------

SELECT
  (SELECT SUM(TOT_ASSETS) FROM training_nic.migrated.financials)      AS cloud_total,
  (SELECT SUM(TOT_ASSETS) FROM training_nic.legacy_onprem.financials) AS source_total;

-- COMMAND ----------

SELECT SUM(c.TOT_ASSETS) AS cloud_total,
       SUM(s.TOT_ASSETS) AS source_total,
       SUM(s.TOT_ASSETS) - SUM(c.TOT_ASSETS) AS difference
FROM training_nic.migrated.financials AS c
JOIN training_nic.legacy_onprem.financials AS s
  ON c.`#ID_RSSD` = s.`#ID_RSSD`;

-- COMMAND ----------

SELECT SUM(CASE WHEN CITY IS NULL THEN 1 ELSE 0 END) AS null_cities,
       SUM(CASE WHEN CITY = ''   THEN 1 ELSE 0 END) AS empty_cities
FROM training_nic.migrated.institutions;

-- COMMAND ----------

SELECT SUM(CASE WHEN CITY IS NULL THEN 1 ELSE 0 END) AS null_cities,
       SUM(CASE WHEN CITY = ''   THEN 1 ELSE 0 END) AS empty_cities
FROM training_nic.legacy_onprem.institutions;

-- COMMAND ----------

INSERT INTO training_nic.analyst.validation_runs
SELECT run_started, 'check_3_aggregate',
       SUM(c.TOT_ASSETS), SUM(s.TOT_ASSETS),
       SUM(c.TOT_ASSETS) = SUM(s.TOT_ASSETS),
       'SUM(TOT_ASSETS) over keys present on both sides'
FROM training_nic.migrated.financials AS c
JOIN training_nic.legacy_onprem.financials AS s
  ON c.`#ID_RSSD` = s.`#ID_RSSD`;

-- COMMAND ----------

SELECT c.`#ID_RSSD`, c.NM_LGL AS cloud_name, s.NM_LGL AS source_name
FROM training_nic.migrated.institutions AS c
JOIN training_nic.legacy_onprem.institutions AS s ON c.`#ID_RSSD` = s.`#ID_RSSD`
WHERE c.NM_LGL <> s.NM_LGL
LIMIT 50;

-- COMMAND ----------

SELECT COUNT(*) AS naive_mismatches
FROM training_nic.migrated.institutions AS c
JOIN training_nic.legacy_onprem.institutions AS s ON c.`#ID_RSSD` = s.`#ID_RSSD`
WHERE c.NM_LGL <> s.NM_LGL;

-- COMMAND ----------

SELECT COUNT(*) AS real_mismatches
FROM training_nic.migrated.institutions AS c
JOIN training_nic.legacy_onprem.institutions AS s ON c.`#ID_RSSD` = s.`#ID_RSSD`
WHERE NULLIF(TRIM(c.NM_LGL), '') IS DISTINCT FROM NULLIF(TRIM(s.NM_LGL), '');

-- COMMAND ----------

INSERT INTO training_nic.analyst.validation_runs
SELECT run_started, 'check_4_row_level',
       (SELECT COUNT(*) FROM training_nic.migrated.institutions c
          JOIN training_nic.legacy_onprem.institutions s ON c.`#ID_RSSD` = s.`#ID_RSSD`
          WHERE NULLIF(TRIM(c.NM_LGL), '') IS DISTINCT FROM NULLIF(TRIM(s.NM_LGL), '')),
       (SELECT COUNT(*) FROM training_nic.migrated.institutions c
          JOIN training_nic.legacy_onprem.institutions s ON c.`#ID_RSSD` = s.`#ID_RSSD`
          WHERE c.NM_LGL <> s.NM_LGL),
       (SELECT COUNT(*) FROM training_nic.migrated.institutions c
          JOIN training_nic.legacy_onprem.institutions s ON c.`#ID_RSSD` = s.`#ID_RSSD`
          WHERE NULLIF(TRIM(c.NM_LGL), '') IS DISTINCT FROM NULLIF(TRIM(s.NM_LGL), '')) = 0,
       'cloud_value = normalized NM_LGL mismatches; source_value = naive count';

-- COMMAND ----------

SELECT c.`#ID_RSSD`, c.D_DT_START AS cloud_date, s.D_DT_START AS source_date,
       datediff(c.D_DT_START, s.D_DT_START) AS day_difference
FROM training_nic.migrated.institutions AS c
JOIN training_nic.legacy_onprem.institutions AS s ON c.`#ID_RSSD` = s.`#ID_RSSD`
WHERE c.D_DT_START IS DISTINCT FROM s.D_DT_START
LIMIT 50;

-- COMMAND ----------

SELECT datediff(c.D_DT_START, s.D_DT_START) AS day_difference,
       COUNT(*) AS row_count
FROM training_nic.migrated.institutions AS c
JOIN training_nic.legacy_onprem.institutions AS s ON c.`#ID_RSSD` = s.`#ID_RSSD`
WHERE c.D_DT_START IS DISTINCT FROM s.D_DT_START
GROUP BY 1 ORDER BY row_count DESC;

-- COMMAND ----------

DESCRIBE HISTORY training_nic.migrated.institutions;

-- COMMAND ----------

SELECT COUNT(*) AS rows_at_version FROM training_nic.migrated.institutions VERSION AS OF 0;

-- COMMAND ----------

SELECT run_ts, check_name, cloud_value, source_value, passed
FROM training_nic.analyst.validation_runs
ORDER BY run_ts, check_name;

-- COMMAND ----------

SELECT run_ts AS run_attempt,
       COUNT(*) AS checks_run,
       SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS checks_passed
FROM training_nic.analyst.validation_runs
GROUP BY run_ts ORDER BY run_ts;