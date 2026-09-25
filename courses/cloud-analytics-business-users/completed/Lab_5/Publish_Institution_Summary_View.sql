-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Publish Institution Summary View — runbook
-- MAGIC Rebuilds the published view and its grants. Run all after any environment rebuild.

-- COMMAND ----------

USE CATALOG training_nic;
USE SCHEMA analyst;

-- COMMAND ----------

CREATE OR REPLACE VIEW institution_summary_published AS
SELECT CHTR_TYPE_CD, start_month, institution_count, distinct_cities
FROM training_nic.analyst.institution_summary
WHERE institution_count > 0;

-- COMMAND ----------

SELECT * FROM institution_summary_published
ORDER BY start_month DESC
LIMIT 20;

-- COMMAND ----------

GRANT SELECT ON VIEW institution_summary_published TO `account users`;

-- COMMAND ----------

SHOW GRANTS ON VIEW institution_summary_published;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA training_nic.analyst;

-- COMMAND ----------

SHOW GRANTS ON CATALOG training_nic;

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG training_nic TO `account users`;
GRANT USE SCHEMA  ON SCHEMA  training_nic.analyst TO `account users`;

-- COMMAND ----------

SHOW GRANTS ON VIEW institution_summary_published;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA training_nic.analyst;

-- COMMAND ----------

SHOW GRANTS ON CATALOG training_nic;