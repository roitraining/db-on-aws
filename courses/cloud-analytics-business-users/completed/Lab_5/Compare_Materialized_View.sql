-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 5 completed — Compare Materialized View (scratch notebook)
-- MAGIC One cell on purpose: this is a throwaway experiment, not a runbook. Attach the serverless SQL warehouse — materialized views cannot be created from serverless notebook compute.

-- COMMAND ----------

-- same query as the published view, precomputed and stored
USE CATALOG training_nic;
USE SCHEMA analyst;

CREATE OR REPLACE MATERIALIZED VIEW institution_summary_mv AS
SELECT CHTR_TYPE_CD, start_month, institution_count, distinct_cities
FROM training_nic.analyst.institution_summary
WHERE institution_count > 0;

SELECT * FROM institution_summary_mv ORDER BY start_month DESC LIMIT 20;

REFRESH MATERIALIZED VIEW institution_summary_mv;