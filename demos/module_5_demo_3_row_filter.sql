-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Module 5 · Demo 3 (bonus): Row Filters and Column Masks
-- MAGIC
-- MAGIC **Five minutes. Bonus depth — not in the outline.** Slots directly after Demo 1:
-- MAGIC *"Grants control WHETHER you can see a table. Row filters control WHICH ROWS you see in it,
-- MAGIC and column masks control WHAT a column shows you. Same table, different truths per person."*
-- MAGIC
-- MAGIC **Setup:** the same second identity as Demo 1. Replace `<colleague>` with that principal's
-- MAGIC email in the two function definitions AND the grants below. Uses the `nic_econ` enrichment
-- MAGIC catalog (the pipeline showcase data the room has been looking at on the dashboard).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 1 — a demo table (row filters attach to tables, not views)

-- COMMAND ----------

CREATE OR REPLACE TABLE nic_econ.gold.state_overview_secured AS
SELECT * FROM nic_econ.gold.mv_state_overview;

SELECT COUNT(*) AS all_rows FROM nic_econ.gold.state_overview_secured;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 2 — the row-filter function
-- MAGIC A boolean UDF, evaluated per row, per query, per user. You (the owner) see everything;
-- MAGIC the colleague sees only Illinois. In production the CASE would be a lookup table or
-- MAGIC `is_account_group_member()` per regional group.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION nic_econ.gold.f_state_access(row_state STRING)
RETURN
  CASE CURRENT_USER()
    WHEN '<colleague>' THEN row_state = 'IL'
    ELSE TRUE
  END;

-- COMMAND ----------

ALTER TABLE nic_econ.gold.state_overview_secured
  SET ROW FILTER nic_econ.gold.f_state_access ON (state_abbr);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 3 — the column mask
-- MAGIC Same idea, but for one column's *values*: the colleague sees bank net income nulled out.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION nic_econ.gold.f_mask_income(income DOUBLE)
RETURN
  CASE CURRENT_USER()
    WHEN '<colleague>' THEN CAST(NULL AS DOUBLE)
    ELSE income
  END;

-- COMMAND ----------

ALTER TABLE nic_econ.gold.state_overview_secured
  ALTER COLUMN net_income_busd SET MASK nic_econ.gold.f_mask_income;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 4 — grants (three privileges, exactly as Demo 1 taught)

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG nic_econ TO `<colleague>`;

-- COMMAND ----------

GRANT USE SCHEMA ON SCHEMA nic_econ.gold TO `<colleague>`;

-- COMMAND ----------

GRANT SELECT ON TABLE nic_econ.gold.state_overview_secured TO `<colleague>`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 5 — run as yourself: 104 rows, income visible

-- COMMAND ----------

SELECT state_abbr, year, institutions_active, net_income_busd
FROM nic_econ.gold.state_overview_secured
ORDER BY state_abbr, year
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 6 — switch to the colleague. Run the SAME query.
-- MAGIC **Two rows (Illinois only), and `net_income_busd` is NULL.** Same table name, same SQL,
-- MAGIC different truth. Nobody wrote a WHERE clause; nobody can forget to apply it; there is no
-- MAGIC unsecured path to the data.
-- MAGIC
-- MAGIC ### Step 7 — draw the line back to Module 6
-- MAGIC *"Remember the dashboard credential decision this afternoon: with SHARED credentials the
-- MAGIC dashboard reads as the publisher, so it bypasses per-viewer row filters. With INDIVIDUAL
-- MAGIC permissions, this filter follows each viewer into the dashboard. That is the same decision,
-- MAGIC one layer up."*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Teardown (after the demo, so re-runs start clean)
-- MAGIC ```sql
-- MAGIC ALTER TABLE nic_econ.gold.state_overview_secured DROP ROW FILTER;
-- MAGIC ALTER TABLE nic_econ.gold.state_overview_secured ALTER COLUMN net_income_busd DROP MASK;
-- MAGIC ```
