-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Module 5 · Demo 1: Watch a Correct Grant Fail
-- MAGIC
-- MAGIC **Ten minutes — the highest-value ten minutes on Day 2. Do not shortcut to the working state.**
-- MAGIC
-- MAGIC **Setup:** Two identities — yours and a second principal you can switch to (browser profile or
-- MAGIC incognito window logged in as the second account). Confirm the switch works *before* the session.
-- MAGIC Replace `<colleague>` below with the second principal's email.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 1 — create the view, query it as yourself. Works.

-- COMMAND ----------

CREATE OR REPLACE VIEW training_nic.analyst.vw_demo_summary AS
SELECT CHTR_TYPE_CD, start_month, institution_count, distinct_cities
FROM training_nic.analyst.demo_institution_summary;

-- COMMAND ----------

SELECT * FROM training_nic.analyst.vw_demo_summary LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 2 — grant SELECT, show it landed, plainly

-- COMMAND ----------

GRANT SELECT ON VIEW training_nic.analyst.vw_demo_summary TO `<colleague>`;

-- COMMAND ----------

SHOW GRANTS ON VIEW training_nic.analyst.vw_demo_summary;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 3 — switch to the second identity. Run the SELECT there. **Access denied.**
-- MAGIC
-- MAGIC ### Step 4 — sit in it.
-- MAGIC *"The grant is on the object. You can see it. Why can they not read it?"*
-- MAGIC Take answers — someone usually suggests the grant failed or needs time to propagate.
-- MAGIC **Neither is true.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 5 — grant traversal. Do **NOT** touch the SELECT grant.

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG training_nic TO `<colleague>`;

-- COMMAND ----------

GRANT USE SCHEMA ON SCHEMA training_nic.analyst TO `<colleague>`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 6 — re-run as the colleague. It works.
-- MAGIC
-- MAGIC Steps 3→6 with `SELECT` untouched are the entire lesson: **what changed was traversal, not
-- MAGIC permission on the object.**
-- MAGIC
-- MAGIC ### Step 7 — say the diagnostic out loud
-- MAGIC *"When someone cannot read your table, check `USE SCHEMA` first. It is almost always that."*
-- MAGIC
-- MAGIC The diagnostic line, for re-use at the knowledge check:

-- COMMAND ----------

SHOW GRANTS ON SCHEMA training_nic.analyst;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Also mention:** the three-privilege dance is per-principal — which is precisely why
-- MAGIC **groups** exist. This is the most common support ticket on the platform; they will be the
-- MAGIC person a colleague asks.
-- MAGIC
-- MAGIC **Transition:** *"They can read it now. The next question is whether it is fast enough, and
-- MAGIC whether it is current."* → Demo 2.
