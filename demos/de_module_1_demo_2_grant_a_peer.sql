-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DE Module 1 · Demo 2: Grant a Peer, and Watch USE SCHEMA Matter
-- MAGIC
-- MAGIC **Six minutes.** The same three-privilege lesson the Intro course taught analysts — now from
-- MAGIC the **platform-owner side**: these engineers will be the ones a colleague asks when a grant
-- MAGIC "does not work."
-- MAGIC
-- MAGIC **Setup:** a second identity to switch to. Set the `peer` widget (top of the notebook) to that
-- MAGIC principal's email — it defaults to the instructor account so the notebook dry-runs safely.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("peer", "jessetoporowski@gmail.com", "Peer principal (email)")
-- MAGIC PEER = dbutils.widgets.get("peer")
-- MAGIC print("granting to:", PEER)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 0 — the table under demonstration, in the engineer's schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS training_nic.eng_demo;

CREATE TABLE IF NOT EXISTS training_nic.eng_demo.branch_counts
AS SELECT STATE_ABBR_NM, COUNT(*) AS institutions
   FROM training_nic.migrated.institutions
   GROUP BY STATE_ABBR_NM;

SELECT * FROM training_nic.eng_demo.branch_counts ORDER BY institutions DESC LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 1 — grant SELECT. Show the grant is plainly there.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"GRANT SELECT ON TABLE training_nic.eng_demo.branch_counts TO `{dbutils.widgets.get('peer')}`")

-- COMMAND ----------

SHOW GRANTS ON TABLE training_nic.eng_demo.branch_counts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 2 — switch to the peer. Query it. **Access denied.**
-- MAGIC
-- MAGIC ### Step 3 — let it sit. *"The grant is correct. Why can they not read it?"*
-- MAGIC Take answers. Someone will say the grant failed, or needs time to propagate. Neither is true.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 4 — grant traversal. Do NOT touch the SELECT grant.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"GRANT USE CATALOG ON CATALOG training_nic TO `{dbutils.widgets.get('peer')}`")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"GRANT USE SCHEMA ON SCHEMA training_nic.eng_demo TO `{dbutils.widgets.get('peer')}`")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 5 — re-query as the peer. It works.
-- MAGIC
-- MAGIC Say the diagnostic: *"Three privileges, not one. `USE CATALOG` is traversal only and grants
-- MAGIC access to nothing by itself."*
-- MAGIC
-- MAGIC **What to highlight:** prefer **group grants and privilege inheritance** for anything beyond
-- MAGIC a one-off — `GRANT SELECT ON SCHEMA` covers future tables; per-table per-person grants become
-- MAGIC an audit problem.
-- MAGIC
-- MAGIC **Transition:** *"Governance is in place. Now the engineering: what Spark actually does when
-- MAGIC you hand it a query."*

-- COMMAND ----------

-- The owner-side audit trail, for the room:
SHOW GRANTS ON SCHEMA training_nic.eng_demo;
