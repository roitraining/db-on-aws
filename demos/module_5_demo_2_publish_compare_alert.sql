-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Module 5 · Demo 2: Publish, Compare, and Alert
-- MAGIC
-- MAGIC **Ten minutes.** Setup: the Lab 4 summary table exists; Alerts sidebar accessible.
-- MAGIC
-- MAGIC Be honest at step 2 — do not oversell the materialized view on a small table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 1 — a plain view over the summary table. Query it — instant.

-- COMMAND ----------

CREATE OR REPLACE VIEW training_nic.analyst.vw_institution_summary AS
SELECT CHTR_TYPE_CD, start_month, institution_count, distinct_cities
FROM training_nic.analyst.demo_institution_summary;

-- COMMAND ----------

SELECT * FROM training_nic.analyst.vw_institution_summary ORDER BY start_month DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 2 — a materialized view over the same query. Query it.
-- MAGIC *"At five thousand rows you will not see the difference. At fifty million you would see
-- MAGIC nothing else."* — say it in exactly that honest register.

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW training_nic.analyst.mv_institution_summary AS
SELECT CHTR_TYPE_CD, start_month, institution_count, distinct_cities
FROM training_nic.analyst.demo_institution_summary;

-- COMMAND ----------

SELECT * FROM training_nic.analyst.mv_institution_summary ORDER BY start_month DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 3 — refresh it. Note: a **serverless pipeline** runs the refresh, independent of the
-- MAGIC warehouse you have selected — cost scales with data volume, not warehouse size.

-- COMMAND ----------

REFRESH MATERIALIZED VIEW training_nic.analyst.mv_institution_summary;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 4 — ask the room which they would publish, and why.
-- MAGIC Push for the trade-off in their own words: **freshness vs query latency vs refresh cost.**
-- MAGIC (View: always current, recomputes each read. MV: stored + fast, current as of last refresh,
-- MAGIC **no time travel**.)
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### Step 5 — Alerts, and the known frustration (show it deliberately)
-- MAGIC
-- MAGIC 1. Left sidebar → **Alerts** → **Create Alert**.
-- MAGIC 2. Try to select the saved query from Lab 2 — **show that you cannot**. Each alert owns its
-- MAGIC    query definition; an existing saved query cannot be reused. Every attendee tries this —
-- MAGIC    showing it now turns a lab dead-end into an expected step.
-- MAGIC
-- MAGIC ### Step 6 — author the alert in the alert editor
-- MAGIC
-- MAGIC 1. Write the row-count query directly in the alert editor (copy from the cell below).
-- MAGIC 2. **Run all (1000)** — choose a SQL warehouse in the compute selector.
-- MAGIC 3. Set the **Condition** just *below* the current count (so it stays OK today but fires on a drop).
-- MAGIC 4. **Test condition**.
-- MAGIC 5. Add yourself under **Notifications**.
-- MAGIC 6. Calendar icon → set the schedule (dropdowns, or tick **Show cron syntax** for Quartz cron).
-- MAGIC 7. **View alert** to save.
-- MAGIC
-- MAGIC ### Step 7 — show the status: `OK`, `TRIGGERED`, or `ERROR`.

-- COMMAND ----------

-- The query to author INSIDE the alert editor (alerts cannot reuse saved queries):
SELECT COUNT(*) AS row_count
FROM training_nic.analyst.vw_institution_summary;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Frame the alert as the cost of publishing:** *"You now own something people depend on.
-- MAGIC This is how you find out first."*
-- MAGIC
-- MAGIC **Transition:** *"Your data is published, it refreshes, and it tells you when it breaks.
-- MAGIC This afternoon you make it something a stakeholder can actually look at."* → Module 6.
