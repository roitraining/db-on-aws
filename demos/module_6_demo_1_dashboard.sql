-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Module 6 · Demo 1: From Published Dataset to Interactive Dashboard
-- MAGIC
-- MAGIC **Twelve minutes. Build live** — the audience needs to see it is *quick*, not watch a
-- MAGIC finished artifact. The deliberate mistake at step 6 is worth the time: a silently mis-scoped
-- MAGIC filter is the defect that damages trust fastest, and it looks like nothing on screen.
-- MAGIC
-- MAGIC **Setup:** the view published in Module 5's demo. Verify it below before class.

-- COMMAND ----------

-- Pre-class check: the dashboard's dataset source exists and has data
SELECT COUNT(*) AS rows_available FROM training_nic.analyst.vw_institution_summary;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The click path
-- MAGIC
-- MAGIC 1. **Create the dashboard.** Left sidebar → **Dashboards** → **Create dashboard**.
-- MAGIC    On the **Data** tab, **Add data source** → pick `training_nic.analyst.vw_institution_summary`.
-- MAGIC    Show the data preview.
-- MAGIC 2. **Bar chart** — institutions by state*. Title it as a stakeholder would say it
-- MAGIC    (e.g. "Institutions by charter type", not "chart_1").
-- MAGIC 3. **Line chart** — institutions by start month. *Two charts, one dataset.*
-- MAGIC 4. **Add a date-range filter** on `start_month`. **Scope it to BOTH charts** — say so
-- MAGIC    explicitly while doing it.
-- MAGIC 5. **Move the filter.** Both charts respond.
-- MAGIC 6. **The deliberate mistake:** edit the filter, re-scope it to ONE chart only. Move it.
-- MAGIC    *"Two charts, same dashboard, disagreeing. Which does the director believe?"* Let the
-- MAGIC    discomfort land.
-- MAGIC 7. **Fix it.** Then click a bar to demonstrate **cross-filtering**.
-- MAGIC
-- MAGIC *The demo summary table is charter type × month; "by state" needs the state column — if you
-- MAGIC want a state chart, point the dataset at `training_nic.migrated.institutions` instead, or add
-- MAGIC STATE_ABBR_NM to the Module 5 demo view before class.
-- MAGIC
-- MAGIC ### What to highlight
-- MAGIC - How little time this took compared with maintaining the spreadsheet.
-- MAGIC - It points at the **view** — restructure underneath and the dashboard survives.
-- MAGIC
-- MAGIC **Transition:** *"It works for you. Publishing decides what it does for everyone else — and
-- MAGIC that decision is bigger than it looks."* → shared vs individual credentials (6.3).
