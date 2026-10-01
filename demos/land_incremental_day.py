# Databricks notebook source
# MAGIC %md
# MAGIC # Land an Incremental Day File
# MAGIC
# MAGIC Staged day files live in `/Volumes/training_nic/raw/landing/incremental_days/` — the
# MAGIC pipeline does not watch that path. Copying one into `branches/` gives Auto Loader exactly
# MAGIC one new file to discover on the next update.
# MAGIC
# MAGIC | File | Contents | Expect after pipeline run |
# MAGIC |---|---|---|
# MAGIC | ~~branches_day2.csv~~ | 300 TX + 3 no-key + 2 short-city | consumed during setup verification |
# MAGIC | branches_day3.csv | 250 NY + 2 no-key + 1 short-city | bronze +253, silver +251, quarantine +3, gold NY +251 |
# MAGIC | branches_day4.csv | 200 FL + 1 no-key + 1 short-city | bronze +202, silver +201, quarantine +2, gold FL +201 |
# MAGIC
# MAGIC (Short-city rows **warn** through to silver and gold — they are counted, flagged, and
# MAGIC also captured in quarantine. Day 2 measured: bronze +305, silver +302, quarantine +5,
# MAGIC gold TX +302.)
# MAGIC
# MAGIC **Flow:** run the BEFORE cell → land a day → **Start** the `medallion_labtest` pipeline →
# MAGIC run the AFTER + INSPECT cells. Attach this notebook to **serverless** compute.

# COMMAND ----------

# BEFORE — capture the current counts (write them down)
display(spark.sql("""SELECT
  (SELECT COUNT(*) FROM eng_labtest.work.branches_bronze)     AS bronze,
  (SELECT COUNT(*) FROM eng_labtest.work.branches_silver)     AS silver,
  (SELECT COUNT(*) FROM eng_labtest.work.branches_quarantine) AS quarantine"""))
display(spark.sql("""SELECT STATE_ABBR_NM, branch_count FROM eng_labtest.work.branch_summary_gold
                     WHERE STATE_ABBR_NM IN ('TX','NY','FL') ORDER BY STATE_ABBR_NM"""))

# COMMAND ----------

# LAND DAY 3 — 250 clean NY rows + 3 rule violators
dbutils.fs.cp("/Volumes/training_nic/raw/landing/incremental_days/branches_day3.csv",
              "/Volumes/training_nic/raw/landing/branches/branches_day3.csv")
print("Day 3 landed. Now Start the medallion_labtest pipeline, then run the cells below.")

# COMMAND ----------

# LAND DAY 4 — 200 clean FL rows + 2 rule violators (save for a second demonstration)
# dbutils.fs.cp("/Volumes/training_nic/raw/landing/incremental_days/branches_day4.csv",
#               "/Volumes/training_nic/raw/landing/branches/branches_day4.csv")
# print("Day 4 landed. Now Start the medallion_labtest pipeline.")

# COMMAND ----------

# AFTER — same queries; compare against what you wrote down
display(spark.sql("""SELECT
  (SELECT COUNT(*) FROM eng_labtest.work.branches_bronze)     AS bronze,
  (SELECT COUNT(*) FROM eng_labtest.work.branches_silver)     AS silver,
  (SELECT COUNT(*) FROM eng_labtest.work.branches_quarantine) AS quarantine"""))
display(spark.sql("""SELECT STATE_ABBR_NM, branch_count FROM eng_labtest.work.branch_summary_gold
                     WHERE STATE_ABBR_NM IN ('TX','NY','FL') ORDER BY STATE_ABBR_NM"""))

# COMMAND ----------

# INSPECT — the new rows are named so the room can see them
display(spark.sql("""SELECT ID_RSSD, NM_LGL, CITY, STATE_ABBR_NM
                     FROM eng_labtest.work.branches_silver
                     WHERE NM_LGL LIKE 'DAY %' ORDER BY NM_LGL LIMIT 20"""))
display(spark.sql("""SELECT ID_RSSD, NM_LGL, CITY
                     FROM eng_labtest.work.branches_quarantine ORDER BY NM_LGL DESC LIMIT 20"""))

# COMMAND ----------

# EXPECTATIONS, PER RUN — the same numbers the pipeline UI's "Data quality" panel shows,
# pulled from the event log. (In the UI: open the pipeline, click the branches_silver NODE
# in the graph, right panel → Data quality — scoped to the selected update only. Catalog
# Explorer's table page never shows expectations; they live on the pipeline, not the table.)
display(spark.sql("""
    SELECT date_trunc('minute', t.timestamp) AS run_at,
           e.col.name            AS expectation,
           e.col.passed_records  AS passed,
           e.col.failed_records  AS failed
    FROM event_log(TABLE(eng_labtest.work.branches_silver)) t
    LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations,
        'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) e
    WHERE event_type = 'flow_progress'
      AND details:flow_progress.data_quality.expectations IS NOT NULL
    ORDER BY run_at, expectation"""))

# COMMAND ----------

# CHANGE DATA FEED, IN THE PIPELINE GRAPH — `silver_changes_feed` is a pipeline dataset
# (visible as a node after branches_silver) that streams silver's change feed into a real,
# inspectable table. Every landed day appends its changes here automatically.
display(spark.sql("""SELECT _commit_version, _change_type, COUNT(*) AS rows
                     FROM eng_labtest.work.silver_changes_feed
                     GROUP BY _commit_version, _change_type ORDER BY _commit_version"""))

# COMMAND ----------

# Same data via the function form (ad-hoc, no table needed):
display(spark.sql("""SELECT _commit_version, _change_type, COUNT(*) AS rows
                     FROM table_changes('eng_labtest.work.branches_silver', 2)
                     GROUP BY _commit_version, _change_type ORDER BY _commit_version"""))

# COMMAND ----------

# THE FULL CDC STORY — updates and deletes, not just inserts.
# The quarantine table took a direct UPDATE (CITY 'X' -> 'FIXED'): its feed carries the
# BEFORE and AFTER image of every changed row, paired by commit. (A pipeline FULL REFRESH
# would recompute the table and revert these rows — incremental runs leave them alone.)
display(spark.sql("""SELECT _change_type, _commit_version, ID_RSSD, NM_LGL, CITY
    FROM table_changes('eng_labtest.work.branches_quarantine', 2)
    WHERE _change_type LIKE 'update%' ORDER BY NM_LGL, _change_type"""))

# And deletes, from the audit copy (3 day-2 no-key rows were deleted from it):
display(spark.sql("""SELECT _change_type, COUNT(*) AS rows
    FROM table_changes('eng_labtest.work.quarantine_audit', 0)
    GROUP BY _change_type ORDER BY _change_type"""))

# COMMAND ----------

# SAVING THE FEED TO A TABLE — the audit process. table_changes() output only reaches back
# as far as Delta retention; materializing it into a log table makes the change history
# permanent, queryable, and grantable like any other table:
#
#   CREATE TABLE ..._changes_log AS
#   SELECT _change_type, _commit_version, _commit_timestamp, <columns>
#   FROM table_changes('<table>', <from_version>);
#
# Already created from day 2:
display(spark.sql("""SELECT _change_type, COUNT(*) AS rows
    FROM eng_labtest.work.silver_changes_log GROUP BY _change_type"""))
