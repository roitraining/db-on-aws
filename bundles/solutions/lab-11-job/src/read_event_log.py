# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 11 solution - read pipeline quality outcomes
# MAGIC
# MAGIC **ANSWER KEY.**
# MAGIC
# MAGIC ## Why this is a Python notebook and not a SQL task
# MAGIC
# MAGIC `dbutils.jobs.taskValues.set` is available in **Python notebooks only**. A SQL task cannot
# MAGIC emit a task value, so the downstream If/else task would have nothing to read. If you rewrite
# MAGIC this as a SQL task the job still deploys and still runs - the gate just silently never sees a
# MAGIC value. Values are capped at 48 KiB.
# MAGIC
# MAGIC ## The event log, and the trap in it
# MAGIC
# MAGIC `event_log()` is a table-valued function taking either a table name or a pipeline_id. It can
# MAGIC only be called by the **owner** of the streaming table or materialized view passed to it.
# MAGIC That collides with Run As: change the job's identity to a service principal and this task
# MAGIC starts failing while every other task keeps succeeding.

# COMMAND ----------

dbutils.widgets.text("catalog", "training_nic")
CATALOG = dbutils.widgets.get("catalog")

SILVER = f"{CATALOG}.lab10_solution.branches_silver"

# COMMAND ----------

dropped = 0
source = "unavailable"

try:
    df = spark.sql(f"""
        SELECT
          SUM(CAST(details:flow_progress.data_quality.dropped_records AS BIGINT)) AS dropped
        FROM event_log(TABLE({SILVER}))
        WHERE event_type = 'flow_progress'
    """)
    row = df.collect()[0]
    dropped = int(row["dropped"] or 0)
    source = "event_log"
    print(f"event_log reports {dropped} dropped record(s)")

except Exception as e:
    msg = str(e)
    print(f"event_log unavailable: {msg[:200]}")
    if "does not have" in msg or "PERMISSION" in msg.upper() or "owner" in msg.lower():
        print()
        print("This is the ownership constraint, not a syntax problem.")
        print("event_log() can only be called by the OWNER of the streaming table.")
        print("If Run As was changed to a service principal, that principal is not the owner.")
    print("Falling back to 0 so the gate still has a value to read.")

# COMMAND ----------

# The cast to int matters. A string operand makes the If/else comparison lexicographic,
# which is why "9" > "100" and the gate appears to take the wrong branch for no reason.
dbutils.jobs.taskValues.set(key="dropped_records", value=int(dropped))
dbutils.jobs.taskValues.set(key="source", value=source)

print(f"emitted dropped_records = {dropped}  (source: {source})")
