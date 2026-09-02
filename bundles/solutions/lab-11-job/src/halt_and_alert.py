# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 11 solution - halt (false branch)
# MAGIC
# MAGIC **ANSWER KEY.** Runs only when the quality gate failed.
# MAGIC
# MAGIC When the gate routes the other way, this task runs and `promote_gold` reports
# MAGIC **SKIPPED / EXCLUDED** rather than failing. A skipped branch is not an error, and the job as a
# MAGIC whole still reports SUCCESS - which is the behaviour you want from a gate and a common
# MAGIC surprise the first time someone sees it.

# COMMAND ----------

dbutils.widgets.text("catalog", "training_nic")

dropped = dbutils.jobs.taskValues.get(
    taskKey="read_event_log", key="dropped_records", default=0, debugValue=0)

print(f"Quality gate FAILED: {dropped} dropped record(s) exceeds the threshold.")
print("Gold promotion halted. Upstream data is not fit to publish.")
print()
print("Wire a notification here in production - the point of a gate is that somebody")
print("finds out before the stakeholders do.")
