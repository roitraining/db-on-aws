# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 11 solution - promote Gold (true branch)
# MAGIC
# MAGIC **ANSWER KEY.** Runs only when the quality gate passed.

# COMMAND ----------

dbutils.widgets.text("catalog", "training_nic")
CATALOG = dbutils.widgets.get("catalog")

dropped = dbutils.jobs.taskValues.get(
    taskKey="read_event_log", key="dropped_records", default=0, debugValue=0)

print(f"Quality gate PASSED with {dropped} dropped record(s). Promoting Gold.")

# COMMAND ----------

gold = f"{CATALOG}.lab10_solution.branch_summary_gold"
try:
    n = spark.table(gold).count()
    print(f"{gold}: {n} row(s) ready for consumers")
    print()
    print("In a real promotion this is where you would grant the analyst group SELECT on")
    print("Gold and nothing else - Lab 10 Task 7 covers publishing only the Gold layer.")
except Exception as e:
    print(f"Gold not available: {str(e)[:200]}")
    print("Deploy and run bundles/solutions/lab-10-pipeline first.")
