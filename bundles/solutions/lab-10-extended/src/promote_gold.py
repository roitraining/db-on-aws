# Databricks notebook source
# Promotion = publication, not recomputation. The pipeline task upstream already refreshed Gold.
dropped = dbutils.jobs.taskValues.get(
    taskKey="read_event_log", key="dropped_records", default=0, debugValue=0)
print(f"Quality gate PASSED with {dropped} dropped record(s). Promoting Gold.")
n = spark.table("eng_labtest.work.branch_summary_gold").count()
print(f"branch_summary_gold: {n} row(s) ready for consumers")
