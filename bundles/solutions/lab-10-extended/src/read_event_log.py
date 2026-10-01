# Databricks notebook source
dropped = spark.sql("""
    SELECT SUM(CAST(details:flow_progress.data_quality.dropped_records AS BIGINT)) AS dropped
    FROM event_log(TABLE(eng_labtest.work.branches_silver))
    WHERE event_type = 'flow_progress'
""").collect()[0]["dropped"] or 0
print(f"dropped records: {dropped}")
dbutils.jobs.taskValues.set(key="dropped_records", value=int(dropped))
