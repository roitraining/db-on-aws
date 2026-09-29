# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Pipeline audit
# MAGIC Final DAG node: row counts for every table in every layer, appended to
# MAGIC `gold._pipeline_run_summary` so each run's shape is queryable over time.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

run_ts = datetime.datetime.now().isoformat()
rows = []
for layer in ["bronze", "silver", "gold"]:
    for t in spark.catalog.listTables(f"{CATALOG}.{layer}"):
        if t.name.startswith("_"):
            continue
        try:
            n = spark.table(f"{CATALOG}.{layer}.{t.name}").count()
        except Exception as e:
            n = -1
        rows.append((run_ts, layer, t.name, n))

summary = spark.createDataFrame(rows, "run_ts string, layer string, table_name string, row_count long")
summary.write.mode("append").saveAsTable(tbl("gold", "_pipeline_run_summary"))

display(summary.orderBy("layer", "table_name"))
totals = {r["layer"]: (r["tables"], r["total_rows"]) for r in
          summary.groupBy("layer").agg(F.count("*").alias("tables"), F.sum("row_count").alias("total_rows")).collect()}
print("PIPELINE COMPLETE:", " | ".join(f"{k}: {v[0]} tables, {v[1]:,} rows" for k, v in sorted(totals.items())))
empty = [r for r in rows if r[3] == 0]
if empty:
    print("Empty tables (expected for pending sources):", [r[2] for r in empty])
