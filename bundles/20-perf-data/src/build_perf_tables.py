# Databricks notebook source
# MAGIC %md
# MAGIC # Performance-scale tables for Lab 8
# MAGIC
# MAGIC **INSTRUCTOR ONLY.** Deployed by `bundles/20-perf-data`.
# MAGIC
# MAGIC Lab 8 quotes specific numbers to attendees — a ~9x partition ratio and CA at ~60% of rows.
# MAGIC Part 2 re-measures them after every build. If a check fails, the lab's Expected Results are
# MAGIC wrong and must be re-measured rather than left to contradict what attendees see.

# COMMAND ----------

dbutils.widgets.text("catalog", "training_nic")
dbutils.widgets.text("perf_rows", "2000000")
dbutils.widgets.text("skew_pct", "60")

CATALOG = dbutils.widgets.get("catalog")
PERF_ROWS = int(dbutils.widgets.get("perf_rows"))
SKEW_PCT = int(dbutils.widgets.get("skew_pct"))
SCHEMA = "perf"

failures = []


def check(label, condition, detail=""):
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}" + (f"  ({detail})" if detail else ""))
    if not condition:
        failures.append(label)


print(f"Catalog: {CATALOG}   rows: {PERF_ROWS:,}   skew: {SKEW_PCT}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Build
# MAGIC
# MAGIC Column names match the migrated tables, including the native leading `#` on the key, so the
# MAGIC Lab 8 join is written exactly as it would be against real data.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.institutions_large AS
SELECT
  id AS `#ID_RSSD`,
  CASE
    WHEN id % 100 < {SKEW_PCT} THEN 'CA'
    ELSE element_at(array('TX','NY','FL','IL','OH','WA'), CAST(id % 6 AS INT) + 1)
  END AS STATE_ABBR_NM,
  element_at(array('200','300','400','500'), CAST(id % 4 AS INT) + 1) AS CHTR_TYPE_CD
FROM range(1, {PERF_ROWS + 1}) AS t(id)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.financials_large AS
SELECT
  id AS `#ID_RSSD`,
  CAST(id % 900000 AS DECIMAL(18,2)) AS TOT_ASSETS
FROM range(1, {PERF_ROWS + 1}) AS t(id)
""")

print(f"Built {PERF_ROWS:,} rows in {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Verify the numbers Lab 8 quotes

# COMMAND ----------

from pyspark.sql import functions as F

print("=== Schema contract ===")
inst_cols = spark.table(f"{CATALOG}.{SCHEMA}.institutions_large").columns
fin_cols = spark.table(f"{CATALOG}.{SCHEMA}.financials_large").columns
check("institutions_large preserves the '#' key", "#ID_RSSD" in inst_cols, f"cols={inst_cols}")
check("financials_large preserves the '#' key", "#ID_RSSD" in fin_cols)
check("financials_large has TOT_ASSETS", "TOT_ASSETS" in fin_cols)

print("\n=== Skew distribution ===")
dist = (spark.table(f"{CATALOG}.{SCHEMA}.institutions_large")
        .groupBy("STATE_ABBR_NM").count().orderBy(F.desc("count")).collect())
total = sum(r["count"] for r in dist)
for r in dist:
    print(f"    {r['STATE_ABBR_NM']}: {r['count']:>9,}  ({100 * r['count'] / total:.0f}%)")

top = dist[0]
top_pct = 100 * top["count"] / total
check("dominant key is CA", top["STATE_ABBR_NM"] == "CA", top["STATE_ABBR_NM"])
check(f"CA holds ~{SKEW_PCT}% of rows", SKEW_PCT - 5 <= top_pct <= SKEW_PCT + 5, f"{top_pct:.0f}%")
check("seven distinct states", len(dist) == 7, f"{len(dist)} states")

print("\n=== Partition ratio — the straggler, expressed as data ===")
prev = spark.conf.get("spark.sql.adaptive.enabled")
spark.conf.set("spark.sql.adaptive.enabled", "false")
try:
    joined = (spark.table(f"{CATALOG}.{SCHEMA}.institutions_large")
              .join(spark.table(f"{CATALOG}.{SCHEMA}.financials_large"),
                    on="#ID_RSSD", how="inner"))
    sizes = joined.repartition(F.col("STATE_ABBR_NM")).rdd.glom().map(len).collect()
    nz = [x for x in sizes if x > 0]
    ratio = max(nz) / sorted(nz)[len(nz) // 2]
    print(f"    max={max(nz):,}  median={sorted(nz)[len(nz)//2]:,}  ratio={ratio:.1f}x")
    check("skew ratio large enough to see (>=5x)", ratio >= 5, f"{ratio:.1f}x")
finally:
    spark.conf.set("spark.sql.adaptive.enabled", prev)

print("\n" + "=" * 55)
if failures:
    raise Exception(f"PERF DATA INCOMPLETE — {len(failures)} check(s) failed: {failures}")
print("Perf tables ready. Lab 8 can run against these.")
print()
print("Reminder: Lab 8 needs a CLASSIC cluster — deploy bundles/10-classic-compute.")
print("The Spark UI is not available on serverless.")
