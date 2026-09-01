# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Setup — Advanced Data Engineering
# MAGIC
# MAGIC **INSTRUCTOR ONLY. Do not distribute to attendees.**
# MAGIC
# MAGIC Run this **after** the Cloud Analytics setup notebook. That notebook builds `training_nic.raw`,
# MAGIC `legacy_onprem`, `migrated` and `reference`, which Labs 7–12 all assume already exist.
# MAGIC
# MAGIC This notebook adds only what the advanced labs need on top:
# MAGIC
# MAGIC | Object | Used by | Why it exists |
# MAGIC |---|---|---|
# MAGIC | `training_nic.perf.institutions_large` | Lab 8 | 2M rows with deliberate 60% `CA` skew |
# MAGIC | `training_nic.perf.financials_large` | Lab 8 | 2M-row join partner carrying `TOT_ASSETS` |
# MAGIC
# MAGIC ## Why a second, larger dataset
# MAGIC
# MAGIC Labs 1–6 run on 4,900 rows, which is the right size to prove a transformation is **correct**.
# MAGIC It is the wrong size to prove anything about **performance**: at that volume the join lands in a
# MAGIC single partition, every task duration is identical, and the skew Lab 8 exists to teach is
# MAGIC invisible in Summary Metrics. Lab 8 therefore translates the stored procedure against the real
# MAGIC migrated tables and then moves to these tables to measure. That switch is itself a taught point
# MAGIC in the lab — do not "simplify" it back to one dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 0 · Configuration

# COMMAND ----------

CATALOG = "training_nic"
PERF_SCHEMA = "perf"
PERF_ROWS = 2_000_000

# Share of rows forced onto a single state key. 60% produces a ~9x max/median partition ratio,
# which is large enough to read off Summary Metrics without making the lab slow to run.
SKEW_PCT = 60

failures = []


def check(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}" + (f"  ({detail})" if detail else ""))
    if not condition:
        failures.append(label)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Prerequisite check
# MAGIC
# MAGIC Fail loudly and early if the Cloud Analytics setup has not been run.

# COMMAND ----------

print("=== Prerequisites from the Cloud Analytics setup ===")
for fq in [
    f"{CATALOG}.migrated.institutions",
    f"{CATALOG}.migrated.financials",
    f"{CATALOG}.legacy_onprem.institutions",
    f"{CATALOG}.reference.state_population",
]:
    try:
        n = spark.table(fq).count()
        check(f"{fq} exists", n > 0, f"{n:,} rows")
    except Exception as e:
        check(f"{fq} exists", False, str(e)[:80])

if failures:
    raise Exception(
        "Run the Cloud Analytics setup notebook first — Labs 7-12 build on its tables. "
        f"Missing: {failures}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Performance-scale tables for Lab 8
# MAGIC
# MAGIC `institutions_large` carries the same column names as the migrated table, including the native
# MAGIC leading `#` on the key, so the Lab 8 join is written exactly the way it would be against real data.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{PERF_SCHEMA}")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{PERF_SCHEMA}.institutions_large AS
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
CREATE OR REPLACE TABLE {CATALOG}.{PERF_SCHEMA}.financials_large AS
SELECT
  id AS `#ID_RSSD`,
  CAST(id % 900000 AS DECIMAL(18,2)) AS TOT_ASSETS
FROM range(1, {PERF_ROWS + 1}) AS t(id)
""")

print(f"Built {PERF_ROWS:,} rows in {CATALOG}.{PERF_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Verify the skew Lab 8 depends on
# MAGIC
# MAGIC Lab 8 quotes specific numbers to attendees. If these checks fail, the lab's Expected Results are
# MAGIC wrong and must be re-measured rather than left to contradict what attendees see.

# COMMAND ----------

from pyspark.sql import functions as F

print("=== Schema contract ===")
inst_cols = spark.table(f"{CATALOG}.{PERF_SCHEMA}.institutions_large").columns
fin_cols = spark.table(f"{CATALOG}.{PERF_SCHEMA}.financials_large").columns
check("institutions_large preserves the '#' key", "#ID_RSSD" in inst_cols, f"cols={inst_cols}")
check("financials_large preserves the '#' key", "#ID_RSSD" in fin_cols, f"cols={fin_cols}")
check("financials_large has TOT_ASSETS", "TOT_ASSETS" in fin_cols)

print("\n=== Skew Lab 8 teaches ===")
dist = (
    spark.table(f"{CATALOG}.{PERF_SCHEMA}.institutions_large")
    .groupBy("STATE_ABBR_NM")
    .count()
    .orderBy(F.desc("count"))
    .collect()
)
total = sum(r["count"] for r in dist)
top = dist[0]
top_pct = 100 * top["count"] / total
for r in dist:
    print(f"    {r['STATE_ABBR_NM']}: {r['count']:>9,}  ({100 * r['count'] / total:.0f}%)")

check("dominant key is CA", top["STATE_ABBR_NM"] == "CA", top["STATE_ABBR_NM"])
check("CA holds ~60% of rows", 55 <= top_pct <= 65, f"{top_pct:.0f}%")
check("seven distinct states", len(dist) == 7, f"{len(dist)} states")

print("\n=== Partition ratio (the straggler, as data) ===")
prev_aqe = spark.conf.get("spark.sql.adaptive.enabled")
spark.conf.set("spark.sql.adaptive.enabled", "false")
try:
    joined = spark.table(f"{CATALOG}.{PERF_SCHEMA}.institutions_large").join(
        spark.table(f"{CATALOG}.{PERF_SCHEMA}.financials_large"), on="#ID_RSSD", how="inner"
    )
    sizes = joined.repartition(F.col("STATE_ABBR_NM")).rdd.glom().map(len).collect()
    nz = [x for x in sizes if x > 0]
    ratio = max(nz) / sorted(nz)[len(nz) // 2]
    print(f"    max={max(nz):,}  median={sorted(nz)[len(nz)//2]:,}  ratio={ratio:.1f}x")
    check("skew ratio is large enough to see (>=5x)", ratio >= 5, f"{ratio:.1f}x")
finally:
    spark.conf.set("spark.sql.adaptive.enabled", prev_aqe)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Result

# COMMAND ----------

print("=" * 55)
if failures:
    raise Exception(f"SETUP INCOMPLETE — {len(failures)} check(s) failed: {failures}")
print("All setup checks passed. Environment is ready for Labs 7-12.")
print()
print("Reminder: Labs 8 and 9 require a CLASSIC cluster — the Spark UI is not")
print("available on serverless. Allow ~6 minutes for a cold cluster to start.")
