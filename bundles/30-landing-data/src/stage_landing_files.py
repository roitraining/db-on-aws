# Databricks notebook source
# MAGIC %md
# MAGIC # Stage the Auto Loader landing files for Lab 10
# MAGIC
# MAGIC **INSTRUCTOR ONLY.** Deployed by `bundles/30-landing-data`.
# MAGIC
# MAGIC Lab 10 ingests these with `cloudFiles` from `/Volumes/<catalog>/raw/landing/branches/`.
# MAGIC
# MAGIC ## Why the header keeps its `#`
# MAGIC
# MAGIC The key column is written as `#ID_RSSD`, exactly as NIC publishes it. Spark's CSV reader
# MAGIC leaves it alone because the `comment` option defaults to `\0` — comment handling is off, so a
# MAGIC leading `#` on the header line is not treated as a comment. Preserving it is deliberate:
# MAGIC attendees meet the real column name in Bronze and rename it at Silver, which is where cleaning
# MAGIC belongs. If you "helpfully" strip the `#` here, Lab 10's Silver rename step teaches nothing.

# COMMAND ----------

dbutils.widgets.text("catalog", "training_nic")
dbutils.widgets.text("file_count", "4")
dbutils.widgets.text("rows_per_file", "500")

CATALOG = dbutils.widgets.get("catalog")
FILE_COUNT = int(dbutils.widgets.get("file_count"))
ROWS_PER_FILE = int(dbutils.widgets.get("rows_per_file"))

LANDING = f"/Volumes/{CATALOG}/raw/landing"
BRANCHES = f"{LANDING}/branches"

failures = []


def check(label, condition, detail=""):
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}" + (f"  ({detail})" if detail else ""))
    if not condition:
        failures.append(label)


print(f"Landing: {BRANCHES}")
print(f"Staging: {FILE_COUNT} file(s) x {ROWS_PER_FILE} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Write the CSV parts
# MAGIC
# MAGIC Each part carries a distinct `#ID_RSSD` range so attendees can tell which file a row came
# MAGIC from. A handful of rows carry a non-numeric key and a blank city, which is what Lab 10's
# MAGIC `expect_or_drop` and `expect_or_fail` expectations act on.

# COMMAND ----------

import os

os.makedirs(BRANCHES, exist_ok=True)

STATES = ["CA", "CA", "TX", "NY", "FL", "IL", "OH", "WA"]

written = []
for part in range(1, FILE_COUNT + 1):
    start = (part - 1) * ROWS_PER_FILE + 1
    lines = ["#ID_RSSD,NM_LGL,CITY,STATE_ABBR_NM,CHTR_TYPE_CD,D_DT_START"]
    for i in range(start, start + ROWS_PER_FILE):
        # Padding and empty-string city are genuine SQL Server export artifacts, kept so
        # Silver has something real to clean.
        name = f"BRANCH {i} NATIONAL BANK".ljust(60)
        city = "" if i % 23 == 0 else f"CITY_{i % 400}"
        state = STATES[i % len(STATES)]
        chtr = ["200", "300", "400", "500"][i % 4]
        year = 1950 + (i % 70)
        key = f"X{i}" if i % 499 == 0 else str(i)   # a few bad keys for the expectations
        lines.append(f"{key},{name},{city},{state},{chtr},{year}-01-01")

    path = f"{BRANCHES}/branches_{part:03d}.csv"
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    written.append(path)
    print(f"  wrote {path}  ({ROWS_PER_FILE} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Verify Lab 10 can actually read these

# COMMAND ----------

print("=== Files present ===")
found = [f for f in os.listdir(BRANCHES) if f.lower().endswith(".csv")]
check("CSV files staged", len(found) == FILE_COUNT, f"{len(found)} file(s): {sorted(found)}")

print("\n=== Header survives with its '#' ===")
with open(written[0]) as fh:
    header = fh.readline().strip()
print(f"    {header}")
check("header keeps the leading '#'", header.startswith("#ID_RSSD"), header.split(",")[0])

print("\n=== Spark reads it the way Lab 10's Bronze will ===")
df = (spark.read.format("csv")
      .option("header", "true")
      .load(BRANCHES))
cols = df.columns
print(f"    columns: {cols}")
check("Spark preserves '#ID_RSSD' as a column", "#ID_RSSD" in cols, f"got {cols[0]}")
check("row count matches what was written",
      df.count() == FILE_COUNT * ROWS_PER_FILE,
      f"{df.count():,} rows")

print("\n=== Rows the Lab 10 expectations will act on ===")
bad_keys = df.filter("`#ID_RSSD` NOT RLIKE '^[0-9]+$'").count()
blank_cities = df.filter("CITY IS NULL OR CITY = ''").count()
check("some non-numeric keys present (expect_or_fail)", bad_keys > 0, f"{bad_keys} rows")
check("some blank cities present (Silver nullif)", blank_cities > 0, f"{blank_cities} rows")

print("\n" + "=" * 55)
if failures:
    raise Exception(f"LANDING DATA INCOMPLETE — {len(failures)} check(s) failed: {failures}")
print("Landing files ready. Lab 10's Auto Loader ingest can run.")
print()
print("To demonstrate incremental ingest mid-lab, re-run this job with a higher")
print("file_count — Auto Loader will pick up only the new parts.")
