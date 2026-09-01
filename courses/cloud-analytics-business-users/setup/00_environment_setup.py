# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Setup — Cloud Analytics for Business Users
# MAGIC
# MAGIC **INSTRUCTOR ONLY. Do not distribute to attendees.**
# MAGIC
# MAGIC This notebook is the **authoritative schema contract** for Labs 1–6. Every table and column
# MAGIC referenced by a lab guide is created here. If a lab and this notebook disagree, this notebook wins
# MAGIC and the lab is wrong.
# MAGIC
# MAGIC It also plants the five deliberate defects that Lab 3 exists to find. Part 5 must never be shown
# MAGIC to attendees before Lab 3 is complete.
# MAGIC
# MAGIC ## Design decisions encoded here
# MAGIC
# MAGIC | Decision | Value |
# MAGIC |---|---|
# MAGIC | Native NIC column names | **Preserved through raw and Bronze**, including the leading `#` on the key |
# MAGIC | Cleaning | Happens at Silver, not on ingest — this is the true migration shape |
# MAGIC | Source files | **Pinned snapshot** uploaded to the landing volume; never downloaded live before a delivery |
# MAGIC | Source of truth | `legacy_onprem` carries genuine SQL Server export artifacts |
# MAGIC | Cloud copy | `migrated` carries five deliberate defects |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 0 · Configuration
# MAGIC
# MAGIC Set the attendee list before running. Everything else is derived.

# COMMAND ----------

CATALOG = "training_nic"
SCHEMAS = ["raw", "legacy_onprem", "migrated", "reference"]

# One schema per attendee. Use short stable identifiers, not initials — initials collide.
ATTENDEES = ["a01", "a02", "a03", "a04", "a05"]

LANDING = f"/Volumes/{CATALOG}/raw/landing"

# The pinned snapshot filenames expected in the landing volume.
EXPECTED_FILES = ["attributes.csv", "financials.csv", "state_population.csv"]

print(f"Catalog:   {CATALOG}")
print(f"Landing:   {LANDING}")
print(f"Attendees: {len(ATTENDEES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Catalog, Schemas, and Volume

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

for s in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")

print(f"Upload the pinned NIC snapshot to: {LANDING}")
print("Expected files:", ", ".join(EXPECTED_FILES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Inspect What Actually Landed
# MAGIC
# MAGIC **Do not skip this.** NIC column names drift between file versions and several files carry a
# MAGIC leading `#` on the first header. Every downstream cell and every lab depends on the real names.
# MAGIC
# MAGIC The CSV reader's `comment` option defaults to `\0`, which disables comment skipping, so a header
# MAGIC beginning with `#` is read as a normal column name rather than discarded. We set it explicitly
# MAGIC anyway so the behaviour does not depend on a default.

# COMMAND ----------

import os

found = [f for f in os.listdir(LANDING) if f.lower().endswith(".csv")]
missing = [f for f in EXPECTED_FILES if f not in found]

print("Found in landing:", found)
if missing:
    raise Exception(f"STOP — missing expected files: {missing}. Upload the pinned snapshot first.")

for f in found:
    df = (spark.read.format("csv")
          .option("header", True)
          .option("comment", "\0")      # explicit: do NOT treat '#' as a comment
          .load(f"{LANDING}/{f}"))
    print(f"\n=== {f}  ({df.count():,} rows) ===")
    print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confirm the hash-prefixed key survived
# MAGIC
# MAGIC If this assertion fails, the labs' backtick-quoting steps will not make sense and the guides
# MAGIC must be adjusted to whatever name actually arrived.

# COMMAND ----------

attrs_raw = (spark.read.format("csv")
             .option("header", True)
             .option("comment", "\0")
             .load(f"{LANDING}/attributes.csv"))

key_col = [c for c in attrs_raw.columns if c.upper().endswith("ID_RSSD")][0]
print(f"Key column as read: {key_col!r}")

if key_col.startswith("#"):
    print("OK — hash-prefixed key preserved. Labs must backtick-quote this column.")
else:
    print("WARNING — no leading hash. Labs referencing `#ID_RSSD` need updating to:", key_col)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Raw / Bronze — native names preserved
# MAGIC
# MAGIC Bronze is a faithful landing of the source. No renaming, no trimming, no type coercion.
# MAGIC Everything stays as it arrived, including the `#` on the key. Cleaning happens at Silver,
# MAGIC which is exactly the pattern the Advanced course builds on.

# COMMAND ----------

for name in ["attributes", "financials", "state_population"]:
    df = (spark.read.format("csv")
          .option("header", True)
          .option("comment", "\0")
          .option("inferSchema", True)
          .load(f"{LANDING}/{name}.csv"))
    df.write.mode("overwrite").saveAsTable(f"{CATALOG}.raw.{name}")
    print(f"{CATALOG}.raw.{name}: {df.count():,} rows, {len(df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the Delta write kept the hash
# MAGIC
# MAGIC Delta rejects a known set of characters in column names — ` ,;{}()\n\t=` — but `#` is not among
# MAGIC them. This cell proves it rather than assuming it, because the whole backtick lesson depends on it.

# COMMAND ----------

cols = spark.table(f"{CATALOG}.raw.attributes").columns
hash_cols = [c for c in cols if c.startswith("#")]
print("Columns with a leading hash in Delta:", hash_cols)
assert hash_cols, "Delta write dropped or renamed the hash-prefixed column — investigate before continuing."

# Demonstrate the backtick requirement — this is the construct attendees learn in Lab 2.
spark.sql(f"SELECT `{hash_cols[0]}` FROM {CATALOG}.raw.attributes LIMIT 5").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Build the On-Premises Source of Truth
# MAGIC
# MAGIC `legacy_onprem` stands in for what came out of SQL Server. It carries the artifacts a real
# MAGIC fixed-width export produces:
# MAGIC
# MAGIC - `NM_LGL` padded to a fixed width with trailing spaces (`CHAR(n)` behaviour)
# MAGIC - `CITY` empty string rather than null where the value is absent
# MAGIC
# MAGIC These are **not** defects. They are faithful source behaviour, and they are what makes the naive
# MAGIC row-level comparison in Lab 3 fail before attendees learn to normalise.
# MAGIC
# MAGIC > If the RDS federation path is in use, this schema is the fallback only. Load RDS from the
# MAGIC > **same pinned snapshot** so both sides match by construction.

# COMMAND ----------

from pyspark.sql import functions as F

KEY = hash_cols[0]          # e.g. '#ID_RSSD'

attrs = spark.table(f"{CATALOG}.raw.attributes")

onprem = (attrs
          .withColumn("NM_LGL", F.rpad(F.col("NM_LGL"), 100, " "))            # CHAR padding
          .withColumn("CITY", F.coalesce(F.col("CITY"), F.lit("")))           # empty string, not null
          )
onprem.write.mode("overwrite").saveAsTable(f"{CATALOG}.legacy_onprem.institutions")

fin = spark.table(f"{CATALOG}.raw.financials")
fin.write.mode("overwrite").saveAsTable(f"{CATALOG}.legacy_onprem.financials")

print(f"legacy_onprem.institutions: {onprem.count():,} rows")
print(f"legacy_onprem.financials:   {fin.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · Build the Cloud Copy, With Its Five Defects
# MAGIC
# MAGIC **INSTRUCTOR ONLY — this cell is the Lab 3 answer key in executable form.**
# MAGIC
# MAGIC | # | Defect | Mechanism | Found by |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Dropped rows | Filter excludes one `CHTR_TYPE_CD` | Checks 1 and 2 |
# MAGIC | 2 | Truncated decimals | `TOT_ASSETS` cast to integer | Check 3 |
# MAGIC | 3 | Shifted dates | Subset of `D_DT_START` moved +1 day | Check 4 |
# MAGIC | 4 | Null vs empty string | `CITY` empty strings become null | Check 3 |
# MAGIC | 5 | Trailing whitespace | `NM_LGL` trimmed in cloud, padded on-prem | Check 4 — hits first |

# COMMAND ----------

DROPPED_CHARTER_CODE = "250"     # the charter type the migration job wrongly filtered out
SHIFTED_DATE_MODULO  = 7         # ~1 row in 7 gets a one-day shift

src = spark.table(f"{CATALOG}.legacy_onprem.institutions")

migrated = (src
            # Defect 1 — dropped rows, all sharing one charter type
            .filter(F.col("CHTR_TYPE_CD").cast("string") != F.lit(DROPPED_CHARTER_CODE))
            # Defect 5 — cloud trimmed the padding the source still carries
            .withColumn("NM_LGL", F.trim(F.col("NM_LGL")))
            # Defect 4 — empty strings became nulls
            .withColumn("CITY", F.when(F.col("CITY") == "", None).otherwise(F.col("CITY")))
            # Defect 3 — a subset of dates shifted forward one day
            .withColumn(
                "D_DT_START",
                F.when(F.crc32(F.col(KEY).cast("string")) % SHIFTED_DATE_MODULO == 0,
                       F.date_add(F.col("D_DT_START").cast("date"), 1))
                 .otherwise(F.col("D_DT_START").cast("date"))
            ))

migrated.write.mode("overwrite").saveAsTable(f"{CATALOG}.migrated.institutions")

# Defect 2 — decimals truncated by an integer cast during migration
fin_migrated = (spark.table(f"{CATALOG}.legacy_onprem.financials")
                .withColumn("TOT_ASSETS", F.col("TOT_ASSETS").cast("bigint")))
fin_migrated.write.mode("overwrite").saveAsTable(f"{CATALOG}.migrated.financials")

print(f"migrated.institutions: {migrated.count():,} rows "
      f"({src.count() - migrated.count():,} dropped)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a second version so time travel has something to show
# MAGIC
# MAGIC Lab 3 queries an earlier version. Time travel is blocked beyond the deleted-file retention
# MAGIC window — 7 days by default — so the version being queried must be created during the class,
# MAGIC not weeks earlier. This second write guarantees a version 1 exists on the day.

# COMMAND ----------

(spark.table(f"{CATALOG}.migrated.institutions")
 .write.mode("overwrite").saveAsTable(f"{CATALOG}.migrated.institutions"))

spark.sql(f"DESCRIBE HISTORY {CATALOG}.migrated.institutions").select(
    "version", "timestamp", "operation").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Reference Data — curated, clean names
# MAGIC
# MAGIC Reference tables are ours, so they get clean names. This is deliberate: the Lab 2 join puts a
# MAGIC native NIC column next to a curated one, which is the normal state of affairs and the reason
# MAGIC a Silver layer exists.

# COMMAND ----------

(spark.table(f"{CATALOG}.raw.state_population")
 .withColumnRenamed("STATE_ABBR_NM", "state_abbr")
 .withColumnRenamed("POPULATION", "population")
 .write.mode("overwrite").saveAsTable(f"{CATALOG}.reference.state_population"))

spark.table(f"{CATALOG}.reference.state_population").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · Per-Attendee Schemas and Grants
# MAGIC
# MAGIC Reading a table needs three privileges, not one — `SELECT` on the table, `USE CATALOG` on the
# MAGIC catalog, and `USE SCHEMA` on the schema. Granting only `SELECT` produces an access-denied error
# MAGIC that looks like a bug. Lab 5 has attendees discover this themselves.

# COMMAND ----------

GROUP = "training_attendees"     # create this group in the account console first

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{GROUP}`")

for s in ["migrated", "legacy_onprem", "reference", "raw"]:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{s} TO `{GROUP}`")
    spark.sql(f"GRANT SELECT     ON SCHEMA {CATALOG}.{s} TO `{GROUP}`")

spark.sql(f"GRANT READ VOLUME ON VOLUME {CATALOG}.raw.landing TO `{GROUP}`")

for a in ATTENDEES:
    schema = f"{CATALOG}.analyst_{a}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA {schema} TO `{GROUP}`")
    print(f"created {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8 · Verification — assert the contract the labs depend on
# MAGIC
# MAGIC Run this before every delivery. If any assertion fails, a lab will fail in the room.

# COMMAND ----------

failures = []

def check(label, condition, detail=""):
    if condition:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}  {detail}")
        failures.append(label)

print("=== Schema contract ===")
attrs_cols = spark.table(f"{CATALOG}.migrated.institutions").columns
for c in ["NM_LGL", "CITY", "STATE_ABBR_NM", "CHTR_TYPE_CD", "D_DT_START"]:
    check(f"migrated.institutions has {c}", c in attrs_cols)
check("migrated.institutions has hash-prefixed key",
      any(x.startswith("#") for x in attrs_cols), f"cols={attrs_cols[:5]}")
check("migrated.financials has TOT_ASSETS",
      "TOT_ASSETS" in spark.table(f"{CATALOG}.migrated.financials").columns)
check("reference.state_population has state_abbr",
      "state_abbr" in spark.table(f"{CATALOG}.reference.state_population").columns)

print("\n=== Defects present ===")
src_n = spark.table(f"{CATALOG}.legacy_onprem.institutions").count()
mig_n = spark.table(f"{CATALOG}.migrated.institutions").count()
check("defect 1 — rows dropped", mig_n < src_n, f"src={src_n:,} mig={mig_n:,}")

pad = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.legacy_onprem.institutions
    WHERE LENGTH(NM_LGL) <> LENGTH(TRIM(NM_LGL))
""").collect()[0]["n"]
check("defect 5 — source padding present", pad > 0, f"padded rows={pad:,}")

nulls = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.migrated.institutions WHERE CITY IS NULL
""").collect()[0]["n"]
check("defect 4 — nulls in cloud CITY", nulls > 0, f"nulls={nulls:,}")

print("\n=== Time travel ===")
versions = spark.sql(f"DESCRIBE HISTORY {CATALOG}.migrated.institutions").count()
check("at least 2 versions exist for time travel", versions >= 2, f"versions={versions}")

print("\n" + "=" * 50)
if failures:
    raise Exception(f"SETUP INCOMPLETE — {len(failures)} check(s) failed: {failures}")
print("All setup checks passed. Environment is ready for Labs 1-6.")
