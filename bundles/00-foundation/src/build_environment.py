# Databricks notebook source
# MAGIC %md
# MAGIC # Build the training environment
# MAGIC
# MAGIC **INSTRUCTOR ONLY.** Deployed by `bundles/00-foundation`.
# MAGIC
# MAGIC This notebook is the **authoritative schema contract** for all twelve labs. Every table and
# MAGIC column a lab references is created here. If a lab and this notebook disagree, this notebook
# MAGIC wins and the lab is wrong.
# MAGIC
# MAGIC It generates NIC-shaped data rather than reading uploaded files, so the whole environment is
# MAGIC reproducible from `bundle deploy` with no manual upload step. The schema matches the FFIEC NIC
# MAGIC data dictionary, including the leading `#` on the key.
# MAGIC
# MAGIC It also plants the five deliberate defects Lab 3 exists to find. **Do not show Part 4 to
# MAGIC attendees before Lab 3 is complete.**
# MAGIC
# MAGIC | Decision | Value |
# MAGIC |---|---|
# MAGIC | Native NIC column names | Preserved through raw and Bronze, including the leading `#` |
# MAGIC | Cleaning | Happens at Silver, not on ingest — the true migration shape |
# MAGIC | `legacy_onprem` | Carries genuine SQL Server export artifacts (padding, empty strings) |
# MAGIC | `migrated` | Carries the five deliberate defects |

# COMMAND ----------

dbutils.widgets.text("catalog", "training_nic")
dbutils.widgets.text("managed_location", "")

CATALOG = dbutils.widgets.get("catalog")
MANAGED_LOCATION = dbutils.widgets.get("managed_location").strip()

SCHEMAS = ["raw", "legacy_onprem", "migrated", "reference"]
ROWS = 5000

failures = []


def check(label, condition, detail=""):
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}" + (f"  ({detail})" if detail else ""))
    if not condition:
        failures.append(label)


print(f"Catalog:          {CATALOG}")
print(f"Managed location: {MANAGED_LOCATION or '(metastore default)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Catalog, schemas, volume
# MAGIC
# MAGIC A bundle cannot declare a Unity Catalog catalog, which is why this runs here as a job task
# MAGIC rather than as a `resources.catalogs` entry. On accounts with Default Storage enabled and no
# MAGIC metastore storage root, `CREATE CATALOG` fails unless `MANAGED LOCATION` is supplied — pass
# MAGIC the `managed_location` variable in that case.

# COMMAND ----------

# Check existence before creating, rather than relying on IF NOT EXISTS.
#
# On accounts with Default Storage enabled and no metastore storage root,
# `CREATE CATALOG IF NOT EXISTS` fails *even when the catalog already exists* — it
# validates the storage location before it checks existence. That makes the naive form
# non-idempotent: the job fails on every re-run of an environment that is already fine.
catalog_exists = any(
    r[0] == CATALOG for r in spark.sql("SHOW CATALOGS").collect()
)

if catalog_exists:
    print(f"Catalog {CATALOG} already exists — skipping creation.")
else:
    try:
        if MANAGED_LOCATION:
            spark.sql(f"CREATE CATALOG {CATALOG} MANAGED LOCATION '{MANAGED_LOCATION}'")
            print(f"Created {CATALOG} at {MANAGED_LOCATION}")
        else:
            spark.sql(f"CREATE CATALOG {CATALOG}")
            print(f"Created {CATALOG} using the metastore default location")
    except Exception as e:
        if "Metastore storage root URL does not exist" in str(e):
            raise Exception(
                f"Cannot create catalog '{CATALOG}': this metastore has no storage root "
                f"(Default Storage is enabled on the account). Re-deploy with an explicit "
                f"location, for example:\n\n"
                f"    databricks bundle deploy -t dev \\\n"
                f"      --var=\"managed_location=s3://<bucket>/<prefix>/{CATALOG}\"\n\n"
                f"The path must sit inside an external location you can use."
            ) from e
        raise

for s in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")

print(f"Catalog, {len(SCHEMAS)} schemas and landing volume ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Source of truth — `legacy_onprem`
# MAGIC
# MAGIC This stands in for the on-premises SQL Server export and carries its artifacts: `NM_LGL` is
# MAGIC right-padded to 60 characters, and missing cities are empty strings rather than NULL. Both are
# MAGIC real behaviours of a fixed-width export, and both are what Lab 3 teaches attendees to spot.
# MAGIC
# MAGIC The date span runs 1950 to 2018 so that Lab 2's 1970–1990 parameter range returns rows.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.legacy_onprem.institutions AS
SELECT
  id AS `#ID_RSSD`,
  RPAD(CONCAT('INSTITUTION ', CAST(id AS STRING), ' NATIONAL BANK'), 60, ' ') AS NM_LGL,
  CASE WHEN id % 23 = 0 THEN '' ELSE CONCAT('CITY_', CAST(id % 400 AS STRING)) END AS CITY,
  element_at(array('CA','CA','CA','CA','TX','NY','FL','IL','OH','WA'),
             CAST(id % 10 AS INT) + 1) AS STATE_ABBR_NM,
  CASE WHEN id % 50 = 7 THEN '250'
       ELSE element_at(array('200','300','400','500'), CAST(id % 4 AS INT) + 1) END AS CHTR_TYPE_CD,
  DATE_ADD(DATE'1950-01-01', CAST(id * 5 AS INT)) AS D_DT_START
FROM range(1, {ROWS + 1}) AS t(id)
""")

# TOT_ASSETS lives in a separate FR Y-9C-derived table. The NIC attributes file has no
# financial fields, and keeping them apart mirrors that.
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.legacy_onprem.financials AS
SELECT
  id AS `#ID_RSSD`,
  CAST(ROUND(((id * 7919) % 900000) + 1000 + ((id % 100) / 100.0), 2) AS DECIMAL(18,2)) AS TOT_ASSETS
FROM range(1, {ROWS + 1}) AS t(id)
""")

print(f"legacy_onprem built: {ROWS:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Reference data

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.reference.state_population AS
SELECT * FROM VALUES
  ('CA', 39538223), ('TX', 29145505), ('FL', 21538187), ('NY', 20201249),
  ('IL', 12812508), ('OH', 11799448), ('WA',  7705281)
AS t(state_abbr, population)
""")

print("reference.state_population built")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · The cloud copy and its five defects
# MAGIC
# MAGIC **Do not show this cell to attendees before Lab 3.** It is the answer key.
# MAGIC
# MAGIC | # | Defect | Planted how | Magnitude |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Rows dropped | `CHTR_TYPE_CD = '250'` filtered out | 100 rows (2%) |
# MAGIC | 2 | Decimals truncated | `TOT_ASSETS` cast to integer | ~$2,443 across 4,900 rows |
# MAGIC | 3 | Dates shifted | +1 day on a hash-selected subset | ~705 rows |
# MAGIC | 4 | Empty string became NULL | `''` mapped to NULL on migration | ~212 null vs ~217 empty |
# MAGIC | 5 | Padding stripped | `TRIM` applied on migration | all 5,000 source rows padded |

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.migrated.institutions AS
SELECT
  `#ID_RSSD`,
  TRIM(NM_LGL) AS NM_LGL,                                    -- defect 5
  CASE WHEN CITY = '' THEN NULL ELSE CITY END AS CITY,        -- defect 4
  STATE_ABBR_NM,
  CHTR_TYPE_CD,
  CASE WHEN crc32(CAST(`#ID_RSSD` AS STRING)) % 7 = 0
       THEN DATE_ADD(D_DT_START, 1) ELSE D_DT_START END AS D_DT_START   -- defect 3
FROM {CATALOG}.legacy_onprem.institutions
WHERE CHTR_TYPE_CD <> '250'                                   -- defect 1
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.migrated.financials AS
SELECT
  f.`#ID_RSSD`,
  CAST(CAST(f.TOT_ASSETS AS BIGINT) AS DECIMAL(18,2)) AS TOT_ASSETS   -- defect 2
FROM {CATALOG}.legacy_onprem.financials f
JOIN {CATALOG}.migrated.institutions i ON f.`#ID_RSSD` = i.`#ID_RSSD`
""")

# Lab 3 time-travels to version 0, which needs more than one version to exist.
spark.sql(f"""
INSERT INTO {CATALOG}.migrated.institutions
SELECT * FROM {CATALOG}.migrated.institutions WHERE 1 = 0
""")

print("migrated built with five defects planted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · Verify
# MAGIC
# MAGIC The labs quote these figures to attendees. If a check fails, a lab's Expected Result is now
# MAGIC wrong and must be re-measured rather than left to contradict what attendees see.

# COMMAND ----------

from pyspark.sql import functions as F

print("=== Schema contract ===")
cols = spark.table(f"{CATALOG}.legacy_onprem.institutions").columns
check("native '#' preserved on the key", "#ID_RSSD" in cols, f"cols={cols[:3]}")
check("financials carries TOT_ASSETS",
      "TOT_ASSETS" in spark.table(f"{CATALOG}.migrated.financials").columns)
check("state_population carries state_abbr",
      "state_abbr" in spark.table(f"{CATALOG}.reference.state_population").columns)

print("\n=== Defect magnitudes (Lab 3 answer key) ===")
src = spark.table(f"{CATALOG}.legacy_onprem.institutions").count()
mig = spark.table(f"{CATALOG}.migrated.institutions").count()
check("defect 1 — 100 rows dropped", src - mig == 100, f"{src:,} -> {mig:,}")

dropped_all_250 = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.legacy_onprem.institutions s
    WHERE NOT EXISTS (SELECT 1 FROM {CATALOG}.migrated.institutions m
                      WHERE m.`#ID_RSSD` = s.`#ID_RSSD`)
      AND s.CHTR_TYPE_CD <> '250'
""").collect()[0]["n"]
check("defect 1 — every dropped row is CHTR_TYPE_CD 250", dropped_all_250 == 0,
      f"non-250 dropped={dropped_all_250}")

shifted = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {CATALOG}.migrated.institutions m
    JOIN {CATALOG}.legacy_onprem.institutions s ON m.`#ID_RSSD` = s.`#ID_RSSD`
    WHERE m.D_DT_START <> s.D_DT_START
""").collect()[0]["n"]
check("defect 3 — dates shifted", shifted > 0, f"{shifted} rows, all +1 day")

nulls = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.migrated.institutions "
                  f"WHERE CITY IS NULL").collect()[0]["n"]
empties = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.legacy_onprem.institutions "
                    f"WHERE CITY = ''").collect()[0]["n"]
check("defect 4 — null in cloud, empty at source", nulls > 0 and empties > 0,
      f"{nulls} null / {empties} empty")

padded = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.legacy_onprem.institutions
    WHERE LENGTH(NM_LGL) <> LENGTH(TRIM(NM_LGL))
""").collect()[0]["n"]
check("defect 5 — all source rows padded", padded == ROWS, f"{padded:,} of {ROWS:,}")

print("\n=== Lab 2 date range ===")
in_range = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.migrated.institutions
    WHERE D_DT_START BETWEEN DATE'1970-01-01' AND DATE'1990-12-31'
""").collect()[0]["n"]
check("Lab 2's 1970-1990 range returns rows", in_range > 0, f"{in_range:,} rows")

print("\n=== Lab 3 time travel ===")
versions = spark.sql(f"DESCRIBE HISTORY {CATALOG}.migrated.institutions").count()
check("2+ versions exist for time travel", versions >= 2, f"{versions} versions")

print("\n" + "=" * 55)
if failures:
    raise Exception(f"SETUP INCOMPLETE — {len(failures)} check(s) failed: {failures}")
print("Foundation ready. Labs 1-7, 9 can run against this.")
print()
print("Next, as needed:")
print("  bundles/10-classic-compute  cluster for Labs 4, 8, 9 (Spark UI)")
print("  bundles/20-perf-data        2M-row tables for Lab 8")
print("  bundles/30-landing-data     Auto Loader source files for Lab 10")
print("  bundles/40-attendees        per-attendee schemas and grants")
