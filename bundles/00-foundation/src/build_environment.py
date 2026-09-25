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
  -- weighted charter mix (~44/33/11/11) so per-charter charts have a real shape;
  -- modulus 9 is coprime with the %10 state cycle, keeping charter and state independent
  CASE WHEN id % 50 = 7 THEN '250'
       ELSE element_at(array('200','200','200','200','300','300','300','400','500'),
                       CAST(id % 9 AS INT) + 1) END AS CHTR_TYPE_CD,
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
# MAGIC This is the Lab 3 answer key (also published as `answers/Lab_3_Answers.md`).
# MAGIC
# MAGIC | # | Defect | Planted how | Magnitude |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Rows dropped | `DELETE WHERE CHTR_TYPE_CD = '250'` | 100 rows (2%) |
# MAGIC | 2 | Decimals truncated | `TOT_ASSETS` cast to integer | ~$2,443 across 4,900 rows |
# MAGIC | 3 | Dates shifted | `UPDATE` +1 day on a hash-selected subset | ~705 rows |
# MAGIC | 4 | Empty string became NULL | `UPDATE` mapping `''` to NULL | ~212 null vs ~217 empty |
# MAGIC | 5 | Padding stripped | `UPDATE` applying `TRIM` | all 5,000 source rows padded |
# MAGIC
# MAGIC **The build is deliberately staged, one commit per defect,** so `DESCRIBE HISTORY` on
# MAGIC `migrated.institutions` reads as the migration's audit log and Lab 3 Task 6 can time
# MAGIC travel to **version 0 — the faithful 5,000-row copy** — to explain the Check 1 gap.
# MAGIC Do not collapse these statements back into a single CTAS: the history IS the lesson.
# MAGIC Table and column comments at the end add metadata entries to the history and give
# MAGIC Catalog Explorer (and Genie) real descriptions to work with.

# COMMAND ----------

# Drop first: CREATE OR REPLACE continues an existing table's version numbering, and
# Lab 3 Task 6 depends on VERSION AS OF 0 being the faithful copy. A fresh table also
# restarts the deleted-file retention clock, so time travel works for the next 7 days.
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.migrated.institutions")

# Version 0 — the faithful copy. Lab 3 Task 6 time-travels here: 5,000 rows, no defects.
spark.sql(f"""
CREATE TABLE {CATALOG}.migrated.institutions AS
SELECT `#ID_RSSD`, NM_LGL, CITY, STATE_ABBR_NM, CHTR_TYPE_CD, D_DT_START
FROM {CATALOG}.legacy_onprem.institutions
""")

# Version 1 — defect 1: rows dropped. The DELETE predicate lands in the history's
# operationParameters, so DESCRIBE HISTORY names the filter Check 2 discovers.
spark.sql(f"""
DELETE FROM {CATALOG}.migrated.institutions WHERE CHTR_TYPE_CD = '250'
""")

# Version 2 — defect 5: padding stripped.
spark.sql(f"""
UPDATE {CATALOG}.migrated.institutions SET NM_LGL = TRIM(NM_LGL)
""")

# Version 3 — defect 4: empty string became NULL.
spark.sql(f"""
UPDATE {CATALOG}.migrated.institutions SET CITY = NULL WHERE CITY = ''
""")

# Version 4 — defect 3: dates shifted +1 day on a hash-selected subset.
spark.sql(f"""
UPDATE {CATALOG}.migrated.institutions
SET D_DT_START = DATE_ADD(D_DT_START, 1)
WHERE crc32(CAST(`#ID_RSSD` AS STRING)) % 7 = 0
""")

# Metadata commits — more history entries, and real descriptions for Catalog Explorer.
spark.sql(f"""
COMMENT ON TABLE {CATALOG}.migrated.institutions IS
'Cloud copy of the on-premises NIC institutions table, loaded during the SQL Server
migration. Validated against legacy_onprem in Lab 3; carries deliberate training defects.'
""")
spark.sql(f"""
ALTER TABLE {CATALOG}.migrated.institutions
ALTER COLUMN `#ID_RSSD` COMMENT 'RSSD identifier assigned by the Federal Reserve. Primary key; native NIC name with the leading # preserved.'
""")
spark.sql(f"""
ALTER TABLE {CATALOG}.migrated.institutions
ALTER COLUMN NM_LGL COMMENT 'Legal name of the institution.'
""")
spark.sql(f"""
ALTER TABLE {CATALOG}.migrated.institutions
ALTER COLUMN D_DT_START COMMENT 'Date the institution record became effective.'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.migrated.financials AS
SELECT
  f.`#ID_RSSD`,
  CAST(CAST(f.TOT_ASSETS AS BIGINT) AS DECIMAL(18,2)) AS TOT_ASSETS   -- defect 2
FROM {CATALOG}.legacy_onprem.financials f
JOIN {CATALOG}.migrated.institutions i ON f.`#ID_RSSD` = i.`#ID_RSSD`
""")

print("migrated built with five defects planted across a staged, readable history")

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

print("\n=== Table history (Lab 3 Task 6) ===")
v0 = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.migrated.institutions VERSION AS OF 0"
               ).collect()[0]["n"]
check("history — version 0 is the faithful 5,000-row copy", v0 == ROWS, f"{v0:,} rows at v0")
hist_ops = [r["operation"] for r in
            spark.sql(f"DESCRIBE HISTORY {CATALOG}.migrated.institutions").collect()]
check("history — staged build produced the audit trail", len(hist_ops) >= 8,
      f"{len(hist_ops)} entries: {hist_ops[::-1]}")
check("history — DELETE commit present (defect 1's fingerprint)", "DELETE" in hist_ops)

print("\n=== Lab 5 alert figure ===")
summary_rows = spark.sql(f"""
    SELECT COUNT(*) AS n FROM (
      SELECT CHTR_TYPE_CD, date_trunc('month', CAST(D_DT_START AS DATE))
      FROM {CATALOG}.migrated.institutions
      WHERE STATE_ABBR_NM = 'CA'
      GROUP BY 1, 2)
""").collect()[0]["n"]
check("Lab 5 — institution_summary holds 1,309 rows (alert threshold 1,200)",
      summary_rows == 1309, f"{summary_rows} rows")

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
print("  (classic cluster for Labs 4, 8, 9 is created below in Part 6 — or via bundles/10-classic-compute)")
print("  bundles/20-perf-data        2M-row tables for Lab 8")
print("  bundles/30-landing-data     Auto Loader source files for Lab 10")
print("  bundles/40-attendees        per-attendee schemas and grants")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Classic cluster for Labs 4, 8 and 9 — created, NOT started
# MAGIC
# MAGIC Mirrors `bundles/10-classic-compute/databricks.yml` so browser-only (Path A) setup
# MAGIC produces the cluster too — no CLI needed. The cluster is created and immediately
# MAGIC terminated, so it sits in **Compute** as a Terminated definition costing nothing.
# MAGIC The first attendee attach (or instructor start) cold-starts it in ~6 minutes.
# MAGIC
# MAGIC If a cluster named `db-on-aws · lab cluster` already exists (from a bundle deploy or
# MAGIC an earlier run), this cell leaves it alone.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
CLUSTER_NAME = "db-on-aws · lab cluster"

existing = [c for c in w.clusters.list() if CLUSTER_NAME in (c.cluster_name or "")]
if existing:
    print(f"Classic cluster already exists: '{existing[0].cluster_name}' "
          f"(state: {existing[0].state}) — leaving it alone.")
else:
    # Spec mirrors bundles/10-classic-compute/databricks.yml — keep the two in sync.
    body = {
        "cluster_name": CLUSTER_NAME,
        "spark_version": "16.4.x-scala2.12",
        "node_type_id": "m5d.large",   # plain m5.large is rejected (needs an EBS volume)
        "num_workers": 0,
        "spark_conf": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        "custom_tags": {"ResourceClass": "SingleNode", "course": "db-on-aws"},
        "autotermination_minutes": 30,
        "data_security_mode": "SINGLE_USER",
        "single_user_name": w.current_user.me().user_name,
    }
    try:
        created = w.api_client.do("POST", "/api/2.1/clusters/create", body=body)
        cid = created["cluster_id"]
        # create also starts the cluster — terminate right away so it costs nothing until class.
        w.api_client.do("POST", "/api/2.1/clusters/delete", body={"cluster_id": cid})
        print(f"Classic cluster created and left terminated: '{CLUSTER_NAME}' ({cid})")
        print("It will appear under Compute as Terminated; starting it takes ~6 minutes.")
    except Exception as e:
        # Deliberately NON-FATAL: everything Labs 1-3, 5-7, 10-12 need is already built.
        msg = str(e)
        print("WARNING: could not create the classic cluster — Labs 4, 8 and 9 need one")
        print(f"  API said: {msg[:300]}")
        if "worker environments" in msg:
            print("  This is Databricks FREE EDITION, which is serverless-only: classic")
            print("  compute cannot be created on this account by anyone. The Spark UI")
            print("  sections of Labs 4, 8 and 9 require a standard (paid/trial) workspace.")
        else:
            print("  Your account may restrict cluster creation. Create it by hand if you")
            print("  can (SETUP.md Part 3 step 6), or ask your admin.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · Performance tables — Lab 4's performance section (and Advanced Lab 8)
# MAGIC
# MAGIC 2M-row versions of the migrated tables, same columns including the `#` key, with
# MAGIC deliberate skew (~60% CA). The 4,900-row tables are too small for performance
# MAGIC differences to be visible; these are big enough that the query profile has a story.
# MAGIC
# MAGIC Inline mirror of `bundles/20-perf-data` Part 1 — keep the two in sync. That bundle's
# MAGIC RDD-based partition-ratio verify stays there (the RDD API is unavailable on serverless).

# COMMAND ----------

PERF_ROWS = 2_000_000
SKEW_PCT = 60

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.perf")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.perf.institutions_large AS
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
CREATE OR REPLACE TABLE {CATALOG}.perf.financials_large AS
SELECT
  id AS `#ID_RSSD`,
  CAST(id % 900000 AS DECIMAL(18,2)) AS TOT_ASSETS
FROM range(1, {PERF_ROWS + 1}) AS t(id)
""")

print("\n=== Perf tables (Lab 4 performance section) ===")
n = spark.table(f"{CATALOG}.perf.institutions_large").count()
check("perf — institutions_large row count", n == PERF_ROWS, f"{n:,}")
ca = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.perf.institutions_large "
               f"WHERE STATE_ABBR_NM = 'CA'").collect()[0]["n"]
check(f"perf — CA skew ~{SKEW_PCT}%", abs(100 * ca / n - SKEW_PCT) <= 5, f"{100 * ca / n:.0f}%")

print("\n" + "=" * 55)
if failures:
    raise Exception(f"SETUP INCOMPLETE — {len(failures)} check(s) failed: {failures}")
print("Everything built. Labs 1-6 can run against this environment.")
