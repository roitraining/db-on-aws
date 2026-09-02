# Databricks notebook source
# MAGIC %md
# MAGIC # Grant attendee access
# MAGIC
# MAGIC **INSTRUCTOR ONLY.** Deployed by `bundles/40-attendees`.
# MAGIC
# MAGIC The schemas themselves are declared in `databricks.yml` as `resources.schemas`. This notebook
# MAGIC only does what a bundle cannot express: grants.
# MAGIC
# MAGIC ## The three privileges
# MAGIC
# MAGIC Reading a table needs `SELECT` on the object, `USE CATALOG` on the catalog, and `USE SCHEMA`
# MAGIC on the schema. Granting `SELECT` alone leaves an attendee stuck with what looks like a bug —
# MAGIC which is exactly the discovery Lab 5 Task 2 and Lab 7 Task 2 are built around. Do not
# MAGIC pre-grant anything on the attendees' *own* schemas beyond ownership, or you remove the lesson.

# COMMAND ----------

dbutils.widgets.text("catalog", "training_nic")
dbutils.widgets.text("attendee_group", "training_attendees")

CATALOG = dbutils.widgets.get("catalog")
GROUP = dbutils.widgets.get("attendee_group")

# Must match the schema list in databricks.yml.
ATTENDEES = ["a01", "a02", "a03", "a04", "a05"]

failures = []


def check(label, condition, detail=""):
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}" + (f"  ({detail})" if detail else ""))
    if not condition:
        failures.append(label)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Confirm the group exists
# MAGIC
# MAGIC Account groups cannot be created by a bundle or from SQL. If this fails, create the group in
# MAGIC the account console and re-run — everything below depends on it.

# COMMAND ----------

group_ok = False
try:
    rows = spark.sql(f"SHOW GROUPS").collect()
    names = [r[0] for r in rows]
    group_ok = GROUP in names
    print(f"  groups visible: {names}")
except Exception as e:
    print(f"  could not list groups from SQL ({str(e)[:100]}); will let the GRANT decide")
    group_ok = True

if not group_ok:
    raise Exception(
        f"Group '{GROUP}' does not exist. Create it in the account console "
        f"(Account console > User management > Groups), add the class, then re-run this job."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Shared read access to the training data
# MAGIC
# MAGIC The class reads `legacy_onprem`, `migrated` and `reference`. They do **not** get write access
# MAGIC to any of them — Labs 4 and 5 have attendees write into their own schema instead.

# COMMAND ----------

print("=== Catalog traversal ===")
spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{GROUP}`")
print(f"  USE CATALOG on {CATALOG} -> {GROUP}")

print("\n=== Read on the shared schemas ===")
for schema in ["legacy_onprem", "migrated", "reference", "raw"]:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO `{GROUP}`")
    spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{schema} TO `{GROUP}`")
    print(f"  USE SCHEMA + SELECT on {CATALOG}.{schema}")

print("\n=== Landing volume (Lab 10 Auto Loader) ===")
try:
    spark.sql(f"GRANT READ VOLUME ON VOLUME {CATALOG}.raw.landing TO `{GROUP}`")
    print(f"  READ VOLUME on {CATALOG}.raw.landing")
except Exception as e:
    print(f"  skipped: {str(e)[:120]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Each attendee owns their own schema
# MAGIC
# MAGIC Ownership rather than a grant list, so an attendee can create, alter and drop inside their own
# MAGIC space without needing anything further from you mid-class.

# COMMAND ----------

print("=== Attendee schema ownership ===")
for a in ATTENDEES:
    schema = f"{CATALOG}.analyst_{a}"
    try:
        spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {schema} TO `{GROUP}`")
        print(f"  ALL PRIVILEGES on {schema} -> {GROUP}")
    except Exception as e:
        print(f"  FAILED on {schema}: {str(e)[:140]}")
        failures.append(schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Verify

# COMMAND ----------

print("=== Schemas exist ===")
existing = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()]
for a in ATTENDEES:
    check(f"analyst_{a} exists", f"analyst_{a}" in existing)

print("\n=== Grants readable back ===")
try:
    grants = spark.sql(f"SHOW GRANTS `{GROUP}` ON CATALOG {CATALOG}").collect()
    check("group holds a grant on the catalog", len(grants) > 0, f"{len(grants)} grant(s)")
except Exception as e:
    print(f"  could not read grants back: {str(e)[:120]}")

print("\n" + "=" * 55)
if failures:
    raise Exception(f"ATTENDEE SETUP INCOMPLETE — {len(failures)} failure(s): {failures}")
print(f"{len(ATTENDEES)} attendee schemas ready, group '{GROUP}' granted read on the training data.")
print()
print("Deliberately NOT granted: cross-attendee access. Labs 5 and 7 have attendees grant")
print("each other access by hand, and discover that SELECT alone is not enough.")
