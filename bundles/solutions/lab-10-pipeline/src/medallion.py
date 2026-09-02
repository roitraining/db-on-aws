# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 10 solution — medallion pipeline
# MAGIC
# MAGIC **ANSWER KEY.** Attendees write this themselves in Lab 10.
# MAGIC
# MAGIC Bronze ingests the landing CSVs with Auto Loader, Silver conforms the native NIC names and
# MAGIC gates on expectations, Gold aggregates. AUTO CDC builds an SCD Type 2 history from the
# MAGIC on-premises snapshot.
# MAGIC
# MAGIC ## Naming
# MAGIC
# MAGIC `from pyspark import pipelines as dp` replaced `import dlt`. The decorators moved with it:
# MAGIC `@dp.materialized_view` replaced `@dp.table` for materialized views, and `@dp.temporary_view`
# MAGIC replaced `@dp.view`. Legacy `@dlt` code still runs — recognise it, do not lead with it.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "/Volumes/training_nic/raw/landing/branches"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — faithful landing
# MAGIC
# MAGIC No renaming, no trimming, no coercion. The leading `#` on the key is preserved exactly as NIC
# MAGIC publishes it, because Bronze's job is to record what arrived, not to improve it.
# MAGIC
# MAGIC `cloudFiles.format` is required. `cloudFiles.schemaLocation` is required when you run Auto
# MAGIC Loader **standalone**, but not here — inside a pipeline the schema location is managed for
# MAGIC you, which is one of the things you gave up direct control of in exchange for lifecycle
# MAGIC management. `rescuedDataColumn` is on by default and is deliberately **not**
# MAGIC `cloudFiles`-prefixed; you can see it arrive as `_rescued_data`.

# COMMAND ----------


@dp.table(
    name="branches_bronze",
    comment="Raw NIC branch records exactly as landed. Native column names preserved.",
)
def branches_bronze():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(LANDING))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — conform and gate
# MAGIC
# MAGIC This is where cleaning belongs. The `#` comes off the key, padding comes off the name, and
# MAGIC empty-string cities become NULL.
# MAGIC
# MAGIC The two expectations encode different business positions. `expect_or_drop` says a bad row is
# MAGIC worse than a missing one. `expect_or_fail` says publishing anything wrong is unacceptable and
# MAGIC stops the update. Neither is a default — each is a decision.
# MAGIC
# MAGIC **Measured on the staged landing data:** `valid_key` drops **nothing**, because the four bad
# MAGIC keys (`X499`, `X998`, …) are non-numeric but not NULL, and `IS NOT NULL` does not test for
# MAGIC numeric-ness. All 2,000 Bronze rows reach Silver. That is not a bug in the expectation — it is
# MAGIC the expectation doing exactly what it says.
# MAGIC
# MAGIC Swap in the `expect_or_fail("key_is_numeric", "ID_RSSD RLIKE '^[0-9]+$'")` form from Lab 10
# MAGIC step 10 and this pipeline **stops** on those four rows. Running it both ways is the fastest
# MAGIC way to show that the predicate and the action are two separate choices, and that a badly
# MAGIC chosen predicate silently protects nothing.

# COMMAND ----------


@dp.table(
    name="branches_silver",
    comment="Conformed branch records. NIC names cleaned, quality gated.",
)
@dp.expect_or_drop("valid_key", "ID_RSSD IS NOT NULL")
@dp.expect("city_present", "CITY IS NOT NULL")
def branches_silver():
    return (spark.readStream.table("branches_bronze")
            .withColumnRenamed("#ID_RSSD", "ID_RSSD")
            .withColumn("NM_LGL", F.trim(F.col("NM_LGL")))
            .withColumn("CITY", F.nullif(F.col("CITY"), F.lit("")))
            .withColumn("D_DT_START", F.col("D_DT_START").cast("date")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — business-ready
# MAGIC
# MAGIC A materialized view rather than a streaming table: this is an aggregate that gets read far
# MAGIC more often than it changes, which is exactly the trade a materialized view is for.

# COMMAND ----------


@dp.materialized_view(
    name="branch_summary_gold",
    comment="Branch counts by state. The layer analysts are granted access to.",
)
def branch_summary_gold():
    return (spark.read.table("branches_silver")
            .groupBy("STATE_ABBR_NM")
            .agg(F.count("*").alias("branch_count"),
                 F.countDistinct("CITY").alias("distinct_cities")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## AUTO CDC — SCD Type 2 from a snapshot
# MAGIC
# MAGIC The SQL Server source has no change data feed, so snapshot comparison is the right variant.
# MAGIC It is Python-only.
# MAGIC
# MAGIC No `SEQUENCE BY` here: snapshot comparison derives ordering from the snapshots themselves.
# MAGIC The streaming `AUTO CDC INTO` form in SQL *does* require it, and also requires a
# MAGIC `CREATE FLOW <name> AS` wrapper — without which it fails with
# MAGIC `Missing clause CREATE FLOW for operation AUTO CDC`.
# MAGIC
# MAGIC Legacy names, still runnable: `apply_changes_from_snapshot()` and `APPLY CHANGES INTO`.

# COMMAND ----------

dp.create_streaming_table(
    "institutions_scd",
    comment="SCD Type 2 history of institutions, built by comparing snapshots.",
)

dp.create_auto_cdc_from_snapshot_flow(
    target="institutions_scd",
    source="training_nic.legacy_onprem.institutions",
    keys=["#ID_RSSD"],
    stored_as_scd_type=2)
