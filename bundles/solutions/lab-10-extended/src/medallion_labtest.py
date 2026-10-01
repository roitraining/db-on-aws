from pyspark import pipelines as dp
from pyspark.sql import functions as F

# One rule set, two tables: silver keeps what passes, quarantine keeps what does not.
RULES = {
    "valid_key": "ID_RSSD IS NOT NULL",
    "plausible_city": "CITY IS NULL OR LENGTH(CITY) > 1",
}
QUARANTINE_PREDICATE = " OR ".join(f"NOT({p})" for p in RULES.values())

@dp.table(name="branches_bronze")
def branches_bronze():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load("/Volumes/training_nic/raw/landing/branches/"))

def conformed_bronze():
    return (spark.readStream.table("branches_bronze")
            .withColumnRenamed("#ID_RSSD", "ID_RSSD")
            .withColumn("NM_LGL", F.trim(F.col("NM_LGL")))
            .withColumn("CITY", F.nullif(F.col("CITY"), F.lit(""))))

# Change Data Feed on: every insert/update/delete to this table is queryable via
#   SELECT * FROM table_changes('eng_labtest.work.branches_silver', <version>)
@dp.table(name="branches_silver",
          table_properties={"delta.enableChangeDataFeed": "true"})
@dp.expect("plausible_city", RULES["plausible_city"])
@dp.expect_or_drop("valid_key", RULES["valid_key"])
def branches_silver():
    return conformed_bronze()

# The opposite table: rows violating ANY rule, same predicates inverted.
@dp.table(name="branches_quarantine",
          table_properties={"delta.enableChangeDataFeed": "true"})
def branches_quarantine():
    return conformed_bronze().filter(F.expr(QUARANTINE_PREDICATE))

# In-pipeline change log: streams silver's own Change Data Feed into an append-only audit
# table, so inserts/updates/deletes land here with _change_type on every run.
# startingVersion=2 (CDF enable point) skips replaying the initial snapshot as inserts.
@dp.table(name="silver_changes_feed")
def silver_changes_feed():
    return (spark.readStream
            .option("readChangeFeed", "true")
            .option("startingVersion", 2)
            .table("eng_labtest.work.branches_silver"))

@dp.materialized_view(name="branch_summary_gold")
def branch_summary_gold():
    return (spark.read.table("branches_silver")
            .groupBy("STATE_ABBR_NM")
            .agg(F.count("*").alias("branch_count")))

dp.create_streaming_table(name="institutions_scd")

dp.create_auto_cdc_from_snapshot_flow(
    target="institutions_scd",
    source="training_nic.legacy_onprem.institutions",
    keys=["#ID_RSSD"],
    stored_as_scd_type=2)
