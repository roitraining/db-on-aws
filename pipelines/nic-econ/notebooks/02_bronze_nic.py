# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: NIC
# MAGIC Loads the FFIEC NIC CSVs from the landing volume as-is (all strings), one table per file type.
# MAGIC Each load is stamped with a snapshot date and **replaces only that snapshot**, so re-downloading
# MAGIC NIC monthly builds up history you can diff later (address moves, charter changes).
# MAGIC
# MAGIC Expected files in `/Volumes/<catalog>/bronze/landing/nic/` (unzipped from the NIC download page):
# MAGIC `CSV_ATTRIBUTES_ACTIVE`, `CSV_ATTRIBUTES_CLOSED`, `CSV_ATTRIBUTES_BRANCHES`, `CSV_RELATIONSHIPS`, `CSV_TRANSFORMATIONS`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("nic_snapshot_date", str(datetime.date.today()), "NIC snapshot date")
SNAP = dbutils.widgets.get("nic_snapshot_date")

NIC_FILES = {
    "nic_attributes_active": "*ATTRIBUTES_ACTIVE*",
    "nic_attributes_closed": "*ATTRIBUTES_CLOSED*",
    "nic_attributes_branches": "*ATTRIBUTES_BRANCHES*",
    "nic_relationships": "*RELATIONSHIPS*",
    "nic_transformations": "*TRANSFORMATIONS*",
}


def load_nic(table, pattern):
    def _run():
        df = (spark.read.option("header", True).option("inferSchema", False)
              .csv(f"{LANDING}/nic/{pattern}")
              .select("*", F.col("_metadata.file_path").alias("source_file_path")))
        df = clean_columns(df).withColumn("_snapshot_date", F.lit(SNAP).cast("date"))
        write_bronze(df, table, "FFIEC NIC", replace_where=f"_snapshot_date = DATE'{SNAP}'")
    return _run


run_steps([(t, load_nic(t, p)) for t, p in NIC_FILES.items()])

# COMMAND ----------

# MAGIC %md ## Profile: check column names against `NIC_COLS` in 00_config

# COMMAND ----------

for t in ["nic_attributes_active", "nic_attributes_branches", "nic_transformations"]:
    print(t, "->", spark.table(tbl("bronze", t)).columns)

missing = [c for c in NIC_COLS.values() if c not in spark.table(tbl("bronze", "nic_attributes_active")).columns]
print("NIC_COLS missing from attributes file:", missing or "none")

display(spark.table(tbl("bronze", "nic_attributes_active"))
        .where(F.col("_snapshot_date") == F.lit(SNAP).cast("date"))
        .groupBy(NIC_COLS["entity_type"]).count().orderBy(F.desc("count")))
