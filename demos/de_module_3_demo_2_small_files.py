# Databricks notebook source
# MAGIC %md
# MAGIC # DE Module 3 · Demo 2: The Small-File Problem, Measured
# MAGIC
# MAGIC **Seven minutes.** The file-count reduction is the observable result — this notebook produces
# MAGIC real numbers, not an assertion. It fragments a table with many tiny incremental loads, counts
# MAGIC files, runs `OPTIMIZE`, and counts again.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0 — fragment a table the way real incremental loads do
# MAGIC Twenty small appends = twenty-plus small files. *"Every incremental load added a few.
# MAGIC Look how many."*

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS training_nic.eng_demo")
spark.sql("DROP TABLE IF EXISTS training_nic.eng_demo.fragmented_loads")

src = spark.table("training_nic.migrated.institutions").limit(200)
for i in range(20):
    (src.withColumn("load_batch", F.lit(i))
        .write.mode("append").saveAsTable("training_nic.eng_demo.fragmented_loads"))
print("20 incremental loads complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — count the files before optimising

# COMMAND ----------

before = spark.sql("DESCRIBE DETAIL training_nic.eng_demo.fragmented_loads").select("numFiles", "sizeInBytes").first()
print(f"BEFORE:  {before['numFiles']} files, {before['sizeInBytes']:,} bytes")

# COMMAND ----------

# Same evidence per-file, via the hidden _metadata column:
display(spark.table("training_nic.eng_demo.fragmented_loads")
        .select("_metadata.file_path", "_metadata.file_size").distinct()
        .orderBy("file_path").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — OPTIMIZE

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE training_nic.eng_demo.fragmented_loads;

# COMMAND ----------

# MAGIC %md
# MAGIC The OPTIMIZE output above has the receipts: `numFilesAdded` / `numFilesRemoved` in the
# MAGIC metrics column. Now the after-count:

# COMMAND ----------

after = spark.sql("DESCRIBE DETAIL training_nic.eng_demo.fragmented_loads").select("numFiles", "sizeInBytes").first()
print(f"BEFORE:  {before['numFiles']} files")
print(f"AFTER:   {after['numFiles']} file(s) — same data, far fewer files, faster reads")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2b — who cleans up the old files? VACUUM — and ask before you act.
# MAGIC OPTIMIZE rewrote 20 files into 1, but the 20 are still in storage (time travel reads
# MAGIC them). `DRY RUN` lists what a real VACUUM would delete — here, **nothing**: every file
# MAGIC is younger than the 7-day retention, and VACUUM refuses to touch the window.
# MAGIC *"VACUUM is irreversible; OPTIMIZE never is."* (Verified 2026-09-30.)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM training_nic.eng_demo.fragmented_loads DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — the modern default: Liquid Clustering, not manual partitions
# MAGIC
# MAGIC Contrast for the room: naming cluster keys and letting **Liquid Clustering** re-cluster
# MAGIC incrementally, versus choosing a partition column you cannot change later.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE training_nic.eng_demo.fragmented_loads CLUSTER BY (STATE_ABBR_NM);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The clustering columns are now table metadata; future OPTIMIZE runs re-cluster incrementally.
# MAGIC DESCRIBE DETAIL training_nic.eng_demo.fragmented_loads;

# COMMAND ----------

# MAGIC %md
# MAGIC **Transition:** *"You can store, version, and compact a migrated table. Tomorrow we turn the
# MAGIC load itself into a governed, incremental pipeline."*
