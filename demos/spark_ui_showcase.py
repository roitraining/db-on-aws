# Databricks notebook source
# MAGIC %md
# MAGIC # Spark UI Showcase — instructor demo
# MAGIC
# MAGIC Run on the shared classic cluster (`[prod] db-on-aws · lab cluster`) — the Spark UI
# MAGIC does not exist on serverless. Keep the Spark UI open in a second tab
# MAGIC (**Compute → the cluster → Spark UI**) and walk the room through the **Stages** tab
# MAGIC after each section. Each section produces exactly one artifact:
# MAGIC
# MAGIC | Section | What appears in the Spark UI |
# MAGIC |---|---|
# MAGIC | 1 · Partition sizes | Same scan, 2 jobs: few big tasks vs many small tasks |
# MAGIC | 2 · Shuffle | A 2-stage job with Shuffle Write on the map side, Shuffle Read on the reduce side |
# MAGIC | 3 · Skew | Summary Metrics where the max task dwarfs the median |
# MAGIC | 4 · Spill | "Spill (Memory)" / "Spill (Disk)" columns appear on the sort stage |
# MAGIC | 5 · OOM | A task that dies with `OutOfMemoryError`, retried 4× then failing the job — **run last, it fails on purpose** |

# COMMAND ----------

from pyspark.sql import functions as F

BIG = "training_nic.perf.spill_test"  # ~30M rows, ~250-byte payload each (~7.5 GB uncompressed)

if not spark.catalog.tableExists(BIG):
    print("rebuilding", BIG, "- takes a few minutes")
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    (spark.range(0, 30_000_000)
       .withColumn("k", (F.rand(seed=42) * 1_000_000).cast("long"))
       .withColumn("payload", F.repeat(F.sha2(F.col("id").cast("string"), 256), 4))
       .orderBy("k")
       .write.mode("overwrite").saveAsTable(BIG))
    spark.conf.unset("spark.sql.shuffle.partitions")

print(f"{BIG}: {spark.table(BIG).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Partition sizes — how Spark splits the input
# MAGIC
# MAGIC Same full-table scan twice. The only change is `spark.sql.files.maxPartitionBytes` —
# MAGIC the target size of each input split. In the UI, compare the two scan stages:
# MAGIC **number of tasks** and, under Summary Metrics, **Input Size / Records per task**.
# MAGIC
# MAGIC Talking point: partition count = parallelism. Too few partitions and cores sit idle;
# MAGIC too many and scheduling overhead beats the work. Nobody hand-tunes this daily —
# MAGIC but when a stage has 3 tasks on an 8-core cluster, this is why.

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")   # the default
spark.table(BIG).agg(F.sum(F.length("payload"))).display()
print("Scan #1 done - note the task count on its stage")

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", "16m")    # 8x smaller splits
spark.table(BIG).agg(F.sum(F.length("payload"))).display()
spark.conf.unset("spark.sql.files.maxPartitionBytes")
print("Scan #2 done - same data, ~8x the tasks, each ~1/8 the size")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Shuffle — data moving between stages
# MAGIC
# MAGIC A `COUNT(DISTINCT payload)` per key forces every full payload to move to the node
# MAGIC that owns its key — Spark cannot pre-combine distinct values on the map side, so
# MAGIC the whole ~2.4 GB physically crosses the network. In the UI this is a
# MAGIC **two-stage job**: the first stage shows **Shuffle Write**, the second shows a
# MAGIC matching **Shuffle Read**.
# MAGIC
# MAGIC Talking point: the shuffle is the expensive thing in distributed computing.
# MAGIC Every `GROUP BY`, `JOIN`, `ORDER BY`, and `DISTINCT` pays this cost. (A plain
# MAGIC `GROUP BY ... COUNT(*)` shuffles almost nothing — Spark pre-aggregates on the map
# MAGIC side. That contrast is worth showing if someone asks.)

# COMMAND ----------

(spark.table(BIG)
   .groupBy("k")
   .agg(F.countDistinct("payload").alias("distinct_payloads"))
   .agg(F.sum("distinct_payloads"))
   .display())
print("Open the job: map stage = Shuffle Write ~2.4GB, reduce stage = matching Shuffle Read")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Skew — when one task gets most of the data
# MAGIC
# MAGIC We tag 60% of rows with one value (`CA`) and force a repartition by that column.
# MAGIC One task receives 60% of all the data; the rest split the crumbs. In the stage's
# MAGIC **Summary Metrics**, compare the **Max** column against the **Median** for
# MAGIC duration and shuffle read size — the max task is the straggler everyone waits for.
# MAGIC
# MAGIC Talking point: the job is as slow as its biggest partition. This mirrors the real
# MAGIC NIC data, where CA has ~4x the institutions of the median state.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")  # AQE would fix the skew - let it hurt for the demo

skewed = (spark.table(BIG).sample(0.2, seed=1)
          .withColumn("state", F.when(F.rand(7) < 0.6, F.lit("CA"))
                                .otherwise(F.concat(F.lit("S"), (F.rand(9) * 49).cast("int").cast("string")))))

(skewed.repartition(F.col("state"))
   .write.mode("overwrite").saveAsTable("training_nic.perf.skew_demo"))

spark.conf.set("spark.sql.adaptive.enabled", "true")
print("Open the write stage: Summary Metrics -> Max vs Median shuffle read / duration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Spill — execution memory runs out, disk takes the overflow
# MAGIC
# MAGIC A full sort of the 30M-row table with only 8 shuffle partitions. Each sort task
# MAGIC gets ~4M rows (~1 GB) — more than its share of execution memory — so it sorts in
# MAGIC chunks and **spills** the overflow to disk. The stage grows two extra columns:
# MAGIC **Spill (Memory)** and **Spill (Disk)**. Expect roughly 7 GB / 2 GB.
# MAGIC
# MAGIC Talking point: spill is not a failure — the job still succeeds. It is the
# MAGIC performance smell that says "this stage needed more memory or more partitions."
# MAGIC The fix is usually just more partitions, which is what AQE does automatically.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 8)
(spark.table(BIG)
   .orderBy("k")
   .write.mode("overwrite").saveAsTable("training_nic.perf.spill_demo"))
spark.conf.unset("spark.sql.shuffle.partitions")
print("Open the sort stage: Spill (Memory) / Spill (Disk) columns are now visible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Out of memory — RUN LAST, THIS CELL FAILS ON PURPOSE
# MAGIC
# MAGIC `collect_list` must build one group's entire array in a single task's memory — it
# MAGIC cannot spill. We aggregate ~20M payloads (~5 GB) into ONE group, which exceeds the
# MAGIC executor heap. Watch the UI live: the executor drowns in garbage collection and is
# MAGIC lost (`ExecutorLostFailure` / "heartbeat timed out" — a real memory death usually
# MAGIC looks like this, not a tidy `OutOfMemoryError`), Spark **retries the task 4 times**
# MAGIC on fresh executors (that retry behaviour is itself worth narrating), then the job
# MAGIC fails. Expect ~10 minutes for the full death spiral — narrate while it burns.
# MAGIC
# MAGIC Talking point: spill saves sorts and joins, but an aggregate whose *single value*
# MAGIC is too big for memory has nowhere to go. The executor is killed and replaced —
# MAGIC other users' jobs on the cluster survive. In the UI: **Stages → failed stage →
# MAGIC Failure Reason**, and the Executors tab shows the dead executor.

# COMMAND ----------

(spark.table(BIG)
   .where("id < 20000000")
   .withColumn("g", F.lit(1))
   .groupBy("g")
   .agg(F.collect_list("payload").alias("everything"))
   .write.mode("overwrite").saveAsTable("training_nic.perf.oom_demo"))
