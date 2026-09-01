# Lab 8: Rewriting a Stored Procedure and Diagnosing Skew

**Course:** Databricks on AWS: Advanced Data Engineering
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

You have a multi-join T-SQL stored procedure that runs on-premises. This lab converts it to PySpark, then uses the Spark UI to work out why the first version is slower than it should be. The rewrite is the easy part. Reading the execution and knowing which lever to pull is the skill.

---

## Prerequisites

- [ ] Lab 7 completed — you have `eng_<id>.work`
- [ ] **A classic cluster attached** — the Spark UI is not available on serverless
- [ ] The T-SQL stored procedure supplied by your instructor
- [ ] `training_nic.perf` tables present (created by the environment setup script)
- [ ] Intro Lab 4 completed (DataFrame basics are assumed, not taught here)

> **Note:** This lab starts where Intro Lab 4 stopped. You already know `filter`, `select`, `withColumn`, `groupBy` and `join`. Here you diagnose what they cost.
<!-- source: facts_extracted.md §9 -->

---

## Objectives

- Translate a multi-join T-SQL procedure into the DataFrame API
- Distinguish narrow from wide transformations in the DAG
- Read Summary Metrics to identify a straggler task
- Diagnose skew as distinct from volume
- Apply a cache, measure the effect, and state when it is the wrong tool

---

## Part 1: Translate

### Task 1: Read the Procedure and Rewrite It

1. **Read the supplied T-SQL procedure**

    Identify its joins, its filters, its aggregation, and its output shape before writing any Python.

2. **Load the source tables**

    ```python
    from pyspark.sql import functions as F

    inst = spark.table("training_nic.migrated.institutions")
    fin  = spark.table("training_nic.migrated.financials")
    pop  = spark.table("training_nic.reference.state_population")
    ```
    <!-- source: facts_extracted.md §9 -->

3. **Write the multi-join**

    ```python
    joined = (inst
              .join(fin, on="#ID_RSSD", how="inner")
              .join(pop, inst.STATE_ABBR_NM == pop.state_abbr, how="left"))
    ```
    <!-- source: facts_extracted.md §9 -->

4. **Add the filter and aggregation**

    ```python
    result = (joined
              .filter(F.col("CHTR_TYPE_CD").isNotNull())
              .groupBy("STATE_ABBR_NM", "CHTR_TYPE_CD")
              .agg(F.count("*").alias("institution_count"),
                   F.sum("TOT_ASSETS").alias("total_assets")))
    ```
    <!-- source: facts_extracted.md §9 -->

5. **Time the first run**

    ```python
    import time
    t0 = time.time()
    result.count()
    print(f"elapsed: {time.time() - t0:.1f}s")
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Expected Result:** An elapsed time you write down. Everything after this is measured against it.

---

## Part 2: Read the Execution

### Task 2: Why You Cannot Measure This on 4,900 Rows

6. **Look at the time you just recorded**

    It is a few seconds, and most of that is scheduling overhead rather than work. The migrated tables hold 4,900 rows. That is the right size for proving your translation is *correct*, and useless for proving anything about *performance*.

    > **Key Insight:** This is the first real lesson of the lab. Benchmarking on toy data tells you nothing, and worse, it tells you something confidently wrong. Every performance figure quoted from here on was measured, not estimated.

7. **Switch to the performance-scale tables**

    ```python
    inst_l = spark.table("training_nic.perf.institutions_large")
    fin_l  = spark.table("training_nic.perf.financials_large")

    joined = inst_l.join(fin_l, on="#ID_RSSD", how="inner")
    result = (joined.groupBy("STATE_ABBR_NM", "CHTR_TYPE_CD")
                    .agg(F.count("*").alias("institution_count"),
                         F.sum("TOT_ASSETS").alias("total_assets")))
    ```
    <!-- source: facts_extracted.md §9 -->

    These hold two million rows and carry the same shape as the migrated tables. Everything from here runs against them.

### Task 3: Find the Wide Transformations

8. **Open the Spark UI**

    From your cluster, open the Spark UI and find the job produced by the previous cell.
    <!-- source: facts_extracted.md §2 -->

9. **Identify the stage boundaries**

    Count the stages. Each stage boundary is a shuffle.

    > **Key Insight:** Narrow transformations — `filter`, `select`, `withColumn` — run inside a stage because each partition can be processed independently. Wide transformations — `join`, `groupBy`, `distinct` — force a shuffle and therefore a new stage. The number of stages tells you how many times your data crossed the cluster.
    <!-- source: facts_extracted.md §2 -->

10. **Record shuffle read and write for the largest stage**

    Note both figures from the **Stages** tab.
    <!-- source: facts_extracted.md §2 -->

### Task 4: Find the Straggler

11. **Turn off Adaptive Query Execution**

    ```python
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Note:** You are deliberately disabling a safety net so you can see the problem it normally hides. Do not leave it off, and never turn it off in production. Step 16 turns it back on.

12. **Time the query with AQE off**

    ```python
    t0 = time.time()
    result.count()
    print(f"AQE off: {time.time() - t0:.1f}s")
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Expected Result:** Noticeably slower than the same query will run in step 16. Measured across repeated runs on the reference cluster: **17–28 seconds**. Absolute times move with cluster size, warm caches, and what else is running, so record *your* number — the step 16 comparison is the point, not matching this figure.

13. **Measure how unevenly the rows are distributed**

    ```python
    sizes = joined.repartition(F.col("STATE_ABBR_NM")).rdd.glom().map(len).collect()
    nz = [x for x in sizes if x > 0]
    print("non-empty partitions:", len(nz))
    print("max:", max(nz), " median:", sorted(nz)[len(nz)//2])
    print("ratio:", round(max(nz)/sorted(nz)[len(nz)//2], 1))
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Expected Result:** Seven non-empty partitions, a maximum of about **1,200,000** rows against a median of about **133,000** — a ratio near **9×**. That ratio is the straggler, expressed as data rather than as time.

14. **Confirm it in Summary Metrics**

    Open Summary Metrics for the shuffle stage and compare maximum task duration against the median.
    <!-- source: facts_extracted.md §2 -->

    > **Key Insight:** If the maximum task duration greatly exceeds the median, one task is doing far more work than its peers. That is **skew** — one key has disproportionately many rows — and it is a different problem from having too much data overall. More cluster does not fix skew; the straggler is still one task.
    <!-- source: facts_extracted.md §2 -->

15. **Name the skewed key**

    ```python
    display(joined.groupBy("STATE_ABBR_NM")
                  .count()
                  .orderBy(F.desc("count"))
                  .limit(10))
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Expected Result:** `CA` holds roughly **60%** of the rows; the remaining six states hold about 7% each.

    > **What Just Happened?** The distribution you just printed is the same distribution the shuffle produced. One key holds a large share of the rows, so the task handling it runs long while the rest finish and wait.

---

## Part 3: Intervene and Measure

### Task 5: Let AQE Do Its Job

16. **Turn Adaptive Query Execution back on and re-time**

    ```python
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    t0 = time.time()
    result.count()
    print(f"AQE on: {time.time() - t0:.1f}s")
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Expected Result:** Substantially faster than step 12 — around **6 seconds**, a **3× to 4×** improvement with no change to your code. The partition ratio in step 13 reproduced at exactly **9.0×** on every run; that structural number is stable even when the timings are not.

17. **Write down what actually fixed it**

    > **Key Insight:** You did not fix the skew — AQE did, at runtime, by splitting the oversized partition once it had real statistics. This is the most important thing to know about skew on current Databricks: the platform handles the common case for you, and it is on by default. The reason to understand skew anyway is that AQE has limits. It cannot help when the skew is in the *source* rather than the shuffle, when a single key is larger than one task can hold, or when a UDF makes the cost *per row* uneven rather than the row *count* uneven. Those are the cases that still land on your desk.
    <!-- source: facts_extracted.md §2 -->

### Task 6: Cache and Compare

18. **Cache the joined DataFrame and re-time**

    ```python
    joined.cache()
    joined.count()

    t0 = time.time()
    joined.count()
    print(f"cached: {time.time() - t0:.1f}s")
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Expected Result:** A large drop — roughly **3.3s to 0.5s** on the reference cluster — because the second count reads memory instead of re-reading and re-joining.

19. **Check what caching did not fix**

    Re-run the distribution measurement from step 13.

    > **Key Insight:** Caching avoids re-reading and re-joining. It does not change how rows are distributed, so the underlying skew is untouched — the ratio is exactly what it was. Caching helps when you reuse a DataFrame several times; it does nothing for skew, and on a DataFrame used once it is pure overhead.
    <!-- source: facts_extracted.md §2 -->

20. **Release the cache**

    ```python
    joined.unpersist()
    ```
    <!-- source: facts_extracted.md §2 -->

21. **Write down your conclusion**

    In two sentences: what made the query slow, and what would you actually change to fix it.

    > **Expected Result:** An AQE-off time, an AQE-on time, a cached time, the skewed key with its share, and a stated fix that is not "add more cluster."

---

## Stretch Task

1. Set `spark.sql.shuffle.partitions` to a much lower and a much higher value than the default of 200, with AQE off. Record task counts and durations for both. Which direction helped, and why did the other hurt?
2. Join `inst_l` to `training_nic.reference.state_population` and inspect the physical plan with `.explain()`. Now wrap the small table in `F.broadcast()` and inspect it again. **Expect no difference** — both plans already show `BroadcastHashJoin`, because that table is seven rows and Spark broadcasts anything under `spark.sql.autoBroadcastJoinThreshold` automatically. The lesson is to read the plan before optimising it: the hint you were about to add had already been applied, and a hint that changes nothing is a hint that hides what the engine is actually doing.
3. Write the version of this query you would put into production, and justify every difference from the version you first wrote.

---

## Checkpoint: Verify Your Progress

- [ ] My notebook is attached to a classic cluster
- [ ] I read the T-SQL procedure before writing Python
- [ ] I reproduced its joins, filter and aggregation in the DataFrame API
- [ ] I can explain why the 4,900-row tables cannot support a performance claim
- [ ] I switched to the performance-scale tables
- [ ] I opened the Spark UI and counted the stages
- [ ] I can explain what each stage boundary represents
- [ ] I recorded shuffle read and write for the largest stage
- [ ] I disabled AQE and recorded a slower baseline
- [ ] I measured the partition-size ratio and got roughly 9x
- [ ] I read Summary Metrics and compared max task duration to median
- [ ] I identified `CA` as the skewed key and recorded its share
- [ ] I re-enabled AQE and recorded the improvement
- [ ] I can state what AQE fixed, and three cases where it would not have
- [ ] I cached the joined DataFrame and re-timed it
- [ ] I confirmed caching did not change the skew ratio
- [ ] I released the cache
- [ ] I wrote a two-sentence conclusion naming the real fix

---

## Troubleshooting Reference

> **Key Insight:** A slow query has three common causes that look identical from the outside — too much data, too many stages, or one skewed key. Summary Metrics distinguishes them in about ten seconds.
<!-- source: facts_extracted.md §2 -->

| Issue | Symptom | Solution |
|---|---|---|
| No Spark UI | No link on the compute | You are on serverless. Attach a classic cluster. |
| Join produces duplicate rows | Row count higher than expected | A one-to-many relationship. Check the join key's cardinality on both sides. |
| Ambiguous column after join | Reference error on a shared name | Qualify with the DataFrame alias, or rename before joining. |
| Cache appears to do nothing | No time improvement | The DataFrame is used once. Caching only pays back on reuse. |
| Cache makes it slower | Time increased | Caching costs a write. On a single-use DataFrame that cost is never recovered. |
| One task runs far longer | Stage waits on a single task | Skew. Find the dominant key; more executors will not help. |
| Error surfaces at `count()` | Failure on an action | Lazy evaluation — the fault is in an earlier transformation. |
| No straggler visible | Max task duration equals median | You are on the 4,900-row tables, or AQE is still on. Either one hides it. |
| Broadcast hint changes nothing | Plan identical before and after | The table was already under the auto-broadcast threshold. Read the plan first. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Classic cluster | Billed while running, including idle | Detach and auto-terminate at session end. |
| Repeated timing runs | Each re-run re-executes the plan | Time deliberately, not repeatedly. |
| Cache | Consumes executor memory | `unpersist()` when finished, as in step 17. |

**Cleanup:** No tables created. Unpersist the cache and detach from the cluster.

---

## Knowledge Check

1. How do you tell from the Spark UI how many shuffles a query performed?
2. Distinguish a narrow from a wide transformation, giving two examples of each.
3. Max task duration is twenty times the median. What is the diagnosis, and what will adding executors achieve?
4. When does caching pay for itself, and when is it pure overhead?
5. Caching improved your total time but the straggler remained. Explain both facts.
6. Your rewrite returns more rows than the stored procedure did. What is the first thing to check?
7. Name one fix for skew that is not caching and not a bigger cluster.

Answers are held in the Knowledge Check Bank.

---

## Next Steps

Lab 9 moves to Delta Lake internals: the transaction log, incremental loads, the small-file problem they create, and how time travel underpins the validation framework your analysts execute.

---

## Resources

- Serverless compute limitations: https://docs.databricks.com/aws/en/compute/serverless/limitations
- Databricks on AWS documentation: https://docs.databricks.com/aws/en/
- Technical terminology glossary: https://docs.databricks.com/aws/en/resources/glossary

---

*Lab 8 Complete*
