# Lab 10: Three-Layer Medallion Pipeline

**Course:** Databricks on AWS: Advanced Data Engineering
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

In Intro Lab 4 you answered a business question with one notebook: read, filter, aggregate, write. This lab answers the same question with a governed, incremental, quality-gated pipeline. Same data, same final figure, considerably more machinery — and every piece of that machinery exists for a reason you will see fail if it is missing.

---

## Prerequisites

- [ ] Labs 7–9 completed
- [ ] Intro Lab 4 completed — you built the simple tier of this pipeline
- [ ] **Serverless Lakeflow pipelines, or Pro/Advanced edition** — AUTO CDC is not supported on Apache Spark Declarative Pipelines
- [ ] `training_nic.raw.landing` volume with a `branches/` subdirectory
- [ ] A peer to receive the Gold grant

---

## Objectives

- Ingest with raw Structured Streaming and explain what the checkpoint guarantees
- Express the same ingestion declaratively as a streaming table
- Apply expectations with each of the three violation actions
- Clean native NIC column names at Silver, not on ingest
- Build a Gold materialized view
- Apply AUTO CDC and name its legacy equivalent
- Grant read-only Gold access without exposing Bronze

---

## Part 1: Ingest — Raw Before Declarative

### Task 1: Structured Streaming by Hand

1. **Read the landing directory as a stream**

    Auto Loader is Structured Streaming. Its source is `cloudFiles`. Writing it out longhand once makes the declarative form legible later.

    ```python
    bronze_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"/Volumes/training_nic/raw/landing/_schema/{'<id>'}")
        .option("header", "true")
        .option("comment", "\0")
        .load("/Volumes/training_nic/raw/landing/branches/"))
    ```
    <!-- source: facts_extracted.md §4 -->

    > **Note:** `comment` is set explicitly so a header beginning with `#` is read as a column name rather than skipped. Native NIC names are preserved here deliberately.

2. **Write the stream with a checkpoint**

    ```python
    (bronze_stream.writeStream
        .option("checkpointLocation", f"/Volumes/training_nic/raw/landing/_ckpt/{'<id>'}")
        .trigger(availableNow=True)
        .toTable(f"eng_<id>.work.branches_bronze"))
    ```
    <!-- source: facts_extracted.md §4 -->

3. **Record the row count**

    ```sql
    SELECT COUNT(*) FROM eng_<id>.work.branches_bronze;
    ```
    <!-- source: facts_extracted.md §4 -->

4. **Run the same write a second time**

    Re-execute step 2 without changing anything.

5. **Re-count and compare**

    > **What Just Happened?** The count did not change. File metadata is persisted in a key-value store in the checkpoint location, so already-ingested files are not reprocessed. That is what exactly-once means in practice, and it is why the checkpoint is not optional.
    <!-- source: facts_extracted.md §4 -->

6. **Note the two detection modes**

    > **Key Insight:** Directory listing is the default and scans the path. File notification uses cloud file events instead and is recommended for most workloads because it avoids repeated listing costs at scale. On a training volume the difference is invisible; on a bucket with millions of objects it is the whole cost model.
    <!-- source: facts_extracted.md §4 -->

### Task 2: The Same Ingestion, Declaratively

7. **Create a pipeline source file with a Bronze streaming table**

    ```python
    from pyspark import pipelines as dp
    from pyspark.sql import functions as F

    @dp.table(name="branches_bronze")
    def branches_bronze():
        return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .load("/Volumes/training_nic/raw/landing/branches/"))
    ```
    <!-- source: facts_extracted.md §5 -->

8. **Compare the two forms**

    > **Key Insight:** The declarative version has no explicit checkpoint, no trigger, and no output table name in the writer — the pipeline manages all three. It is the same Structured Streaming underneath. What you gained is lifecycle management; what you gave up is direct control of the writer.

    > **Note:** `dp` comes from `from pyspark import pipelines as dp`, which replaced `import dlt`. You will still see `@dlt` in existing code and it still runs.
    <!-- source: facts_extracted.md §5 -->

---

## Part 2: Silver — Conform and Gate

### Task 3: Clean the Native Names

9. **Add a Silver table that cleans NIC naming**

    ```python
    @dp.table(name="branches_silver")
    @dp.expect_or_drop("valid_key", "ID_RSSD IS NOT NULL")
    def branches_silver():
        return (spark.readStream.table("branches_bronze")
                .withColumnRenamed("#ID_RSSD", "ID_RSSD")
                .withColumn("NM_LGL", F.trim(F.col("NM_LGL")))
                .withColumn("CITY", F.nullif(F.col("CITY"), F.lit(""))))
    ```
    <!-- source: facts_extracted.md §5 -->

10. **Note where cleaning happened**

    > **Key Insight:** Bronze kept the `#`. Silver removed it. That ordering is the point of the Medallion pattern — Bronze is a faithful record of what arrived, so you can always re-derive Silver if your cleaning logic turns out to be wrong. Clean on ingest and you have destroyed the evidence.

### Task 4: All Three Violation Actions

11. **Add a warn expectation**

    Invalid records are written to the target.

    ```python
    @dp.expect("plausible_city", "CITY IS NULL OR LENGTH(CITY) > 1")
    ```
    <!-- source: facts_extracted.md §5 -->

12. **Add a drop expectation**

    Invalid records are dropped before data is written.

    ```python
    @dp.expect_or_drop("valid_key", "ID_RSSD IS NOT NULL")
    ```
    <!-- source: facts_extracted.md §5 -->

13. **Add a fail expectation**

    Invalid records prevent the update from succeeding.

    ```python
    @dp.expect_or_fail("key_is_numeric", "ID_RSSD RLIKE '^[0-9]+$'")
    ```
    <!-- source: facts_extracted.md §5 -->

14. **Write the SQL equivalents**

    ```sql
    CONSTRAINT valid_key EXPECT (ID_RSSD IS NOT NULL) ON VIOLATION DROP ROW
    CONSTRAINT key_is_numeric EXPECT (ID_RSSD RLIKE '^[0-9]+$') ON VIOLATION FAIL UPDATE
    ```
    <!-- source: facts_extracted.md §5 -->

15. **Choose deliberately and record why**

    > **Key Insight:** The three actions encode three different business positions. Warn says the data is worth having even when imperfect. Drop says a bad row is worse than a missing one. Fail says publishing anything wrong is unacceptable. That is a business decision wearing engineering clothes — do not make it by default.

---

## Part 3: Gold and CDC

### Task 5: Gold Materialized View

16. **Aggregate to the business-ready layer**

    ```python
    @dp.materialized_view(name="branch_summary_gold")
    def branch_summary_gold():
        return (spark.read.table("branches_silver")
                .groupBy("STATE_ABBR_NM")
                .agg(F.count("*").alias("branch_count")))
    ```
    <!-- source: facts_extracted.md §5 -->

    > **Note:** `@materialized_view` replaced the older `@table` for materialized views, and `@temporary_view` replaced `@view`.
    <!-- source: facts_extracted.md §5 -->

### Task 6: AUTO CDC — Both Names

17. **Apply AUTO CDC from a snapshot**

    Your SQL Server source has no change data feed, so snapshot comparison is the correct variant. It is Python-only.

    ```python
    dp.create_auto_cdc_from_snapshot_flow(
        target="institutions_scd",
        source="training_nic.legacy_onprem.institutions",
        keys=["#ID_RSSD"],
        stored_as_scd_type=2)
    ```
    <!-- source: facts_extracted.md §6 -->

18. **Note the SQL form and both legacy names**

    The streaming CDC form in SQL needs two things the Python snapshot call above does not: a
    target declared up front, and a named **flow** that writes into it.

    ```sql
    CREATE OR REFRESH STREAMING TABLE institutions_scd;

    CREATE FLOW institutions_scd_flow AS
    AUTO CDC INTO institutions_scd
    FROM STREAM(source_table)
    KEYS (`#ID_RSSD`)
    SEQUENCE BY D_DT_START
    STORED AS SCD TYPE 2;
    ```
    <!-- source: facts_extracted.md §6 -->

    > **Common Pitfall:** `AUTO CDC INTO` is not a statement you can write on its own. Without the
    > `CREATE FLOW <name> AS` wrapper the pipeline fails with **`Missing clause CREATE FLOW for
    > operation AUTO CDC`**, and `SEQUENCE BY` is required rather than optional — it tells the
    > engine which column orders the change events. Omit either and the failure is a syntax error,
    > not a data error, so do not go looking at your source table.
    <!-- source: facts_extracted.md §6 -->

    > **Note:** `SEQUENCE BY` appears here but not in the Python snapshot call in step 17. That is
    > not an inconsistency. Snapshot comparison derives ordering from the snapshots themselves;
    > a stream of change events has no inherent order, so you must name the column that supplies it.
    <!-- source: facts_extracted.md §6 -->

    > **Key Insight:** The AUTO CDC APIs replace the APPLY CHANGES APIs and have identical syntax. You will meet `APPLY CHANGES INTO`, `apply_changes()` and `apply_changes_from_snapshot()` in existing pipelines; they still work. Lead with the new names, recognise the old.
    <!-- source: facts_extracted.md §6 -->

    > **Common Pitfall:** AUTO CDC is not supported on Apache Spark Declarative Pipelines. The pipeline must run on serverless Lakeflow pipelines or the Pro or Advanced edition. If your pipeline fails at this step, check the edition before debugging the syntax.
    <!-- source: facts_extracted.md §6 -->

### Task 7: Publish Gold Only

19. **Grant your peer access to Gold and nothing else**

    ```sql
    GRANT USE CATALOG ON CATALOG eng_<id> TO `<peer>`;
    GRANT USE SCHEMA  ON SCHEMA  eng_<id>.work TO `<peer>`;
    GRANT SELECT      ON TABLE   eng_<id>.work.branch_summary_gold TO `<peer>`;
    ```
    <!-- source: facts_extracted.md §1 -->

20. **Have your peer verify both the access and its limit**

    Ask them to query Gold successfully, then attempt Bronze and confirm they cannot.

    > **Expected Result:** Gold returns rows; Bronze is refused. Consumers see conformed, quality-gated data and never the raw landing.

21. **Compare your Gold figure against Intro Lab 4**

    > **What Just Happened?** The number matches what you produced in one notebook two days ago. Everything added since — incremental ingest, checkpoints, expectations, layer separation, CDC — bought you repeatability, auditability, and a defensible answer to "how do you know this is right?", not a different answer.

---

## Stretch Task

1. Delete the checkpoint directory and re-run the raw stream. What happens, and what does that tell you about where the exactly-once guarantee actually lives?
2. Set the `key_is_numeric` expectation to `FAIL UPDATE` and feed the pipeline a bad row. Capture the failure, then argue whether that action is right for this dataset.
3. Implement the same Bronze ingest with file notification mode instead of directory listing. What extra cloud configuration did it require, and at what data volume would it pay for itself?

---

## Checkpoint: Verify Your Progress

- [ ] I ingested with raw `readStream` and an explicit checkpoint
- [ ] I re-ran the write and confirmed no rows were duplicated
- [ ] I can explain what the checkpoint stores and why
- [ ] I can state the difference between directory listing and file notification
- [ ] I expressed the same ingestion as a declarative streaming table
- [ ] Bronze preserves the hash-prefixed native NIC key
- [ ] Silver renames it and cleans values
- [ ] I applied a warn expectation
- [ ] I applied a drop expectation
- [ ] I applied a fail expectation
- [ ] I wrote the SQL equivalent of at least one expectation
- [ ] I built a Gold materialized view
- [ ] I applied AUTO CDC and can name its legacy equivalent
- [ ] My peer can read Gold and cannot read Bronze
- [ ] My Gold figure matches the Intro Lab 4 result

---

## Troubleshooting Reference

> **Key Insight:** Pipeline failures divide cleanly into edition problems, checkpoint problems, and expectation problems. Check the edition first — it produces the most confusing error of the three.
<!-- source: facts_extracted.md §6 -->

| Issue | Symptom | Solution |
|---|---|---|
| AUTO CDC step fails | Unsupported operation | The pipeline is not on serverless, Pro, or Advanced. AUTO CDC is unsupported on Apache Spark Declarative Pipelines. |
| Re-run duplicates rows | Row count grows on re-execution | The checkpoint location changed or was deleted. |
| Header read as data | Columns named `_c0`, `_c1` | `header` not set, or a `#` header was treated as a comment. Set `comment` explicitly. |
| Column not found in Silver | Error on `#ID_RSSD` | Bronze keeps the hash; rename at Silver. In SQL, backtick-quote it. |
| Pipeline fails on an expectation | Update does not complete | Expected if the action is `FAIL UPDATE`. That is the action working. |
| `import dlt` in existing code | Legacy import | Still runs. `from pyspark import pipelines as dp` is current. |
| Peer can read Bronze | Over-granted | You granted at schema level rather than on the Gold table. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Serverless Lakeflow pipeline | Runs on update; cost scales with data processed | Use `availableNow` triggers in the lab rather than continuous. |
| Materialized view refresh | Runs a serverless pipeline | Match refresh frequency to how often the data actually changes. |
| Directory listing | Repeated listing at scale | File notification mode is recommended for most production workloads. |

**Cleanup:** Keep the pipeline — Labs 11 and 12 wrap and deploy it. Stop any continuous update.

---

## Knowledge Check

1. Auto Loader is built on what, and why does that matter for how you reason about it?
2. What does the checkpoint store, and what would break if you deleted it?
3. Distinguish directory listing from file notification, and state at what point the choice matters.
4. Name the three expectation actions and describe the business position each encodes.
5. Why does Bronze keep the `#` in the column name when Silver removes it?
6. Which AUTO CDC variant suits a SQL Server source without a change data feed, and what is its one limitation?
7. Name the legacy equivalents of `AUTO CDC INTO` and `create_auto_cdc_flow()`.
8. Your Gold figure matches the Intro course's single-notebook result. What did all this machinery actually buy?

Answers are held in the Knowledge Check Bank.

---

## Next Steps

Lab 11 turns this pipeline into an operational asset: a Job that refuses to promote to Gold when quality degrades, alerts on failure, and runs under a service principal rather than your account.

---

## Resources

- Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/
- Spark Declarative Pipelines: https://docs.databricks.com/aws/en/ldp/
- Expectations: https://docs.databricks.com/aws/en/ldp/expectations
- AUTO CDC APIs: https://docs.databricks.com/aws/en/ldp/cdc
- What happened to Delta Live Tables: https://docs.databricks.com/aws/en/ldp/concepts/where-is-dlt

---

*Lab 10 Complete*
