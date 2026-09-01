# Lab 4: Spark DataFrames and the ETL Path

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

Everything so far has been reading someone else's data. This lab is the first thing you build and own: read a table, clean it, summarise it, and write the result back as a table your colleagues can use. It is the simplest complete pipeline, and the same shape scales all the way up.

---

## Prerequisites

- [ ] Labs 1–3 completed
- [ ] A **classic cluster** attached to your notebook, not a serverless SQL warehouse
- [ ] Your attendee ID and personal schema `training_nic.analyst_<id>`
- [ ] A new notebook created in your workspace folder

> **Note:** This lab requires a classic cluster because the Spark UI is not available on serverless compute. Everything else in this course runs on a serverless SQL warehouse; this session is the exception.
<!-- source: facts_extracted.md §13 -->

---

## Objectives

- Read a Unity Catalog table into a DataFrame
- Apply `filter`, `select`, `withColumn`, `groupBy` and `agg`
- Explain why nothing executes until an action is called
- Write a result back to Unity Catalog as a managed Delta table
- Read partition count and shuffle volume in the Spark UI
- State, for each stage, whether SQL or PySpark was the right tool

---

## Part 1: Read and Explore

### Task 1: Attach and Read

1. **Confirm your compute**

    Attach the notebook to the classic cluster named by your instructor. Check the cluster selector at the top of the notebook.

2. **Read the migrated table into a DataFrame**

    ```python
    df = spark.table("training_nic.migrated.institutions")
    ```
    <!-- source: facts_extracted.md §13 -->

3. **Observe that nothing ran**

    The cell finished immediately. No data has been read yet.

    > **Key Insight:** This is lazy evaluation, the concept that most often surprises SQL Server engineers. `spark.table()` builds a plan; it does not fetch data. Nothing executes until an action asks for results.
    <!-- source: facts_extracted.md §13 -->

4. **Trigger an action**

    ```python
    display(df.limit(20))
    ```
    <!-- source: facts_extracted.md §13 -->

    > **Expected Result:** Twenty rows appear. This cell takes noticeably longer than the previous one, because this is the one that actually read data.

5. **Note the hash-prefixed column**

    ```python
    print(df.columns)
    ```
    <!-- source: facts_extracted.md §12 -->

    > **Note:** The key column arrived from NIC with a leading `#` and has been carried through untouched. In SQL you must wrap it in backticks. In the DataFrame API you can pass it as an ordinary string, which is one practical reason to reach for PySpark when column names are awkward.

---

## Part 2: Transform

### Task 2: Filter and Derive

6. **Filter to active institutions in one state**

    ```python
    from pyspark.sql import functions as F

    filtered = df.filter(F.col("STATE_ABBR_NM") == "CA")
    ```
    <!-- source: facts_extracted.md §12 -->

7. **Add a calculated column**

    Derive a clean name and a name-length flag, so the padding you met in Lab 3 is handled once rather than in every downstream query.

    ```python
    cleaned = (filtered
               .withColumn("NM_LGL_CLEAN", F.trim(F.col("NM_LGL")))
               .withColumn("start_year", F.year(F.col("D_DT_START").cast("date"))))
    ```
    <!-- source: facts_extracted.md §12 -->

8. **Select only what you need**

    ```python
    slim = cleaned.select("`#ID_RSSD`", "NM_LGL_CLEAN", "CITY",
                          "STATE_ABBR_NM", "CHTR_TYPE_CD", "start_year")
    ```
    <!-- source: facts_extracted.md §12 -->

    > **Common Pitfall:** Selecting every column and filtering later works, but reads far more than you need. Narrow early.

9. **Confirm the transformations still have not executed**

    ```python
    slim.explain(mode="formatted")
    ```
    <!-- source: facts_extracted.md §13 -->

    > **What Just Happened?** You have built a four-step plan and Spark has read nothing. The plan is what gets optimised, which is why chaining transformations costs nothing until you ask for an answer.

### Task 3: Aggregate

10. **Summarise by business unit and period**

    ```python
    summary = (slim
               .groupBy("CHTR_TYPE_CD", "start_year")
               .agg(F.count("*").alias("institution_count"),
                    F.countDistinct("CITY").alias("distinct_cities"))
               .orderBy("start_year", "CHTR_TYPE_CD"))
    ```
    <!-- source: facts_extracted.md §13 -->

11. **Run the aggregation**

    ```python
    display(summary)
    ```
    <!-- source: facts_extracted.md §13 -->

    > **Expected Result:** One row per charter type per year, with a count and a distinct-city count.

---

## Part 3: Read the Spark UI

### Task 4: Find the Shuffle

12. **Open the Spark UI**

    From the cluster attached to your notebook, open the Spark UI and select the **Stages** tab. Find the stages produced by the cell you just ran.
    <!-- source: facts_extracted.md §13 -->

13. **Record what you see**

    Note three figures for the aggregation stage: the number of tasks, the shuffle write volume, and the shuffle read volume.

    > **Key Insight:** The task count reflects how many partitions the data was split into. The shuffle figures show how much data moved across the cluster to bring matching keys together. A `groupBy` cannot avoid a shuffle — that is what it is.
    <!-- source: facts_extracted.md §13 -->

14. **Compare against a query that does not shuffle**

    ```python
    display(slim.filter(F.col("start_year") > 2000).limit(50))
    ```
    <!-- source: facts_extracted.md §13 -->

15. **Look at the stages for that cell**

    > **What Just Happened?** A filter is narrow — each partition can be processed independently, so there is no shuffle. An aggregation is wide — rows with the same key must end up together, which means moving data. When a query is slow, this distinction is the first thing to check.

---

## Part 4: Write It Back

### Task 5: Publish to Unity Catalog

16. **Write the summary as a managed Delta table**

    Substitute your attendee ID.

    ```python
    (summary.write
        .mode("overwrite")
        .saveAsTable("training_nic.analyst_<id>.institution_summary"))
    ```
    <!-- source: facts_extracted.md §13 -->

17. **Verify it with SQL, not Python**

    ```sql
    %sql
    SELECT * FROM training_nic.analyst_<id>.institution_summary
    ORDER BY start_year DESC
    LIMIT 20;
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Key Insight:** You just switched languages mid-notebook and it cost nothing. Verification queries are read-only and ad hoc, which is exactly what SQL is best at. The read-clean-aggregate chain above was PySpark because each step needed to be inspectable and re-runnable.

18. **Record which language you would use for each stage**

    Write down, for each of read, clean, aggregate and verify, whether you would reach for PySpark or SQL and why. There is no single right answer, but there is a defensible one.

    > **Expected Result:** A table in your personal schema with one row per charter type per year, queryable by SQL, ready to publish in Lab 5.

---

## Stretch Task

For attendees who finish early.

1. Re-run the aggregation with `spark.conf.set("spark.sql.shuffle.partitions", 8)` and compare task counts in the Spark UI. What changed, and did it get faster?
2. Add `.cache()` before the aggregation and run it twice. Compare the two run times, then explain why the second was faster and when caching would be a bad idea.
3. Rewrite the entire pipeline as a single SQL statement. Which version would you rather hand to a colleague, and which would you rather maintain as a scheduled job?

---

## Checkpoint: Verify Your Progress

- [ ] My notebook is attached to a classic cluster, not a serverless warehouse
- [ ] I read a Unity Catalog table into a DataFrame
- [ ] I can explain why the read cell finished instantly
- [ ] I identified the hash-prefixed key column in the output of `df.columns`
- [ ] I applied a filter and added two calculated columns
- [ ] I selected a narrowed set of columns
- [ ] I ran `explain()` and saw a plan rather than data
- [ ] I produced an aggregation by charter type and year
- [ ] I opened the Spark UI and found the Stages tab
- [ ] I recorded task count, shuffle read and shuffle write for the aggregation
- [ ] I compared those against a filter-only query and saw no shuffle
- [ ] I wrote a managed Delta table into my own schema
- [ ] I verified the table using SQL in the same notebook
- [ ] I recorded a language choice and reason for each stage

---

## Troubleshooting Reference

> **Key Insight:** If a cell returns instantly, it probably did nothing. Lazy evaluation means errors often surface at the action, not at the transformation that caused them — read the whole chain, not just the failing line.
<!-- source: facts_extracted.md §13 -->

| Issue | Symptom | Solution |
|---|---|---|
| No Spark UI available | No Spark UI link on the compute | You are on serverless. Attach to the classic cluster; serverless exposes a query profile instead. |
| Column not found on the key | Error naming the `#`-prefixed column | In SQL wrap it in backticks. In Python pass it as a plain string. |
| Write fails | Permission or path error on `saveAsTable` | You have not replaced `<id>` with your attendee ID, or your schema was not created. |
| Error appears at the wrong line | Failure reported on a `display()` | Lazy evaluation. The fault is in an earlier transformation; the action merely triggered it. |
| Aggregation very slow | Long-running stage | Check partition count in the Spark UI. Very many small partitions or very few large ones both hurt. |
| `%sql` cell cannot see the table | Table not found | Fully qualify with catalog and schema, or set `USE CATALOG` and `USE SCHEMA` in that cell. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Classic cluster | Billed while running, including idle | Detach and let it auto-terminate at the end of the session. |
| `display()` on a full DataFrame | Reads far more than needed | Always `.limit()` when eyeballing data. |
| Repeated re-runs | Each action re-reads unless cached | Cache only when you will reuse the same DataFrame several times. |

**Cleanup:** Keep `institution_summary` — Labs 5 and 6 both build on it. Detach from the cluster when finished.

---

## Knowledge Check

1. Why did `spark.table(...)` finish instantly while `display(df.limit(20))` took several seconds?
2. Name two transformations that do not cause a shuffle and one that always does.
3. Your `groupBy` produced a large shuffle read. What does that figure represent physically?
4. The key column is `#ID_RSSD`. How do you reference it in SQL, and how in the DataFrame API?
5. You wrote the summary with `saveAsTable` rather than saving a file. What did that buy you?
6. For read, clean, aggregate and verify — which would you do in PySpark and which in SQL, and why?
7. When is caching a mistake?

Answers are held in the Knowledge Check Bank.

---

## Next Steps

Lab 5 takes the table you just created and publishes it: a view over it, a grant so a colleague can read it, and an alert that tells you when it breaks before a stakeholder notices.

---

## Resources

- Serverless compute limitations: https://docs.databricks.com/aws/en/compute/serverless/limitations
- Databricks on AWS documentation: https://docs.databricks.com/aws/en/
- Technical terminology glossary: https://docs.databricks.com/aws/en/resources/glossary

---

*Lab 4 Complete*
