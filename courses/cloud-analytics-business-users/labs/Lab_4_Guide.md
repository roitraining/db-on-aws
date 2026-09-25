# Lab 4: Spark DataFrames and the ETL Path

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

Everything so far has been reading someone else's data. This lab is the first thing you build and own: read a table, clean it, summarize it, and write the result back as a table your colleagues can use. It is the simplest complete pipeline, and the same shape scales all the way up.

---

## Prerequisites

- [ ] Labs 1–3 completed
- [ ] For **Part 4 only**: access to the classic cluster named by your instructor (possibly in the shared class workspace)
- [ ] Nothing to prepare for storage — your personal schema `training_nic.analyst` is created in this lab

> **Note:** Parts 1–3 run on serverless compute in your own account. Only Part 4 needs a classic cluster, because the Spark UI is not available on serverless compute—and that part may run in a different, shared workspace. The lab keeps it last so you switch environments once, at a clean boundary.
<!-- source: facts_extracted.md §13 -->

---

## Objectives

- Read a Unity Catalog table into a DataFrame
- Apply `filter`, `select`, `withColumn`, `groupBy` and `agg`
- Explain why nothing executes until an action is called
- Write a result back to Unity Catalog as a managed Delta table
- Verify a query's performance on serverless with the query profile
- Read partition count and shuffle volume in the Spark UI
- State, for each stage, whether SQL or PySpark was the right tool

---

## PySpark Quick Reference

Every command this lab uses, mapped to the SQL you already know. Refer back here whenever a step introduces something new.

| Command | SQL equivalent | What it does |
|---|---|---|
| `spark.table("cat.schema.tbl")` | `FROM cat.schema.tbl` | Loads a table as a **DataFrame**—a query plan, not data |
| `display(df)` | running the query | Databricks notebook function: executes the plan and renders the result as an interactive, sortable table. This is an **action** |
| `F.col("name")` | a column reference | Names a column inside an expression |
| `.filter(...)` | `WHERE` | Keeps rows matching a condition |
| `.withColumn("new", expr)` | a computed column in `SELECT` | Adds or replaces one column |
| `.select("a", "b")` | the `SELECT` list | Keeps only the named columns |
| `.groupBy(...).agg(...)` | `GROUP BY` + aggregates | Groups rows and computes aggregates |
| `.orderBy(...)` | `ORDER BY` | Sorts the result |
| `F.trim`, `F.trunc`, `F.count`, `F.countDistinct` | `TRIM`, `DATE_TRUNC`, `COUNT`, `COUNT(DISTINCT ...)` | Column functions—same names, Python syntax |
| `.explain()` | `EXPLAIN` | Prints the query plan without running it |
| `.write.mode("overwrite").saveAsTable(...)` | `CREATE OR REPLACE TABLE ... AS SELECT` | Saves the DataFrame as a managed table |
| `%sql` at the top of a cell | — | Switches that one notebook cell to SQL |

Official documentation, if you want the full detail behind any row:

- PySpark basics on Databricks: https://docs.databricks.com/aws/en/pyspark/basics
- PySpark SQL functions reference: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
- The `display()` function and visualizations: https://docs.databricks.com/aws/en/visualizations/

---

## Part 1: Read and Explore

### Task 1: Create a Notebook, Attach and Read

1. **Create a notebook and attach compute**

    In Lab 3 you created a notebook just to hold notes. This time the notebook is where the work happens, and it needs compute:

    1. In the left sidebar, click **Workspace**, and navigate to **Users → your.email** (your own user folder—the same place you created the Git folder in Lab 0).
    2. Click **Create** at the top right and choose **Notebook**.
    3. Rename it from **Untitled Notebook** to `Lab 4 - DataFrames` by clicking the title.
    4. In the language selector next to the title, choose **Python**.
    5. Open the compute selector at the top right of the notebook and attach **serverless** compute. Parts 1–3 all run on serverless; only Part 4 (the Spark UI) switches to the classic cluster.

2. **Read the migrated table into a DataFrame**

    ```python
    df = spark.table("training_nic.migrated.institutions")
    ```
    <!-- source: facts_extracted.md §13 -->

3. **Observe that nothing ran**

    The cell finished immediately. No data has been read yet.

    > **Key Insight:** This is lazy evaluation, the concept that most often surprises anyone coming from a traditional database. `spark.table()` builds a plan; it does not fetch data. Nothing executes until an action asks for results.
    <!-- source: facts_extracted.md §13 -->

4. **Trigger an action**

    `display()` is a Databricks notebook function: it executes the plan and renders the result as an interactive table you can sort, filter, and turn into a chart. `.limit(20)` caps how many rows it asks for—the equivalent of `LIMIT 20`.

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

    Look at what `filtered` now holds:

    ```python
    display(filtered.limit(10))
    ```
    <!-- source: facts_extracted.md §13 -->

    > **Expected Result:** Ten rows, every one with `STATE_ABBR_NM` = `CA`.

7. **Add a calculated column**

    Derive a clean name and the month each record starts in—the padding you met in Lab 3 gets handled once, and the approved report grain is business unit by month.

    ```python
    cleaned = (filtered
               .withColumn("NM_LGL_CLEAN", F.trim(F.col("NM_LGL")))
               .withColumn("start_month", F.trunc(F.col("D_DT_START").cast("date"), "month")))
    ```
    <!-- source: facts_extracted.md §12 -->

    Compare the raw and derived columns side by side:

    ```python
    display(cleaned.select("NM_LGL", "NM_LGL_CLEAN", "D_DT_START", "start_month").limit(10))
    ```
    <!-- source: facts_extracted.md §13 -->

    > **Expected Result:** `NM_LGL_CLEAN` shows the same names without the trailing padding, and `start_month` holds the first day of the month `D_DT_START` falls in.

8. **Select only what you need**

    ```python
    slim = cleaned.select("`#ID_RSSD`", "NM_LGL_CLEAN", "CITY",
                          "STATE_ABBR_NM", "CHTR_TYPE_CD", "start_month")
    ```
    <!-- source: facts_extracted.md §12 -->

    ```python
    display(slim.limit(10))
    ```
    <!-- source: facts_extracted.md §13 -->

    > **Expected Result:** The same rows, now only six columns wide.

    > **Common Pitfall:** Selecting every column and filtering later works, but reads far more than you need. Narrow early.

9. **Look at the plan behind the chain**

    Each `display()` you just ran was an **action**—it executed the plan built up to that point so you could see the data. The chain itself is still only a plan. Prove it:

    ```python
    slim.explain(mode="formatted")
    ```
    <!-- source: facts_extracted.md §13 -->

    > **What Just Happened?** `explain()` printed a four-step plan without reading any data. Defining the chain costs nothing; only an action—like each `display()` above—executes it, and every action re-runs the whole plan to that point, not just the newest step. The plan is what gets optimized, which is why chaining transformations is free until you ask for an answer.

### Task 3: Aggregate

10. **Summarize by business unit and period**

    ```python
    summary = (slim
               .groupBy("CHTR_TYPE_CD", "start_month")
               .agg(F.count("*").alias("institution_count"),
                    F.countDistinct("CITY").alias("distinct_cities"))
               .orderBy("start_month", "CHTR_TYPE_CD"))
    ```
    <!-- source: facts_extracted.md §13 -->

11. **Run the aggregation**

    ```python
    display(summary)
    ```
    <!-- source: facts_extracted.md §13 -->

    > **Expected Result:** One row per charter type per month, with a count and a distinct-city count.

---

## Part 3: Write It Back

### Task 4: Publish to Unity Catalog

12. **Create your personal schema, then write the summary as a managed Delta table**

    Lab 3 already created your personal schema for the validation table; `IF NOT EXISTS` makes this cell safe to run either way.

    ```python
    spark.sql("CREATE SCHEMA IF NOT EXISTS training_nic.analyst")
    ```

    ```python
    (summary.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("training_nic.analyst.institution_summary"))
    ```
    <!-- source: facts_extracted.md §13 -->

13. **Verify it with SQL, not Python**

    ```sql
    %sql
    SELECT * FROM training_nic.analyst.institution_summary
    ORDER BY start_month DESC
    LIMIT 20;
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Key Insight:** You just switched languages mid-notebook and it cost nothing. Verification queries are read-only and ad hoc, which is exactly what SQL is best at. The read-clean-aggregate chain above was PySpark because each step needed to be inspectable and rerunnable.

14. **Record which language you would use for each stage**

    Write down, for each of read, clean, aggregate and verify, whether you would reach for PySpark or SQL and why. There is no single right answer, but there is a defensible one.

    > **Expected Result:** A table in your personal schema with one row per charter type per month, queryable by SQL, ready to publish in Lab 5.

### Task 5: See Your Table in the Catalog

15. **Find your table in Catalog Explorer**

    In the left sidebar, click **Catalog**, then expand **training_nic → analyst** and select **institution_summary**. The **Overview** tab shows the columns and types you defined in Python—now visible to anyone with access, without opening a notebook.

16. **Add a description**

    Click the edit (pencil) control next to the table description and write one sentence a colleague would understand: what the table holds and where it came from. Catalog Explorer can draft this with AI (**AI generate**)—if you use it, read the draft critically and correct it before saving. It is a suggestion based on names and sample values, not knowledge of your intent.

    > **Key Insight:** Descriptions are not decoration. They are what Catalog Explorer search matches on, and what Genie reads for context in Lab 6. An undescribed table is invisible to both.

17. **Read the History and Permissions tabs**

    Open the **History** tab: your `saveAsTable` write is there as the table's first commit—the same audit log you read on the migrated table in Lab 3, and every Delta table carries one from its first write. Then glance at the **Permissions** tab: empty apart from your own ownership. Lab 5 is where you fill it in.

---

## Part 4: Verify Performance on Serverless

### Task 6: Read the Query Profile

18. **Run the same shape of query at 2 million rows**

    Your 4,900-row pipeline finishes before performance can matter. Setup built 2-million-row versions of the same tables—`training_nic.perf.institutions_large` and `training_nic.perf.financials_large`—where slow is visible. Run the join-and-aggregate in a `%sql` cell and note the wall time:

    ```sql
    %sql
    SELECT i.CHTR_TYPE_CD,
           COUNT(*)          AS institution_count,
           SUM(f.TOT_ASSETS) AS total_assets
    FROM training_nic.perf.institutions_large AS i
    JOIN training_nic.perf.financials_large  AS f
      ON i.`#ID_RSSD` = f.`#ID_RSSD`
    GROUP BY i.CHTR_TYPE_CD;
    ```

19. **Open its query profile**

    On serverless there is no Spark UI—the **query profile** is how you verify a query performs well:

    1. In the left sidebar, click **Query History**.
    2. Your aggregation is at the top of the list—click it.
    3. Click **See query profile**.

    Read the operator graph top to bottom: the scans (note **rows read** and **bytes read** on each), the join, and between them and the aggregate an **Exchange** operator. The Exchange **is** the shuffle—the same thing the Spark UI reports as shuffle write and read, here shown as rows and bytes on one node.

20. **Compare against a narrow query**

    ```sql
    %sql
    SELECT `#ID_RSSD`, STATE_ABBR_NM
    FROM training_nic.perf.institutions_large
    WHERE STATE_ABBR_NM = 'WA'
    LIMIT 100;
    ```

    Open this query's profile the same way.

    > **Expected Result:** No Exchange operator, a fraction of the bytes read, and a fraction of the duration. A filter is narrow—every part of the data can be processed independently and the `LIMIT` stops the scan early. The aggregation is wide—rows sharing a key must be brought together, and that movement is what you pay for.

21. **Write the one factual sentence**

    In a Markdown cell, describe the aggregation the way you would to an engineer: which operator dominated the time, how many rows and bytes crossed the Exchange, and how long the query took. "It was slow" is not actionable; "the join shuffled N rows and the Exchange dominated a M-second query" is.

    > **Key Insight:** This is the serverless answer to "does my query perform well?"—run it, open the profile, find the Exchange and the bytes read. Task counts, partition counts, and straggler diagnosis need the Spark UI, which is where Part 5 goes.

---

## Part 5: Read the Spark UI

### Task 7: Find the Shuffle

22. **Attach the classic cluster and re-run the pipeline**

    The Spark UI belongs to classic compute, so this part runs on the classic cluster named by your instructor—and if your own account cannot create classic compute (Free Edition), it happens in the **shared class workspace** your instructor provides. The login steps for that workspace are covered separately; everything below assumes you are in a workspace where the classic cluster exists.

    Open the compute selector, attach the **classic cluster**, and click **Run All** so the whole pipeline executes on compute whose Spark UI you can open.

    > **Note:** The Spark UI shows work done by *that cluster only*. Your serverless runs from Parts 1–3 are not in it—the re-run is what puts stages there.

23. **Open the Spark UI**

    1. In the left sidebar, click **Compute**. (Open it in a new browser tab if you want to keep the notebook visible—right-click, **Open link in new tab**.)
    2. In the cluster list, click the name of the classic cluster your notebook is attached to—**`db-on-aws · lab cluster`** in the standard deploy.
    3. Across the top of the cluster page, select the **Spark UI** tab.
    4. Inside the Spark UI, select the **Stages** tab.

    The list shows every stage the cluster has run, newest at the top. The ones with a **Submitted** time from a moment ago are from the `display(summary)` cell you just ran.
    <!-- source: facts_extracted.md §13 -->

    > **Note:** If you ran the aggregation on **serverless** compute, you will not be able to see this—serverless has no Spark UI and exposes a query profile instead. Go back to step 1, attach the classic cluster, re-run the `display(summary)` cell, and then open the Spark UI.
    <!-- source: facts_extracted.md §13 -->

24. **Record what you see**

    Note three figures for the aggregation stage: the number of tasks, the shuffle write volume, and the shuffle read volume.

    > **Key Insight:** The task count reflects how many partitions the data was split into. The shuffle figures show how much data moved across the cluster to bring matching keys together. A `groupBy` cannot avoid a shuffle—that is what it is.
    <!-- source: facts_extracted.md §13 -->

25. **Compare against a query that does not shuffle**

    ```python
    display(slim.filter(F.col("start_month") >= "2000-01-01").limit(50))
    ```
    <!-- source: facts_extracted.md §13 -->

26. **Look at the stages for that cell**

    > **What Just Happened?** A filter is narrow—each partition can be processed independently, so there is no shuffle. An aggregation is wide—rows with the same key must end up together, which means moving data. When a query is slow, this distinction is the first thing to check.

### No classic cluster?

You have already done the serverless version of this lesson—Part 4's query profile. The Spark UI adds what the profile cannot show: task counts, partition counts, and straggler diagnosis. If your account cannot create classic compute (Free Edition), this part runs in the shared class workspace your instructor provides.

---

## Stretch Task

For attendees who finish early.

1. Rerun the aggregation with `spark.conf.set("spark.sql.shuffle.partitions", 8)` and compare task counts in the Spark UI. What changed, and did it get faster?
2. Add `.cache()` before the aggregation and run it twice. Compare the two run times, then explain why the second was faster and when caching would be a bad idea.
3. Rewrite the entire pipeline as a single SQL statement. Which version would you rather hand to a colleague, and which would you rather maintain as a scheduled job?

---

## Checkpoint: Verify Your Progress

- [ ] I created a notebook and attached serverless compute
- [ ] I read a Unity Catalog table into a DataFrame
- [ ] I can explain why the read cell finished instantly
- [ ] I identified the hash-prefixed key column in the output of `df.columns`
- [ ] I applied a filter and added two calculated columns
- [ ] I selected a narrowed set of columns
- [ ] I ran `explain()` and saw a plan rather than data
- [ ] I produced an aggregation by charter type and month
- [ ] I wrote a managed Delta table into my own schema
- [ ] I verified the table using SQL in the same notebook
- [ ] I recorded a language choice and reason for each stage
- [ ] I found my table in Catalog Explorer and added a description
- [ ] I read the table's History tab and found my write
- [ ] I ran the 2M-row aggregation and read its query profile
- [ ] I found the Exchange operator and recorded its rows and bytes
- [ ] I compared a narrow query's profile and saw no Exchange
- [ ] I attached the classic cluster and re-ran the pipeline
- [ ] I opened the Spark UI and found the Stages tab
- [ ] I recorded task count, shuffle read and shuffle write for the aggregation
- [ ] I compared those against a filter-only query and saw no shuffle

---

## Troubleshooting Reference

> **Key Insight:** If a cell returns instantly, it probably did nothing. Lazy evaluation means errors often surface at the action, not at the transformation that caused them—read the whole chain, not just the failing line.
<!-- source: facts_extracted.md §13 -->

| Issue | Symptom | Solution |
|---|---|---|
| No Spark UI available | No Spark UI link on the compute | You are on serverless. Attach to the classic cluster; serverless exposes a query profile instead. |
| Column not found on the key | Error naming the `#`-prefixed column | In SQL wrap it in backticks. In Python pass it as a plain string. |
| Write fails | Schema-not-found or permission error on `saveAsTable` | Run the `CREATE SCHEMA` cell in step 12 first. |
| Error appears at the wrong line | Failure reported on a `display()` | Lazy evaluation. The fault is in an earlier transformation; the action merely triggered it. |
| Aggregation very slow | Long-running stage | Check partition count in the Spark UI. Very many small partitions or very few large ones both hurt. |
| `%sql` cell cannot see the table | Table not found | Fully qualify with catalog and schema, or set `USE CATALOG` and `USE SCHEMA` in that cell. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Classic cluster | Billed while running, including idle | Detach and let it auto-terminate at the end of the session. |
| `display()` on a full DataFrame | Reads far more than needed | Always `.limit()` when eyeballing data. |
| Repeated reruns | Each action rereads unless cached | Cache only when you will reuse the same DataFrame several times. |

**Cleanup:** Keep `institution_summary`—Labs 5 and 6 both build on it. Detach from the cluster when finished.

---

## Knowledge Check

1. Why did `spark.table(...)` finish instantly while `display(df.limit(20))` took several seconds?
2. Name two transformations that do not cause a shuffle and one that always does.
3. Your `groupBy` produced a large shuffle read. What does that figure represent physically?
4. The key column is `#ID_RSSD`. How do you reference it in SQL, and how in the DataFrame API?
5. You wrote the summary with `saveAsTable` rather than saving a file. What did that buy you?
6. For read, clean, aggregate and verify—which would you do in PySpark and which in SQL, and why?
7. When is caching a mistake?

Answers — including worked stretch task answers — are in [`answers/Lab_4_Answers.md`](../answers/Lab_4_Answers.md). Attempt the questions before opening it.

---

## Next Steps

Lab 5 takes the table you just created and publishes it: a view over it, a grant so a colleague can read it, and an alert that tells you when it breaks before a stakeholder notices.

---

## Resources

- PySpark basics on Databricks: https://docs.databricks.com/aws/en/pyspark/basics
- PySpark SQL functions reference: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
- Visualizations and `display()`: https://docs.databricks.com/aws/en/visualizations/
- Query profile: https://docs.databricks.com/aws/en/sql/user/queries/query-profile
- Serverless compute limitations: https://docs.databricks.com/aws/en/compute/serverless/limitations
- Databricks on AWS documentation: https://docs.databricks.com/aws/en/
- Technical terminology glossary: https://docs.databricks.com/aws/en/resources/glossary

---

*Lab 4 Complete*
