# Lab 9: Delta Internals, Incremental Loads, and OPTIMIZE

**Course:** Databricks on AWS: Advanced Data Engineering
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

Your analysts ran a four-check comparison in Intro Lab 3. This lab builds the machinery underneath it: how Delta records what changed, how incremental loads accumulate small files, and how time travel lets you compare a table against itself. You will end with the validation pattern the analysts execute, expressed as something repeatable.

---

## Prerequisites

- [ ] Labs 7–8 completed
- [ ] **A classic cluster attached** — file-level inspection needs it
- [ ] `eng_<id>.work` schema from Lab 7
- [ ] Intro Lab 3 completed — the four-check framework is assumed

---

## Objectives

- Write a Delta table from a source extract and read its transaction log
- Perform two incremental loads and observe version accumulation
- Compare current state against version 0 using time travel
- Measure the small-file problem incremental loads create
- Run `OPTIMIZE` and quantify the file-count reduction
- State when Liquid Clustering replaces manual partitioning

---

## Part 1: Build and Inspect

### Task 1: Initial Load

1. **Write the first load from the source extract**

    ```sql
    CREATE OR REPLACE TABLE eng_<id>.work.institutions_delta AS
    SELECT * FROM training_nic.legacy_onprem.institutions;
    ```
    <!-- source: facts_extracted.md §9 -->

2. **Read the history**

    ```sql
    DESCRIBE HISTORY eng_<id>.work.institutions_delta;
    ```
    <!-- source: facts_extracted.md §3 -->

3. **Record version 0**

    Note the version number, timestamp, and operation.

    > **Key Insight:** Every write appends a commit to the transaction log. The log — not the files — is the table. This is what makes ACID guarantees possible against object storage, where you cannot lock a file.

4. **Count the underlying files**

    ```sql
    DESCRIBE DETAIL eng_<id>.work.institutions_delta;
    ```
    <!-- source: facts_extracted.md §3 -->

    > **Expected Result:** `numFiles` of **1**, at roughly 30 KB. Write both down — this is your baseline for Part 4.

---

## Part 2: Incremental Loads

### Task 2: Simulate Two Rounds

5. **First incremental load**

    ```sql
    INSERT INTO eng_<id>.work.institutions_delta
    SELECT * FROM training_nic.legacy_onprem.institutions
    WHERE CHTR_TYPE_CD IS NOT NULL
    LIMIT 500;
    ```
    <!-- source: facts_extracted.md §9 -->

6. **Second incremental load**

    ```sql
    INSERT INTO eng_<id>.work.institutions_delta
    SELECT * FROM training_nic.legacy_onprem.institutions
    WHERE CITY IS NOT NULL
    LIMIT 500;
    ```
    <!-- source: facts_extracted.md §9 -->

7. **Re-read the history**

    ```sql
    DESCRIBE HISTORY eng_<id>.work.institutions_delta;
    ```
    <!-- source: facts_extracted.md §3 -->

8. **Re-count the files**

    ```sql
    DESCRIBE DETAIL eng_<id>.work.institutions_delta;
    ```
    <!-- source: facts_extracted.md §3 -->

9. **Compare against your baseline**

    > **Expected Result:** Three versions in the history — `CREATE OR REPLACE TABLE AS SELECT`, `WRITE`, `WRITE` — and `numFiles` risen from **1 to 3**.

    > **What Just Happened?** Two small loads added two commits and one file each. Each incremental write produces its own files regardless of how few rows it carries. Three files is harmless. The point is the *rate*: this table grew its file count by 200% on two loads of 500 rows. Run that pattern nightly for a year and you have thousands of small files, each of which must be opened on every read.

---

## Part 3: Time Travel as a Validation Tool

### Task 3: Compare the Table Against Itself

10. **Query version 0**

    ```sql
    SELECT COUNT(*) AS rows_at_v0
    FROM eng_<id>.work.institutions_delta VERSION AS OF 0;
    ```
    <!-- source: facts_extracted.md §3 -->

11. **Compare against current**

    ```sql
    SELECT
      (SELECT COUNT(*) FROM eng_<id>.work.institutions_delta VERSION AS OF 0) AS at_load,
      (SELECT COUNT(*) FROM eng_<id>.work.institutions_delta)                 AS current_rows;
    ```
    <!-- source: facts_extracted.md §3 -->

12. **Find what the incremental loads actually added**

    ```sql
    SELECT `#ID_RSSD`, COUNT(*) AS occurrences
    FROM eng_<id>.work.institutions_delta
    GROUP BY `#ID_RSSD`
    HAVING COUNT(*) > 1
    LIMIT 20;
    ```
    <!-- source: facts_extracted.md §9 -->

    > **Note:** The key column carries a leading `#` from the NIC source and must be backtick-quoted in SQL. Native names are preserved through raw and Bronze deliberately; cleaning happens at Silver.

13. **Record the retention constraint**

    > **Common Pitfall:** History is governed by `logRetentionDuration`, 30 days by default, but data files are retained 7 days by default. In Databricks Runtime 18.0 and above a time travel query is blocked if it requests a version older than the deleted-file retention period. Time travel is a recent-comparison tool, not an archive.
    <!-- source: facts_extracted.md §3 -->

14. **State the implication for the validation framework**

    Write one sentence on what this means for a UAT process that runs weekly.

---

## Part 4: OPTIMIZE

### Task 4: Fix the Small-File Problem

15. **Record the pre-OPTIMIZE file count**

    ```sql
    DESCRIBE DETAIL eng_<id>.work.institutions_delta;
    ```
    <!-- source: facts_extracted.md §3 -->

16. **Run OPTIMIZE**

    ```sql
    OPTIMIZE eng_<id>.work.institutions_delta;
    ```
    <!-- source: facts_extracted.md §3 -->

17. **Record the post-OPTIMIZE file count**

    ```sql
    DESCRIBE DETAIL eng_<id>.work.institutions_delta;
    ```
    <!-- source: facts_extracted.md §3 -->

18. **Confirm OPTIMIZE added a version rather than removing history**

    ```sql
    DESCRIBE HISTORY eng_<id>.work.institutions_delta;
    ```
    <!-- source: facts_extracted.md §3 -->

    > **Key Insight:** `OPTIMIZE` compacts small files into larger ones and commits that as another version. It rewrites data layout without changing data. Time travel to earlier versions still works, which is why compaction is safe to schedule.
    <!-- source: facts_extracted.md §3 -->

19. **Record the reduction**

    Write the before and after file counts and the ratio.

    > **Expected Result:** `numFiles` back down from **3 to 1**, an unchanged row count of 6,000, and a fourth version logged as `OPTIMIZE`. Total size drops too — roughly **39 KB to 19 KB** — because one compacted file compresses far better than three fragments of the same data.

    > **Common Pitfall:** Do not read that size drop as data loss. Confirm it is not by re-running your row count, and by time travelling to version 0 — it still returns the original 5,000 rows.

20. **State when you would use Liquid Clustering instead of partitioning**

    Write two sentences.

    > **Key Insight:** Manual partitioning fixes a layout at write time and is expensive to change. Liquid Clustering re-clusters incrementally as data arrives, which suits a migration where load patterns are still being discovered. For a table whose access pattern you do not yet know, choosing partitions early is a guess you will pay for.

---

## Stretch Task

1. Run `VACUUM` with a dry run and describe exactly what it would remove. Why is that irreversible in a way `OPTIMIZE` is not?
2. Build a reusable validation query that takes two version numbers and reports row-count delta, key delta, and aggregate delta in one result set. This is the framework your analysts execute.
3. Enable Liquid Clustering on a copy of the table with a clustering key, load data, and compare file layout against the unclustered original.

---

## Checkpoint: Verify Your Progress

- [ ] I created a Delta table from the source extract
- [ ] I read `DESCRIBE HISTORY` and recorded version 0
- [ ] I recorded the baseline file count from `DESCRIBE DETAIL`
- [ ] I performed two incremental loads
- [ ] I confirmed each load added a version
- [ ] I confirmed the file count grew with each load
- [ ] I queried the table at `VERSION AS OF 0`
- [ ] I compared version 0 row count against current
- [ ] I backtick-quoted the hash-prefixed key correctly
- [ ] I can state the practical limit on how far back time travel reaches
- [ ] I recorded the file count before `OPTIMIZE`
- [ ] I ran `OPTIMIZE` and recorded the file count after
- [ ] I confirmed history survived compaction
- [ ] I stated when Liquid Clustering beats manual partitioning

---

## Troubleshooting Reference

> **Key Insight:** Delta problems are almost always about the transaction log rather than the data. `DESCRIBE HISTORY` and `DESCRIBE DETAIL` answer most questions before you open a single file.
<!-- source: facts_extracted.md §3 -->

| Issue | Symptom | Solution |
|---|---|---|
| Time travel fails | Version-not-available error | The version is older than the retention window, or the table has only one version. Check `DESCRIBE HISTORY`. |
| Column not found on the key | Error naming `#ID_RSSD` | Backtick-quote it. Native NIC names are preserved deliberately. |
| File count does not drop | `numFiles` unchanged after `OPTIMIZE` | The files were already large enough to leave alone. Compaction has a target size. |
| `OPTIMIZE` is slow | Long-running command | Expected on a fragmented table. It rewrites data. |
| Row count changed after `OPTIMIZE` | Counts differ | It should not. Re-check the query — compaction does not alter data. |
| Duplicate keys after incremental loads | Repeated `#ID_RSSD` values | Expected here — the loads deliberately overlap. In production this is what a merge or AUTO CDC prevents. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Classic cluster | Billed while running | Auto-terminate at session end. |
| `OPTIMIZE` | Rewrites data; cost scales with table size | Schedule it, do not run it after every write. |
| Version accumulation | Each write retains prior files for the retention window | Understand `VACUUM` before using it — it is irreversible. |

**Cleanup:** Keep `institutions_delta` — Lab 10 references its layout for comparison. Detach from the cluster.

---

## Knowledge Check

1. What makes the transaction log rather than the files the authoritative table?
2. Two incremental loads of 500 rows each. What happened to the version count and to the file count, and why are they different questions?
3. Why do incremental loads degrade read performance over time?
4. What does `OPTIMIZE` change and what does it deliberately leave alone?
5. Did `OPTIMIZE` destroy your ability to time travel to version 0? Explain.
6. What is the practical limit on time travel, and what would you change to extend it?
7. When is Liquid Clustering the better choice than partitioning, and why does that suit a migration in particular?

Answers are held in the Knowledge Check Bank.

---

## Next Steps

Lab 10 begins Day 2 and the advanced tier of the pipeline ladder. You will rebuild the Intro course's single-notebook pipeline as a three-layer Medallion pipeline with incremental ingestion and quality gates.

---

## Resources

- Delta Lake table history: https://docs.databricks.com/aws/en/delta/history
- Databricks on AWS documentation: https://docs.databricks.com/aws/en/
- Technical terminology glossary: https://docs.databricks.com/aws/en/resources/glossary

---

*Lab 9 Complete*
