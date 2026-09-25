# Lab 3: UAT—Comparing Cloud Data to On-Premises

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

You are being asked to sign off that the migrated data matches the source. This lab gives you a framework for answering that question properly rather than by spot-checking a few rows. Work through the four checks in order. The data does not agree, and part of the exercise is distinguishing a real defect from noise you created yourself. By the end, the whole investigation is a **runbook**: a notebook that records every check's verdict in a table and re-runs against each migration attempt.

---

## Prerequisites

- [ ] Labs 1 and 2 completed
- [ ] A running serverless SQL warehouse selected
- [ ] Access to `training_nic.legacy_onprem` (verify: `SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions` returns 62,080)
- [ ] Your `LENGTH()` observation from Lab 2, step 5

---

## Objectives

- Apply a four-check UAT framework in the order that keeps noise out of your results
- Compare row counts, key sets, and aggregates between two systems
- Normalize both sides before drawing a conclusion from a row-level comparison
- Distinguish a genuine data defect from a comparison artifact
- Query a Delta table as it existed at an earlier version
- Document findings so an engineer can act on them
- Record check verdicts in a `validation_runs` table and re-run the whole validation in one click

---

## Part 1: Establish the Comparison

### Task 1: Identify Both Sides

1. **Create the validation runbook notebook**

    The approved deliverable for this lab is a shared notebook of findings. We go one better: the notebook **is** the validation. You will build the four checks as cells, and each check records its verdict in a table—so the validation can be re-run against every migration attempt, and Lab 6 charts those attempts on a dashboard.

    1. In the left sidebar, click **Workspace**, open **Users → your.email**, click **Create** at the top right, and choose **Notebook**.
    2. Rename it `Lab 3 - Migration Validation Runbook`.
    3. In the language selector next to the title, choose **SQL**.
    4. In the compute selector at the top right, attach the **serverless SQL warehouse**.

    > **Note:** In a shared workspace you would also click **Share** and give your instructor **Can View**. Everyone here runs an isolated account, so the findings review happens over screen-share instead—and once GitLab is wired into the course, this runbook is the first thing you will push.

    Each numbered step below is a **new cell**, added in order.

2. **Create your schema and the validation results table**

    Your personal schema holds everything you build in this course, and `validation_runs` is where every check records its verdict.

    ```sql
    -- one-time setup: your schema, plus the table every check writes its verdict into
    CREATE SCHEMA IF NOT EXISTS training_nic.analyst;

    CREATE TABLE IF NOT EXISTS training_nic.analyst.validation_runs (
      run_ts       TIMESTAMP,
      check_name   STRING,
      cloud_value  DECIMAL(18,2),
      source_value DECIMAL(18,2),
      passed       BOOLEAN,
      note         STRING);

    -- one timestamp per Run All, shared by every check in this attempt
    DECLARE OR REPLACE VARIABLE run_started TIMESTAMP DEFAULT current_timestamp();
    ```

    > **Note:** `run_started` is a session variable, set once when this cell runs. Every check stamps its verdict with the **same** timestamp, so one **Run all** is one attempt—without it, inserts that straddle a clock boundary would split a single run into ragged pieces.
    <!-- source: facts_extracted.md §10 -->

3. **Confirm the cloud side**

    ```sql
    -- how many rows made it to the cloud?
    USE CATALOG training_nic;
    USE SCHEMA migrated;
    SELECT COUNT(*) AS cloud_rows FROM institutions;
    ```
    <!-- source: facts_extracted.md §2 -->

4. **Identify the source of truth**

    The on-premises source is **`training_nic.legacy_onprem`**—a snapshot schema holding the extract taken at cutover. Every query in this lab uses it.

    > **Note:** Some deliveries federate live over the on-premises SQL Server instead, via a foreign catalog (`CREATE FOREIGN CATALOG ... USING CONNECTION ...`—instructor-created, since it needs metastore privileges). In that variant, use `<foreign_catalog>.dbo` wherever this lab says `training_nic.legacy_onprem`. Everything else is identical.
    <!-- source: facts_extracted.md §9 -->

5. **Count the source side**

    ```sql
    -- and how many the source of truth holds
    SELECT COUNT(*) AS source_rows
    FROM training_nic.legacy_onprem.institutions;
    ```
    <!-- source: facts_extracted.md §9 -->

    > **Expected Result:** `source_rows` is **62,080**.

    > **Troubleshooting (federated variant only):** If a federated query fails immediately rather than returning rows, the cause is usually the connection rather than your SQL. Federated connections are always encrypted with SSL and the certificate hostname must match the endpoint requested, or the connection fails during the handshake.

6. **Confirm the gap**

    Source: **62,080**. Cloud: **61,699**. The four checks that follow find where the 381 rows went—and what else the migration broke.

---

## Part 2: The Four Checks

Run these in order. Each answers a different question, and each has a blind spot the next one covers.

### Task 2: Check 1—Row Count Parity

7. **Compare the totals**

    ```sql
    -- CHECK 1: row count parity — did everything arrive?
    SELECT
      (SELECT COUNT(*) FROM training_nic.migrated.institutions)   AS cloud_rows,
      (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions)    AS source_rows;
    ```
    <!-- source: facts_extracted.md §10 -->

8. **Record the difference**

    Record the verdict as data, not as a note to yourself:

    ```sql
    -- record check 1's verdict
    INSERT INTO training_nic.analyst.validation_runs
    SELECT run_started, 'check_1_row_count',
           (SELECT COUNT(*) FROM training_nic.migrated.institutions),
           (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions),
           (SELECT COUNT(*) FROM training_nic.migrated.institutions) =
           (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions),
           'total row counts, cloud vs source';
    ```
    <!-- source: facts_extracted.md §10 -->

    > **Expected Result:** One row added to `validation_runs` with `passed` = `false`—61,699 against 62,080.

    > **Key Insight:** Check 1 answers "did everything arrive?" and nothing else. It tells you nothing about whether the rows that *did* arrive are correct. A migration can pass this check and still be badly wrong.
    <!-- source: facts_extracted.md §10 -->

### Task 3: Check 2—Key Parity

9. **Find keys present in the source but missing from the cloud**

    ```sql
    -- CHECK 2: key parity — keys present in the source but missing from the cloud
    SELECT s.`#ID_RSSD`
    FROM training_nic.legacy_onprem.institutions AS s
    LEFT ANTI JOIN training_nic.migrated.institutions AS c
      ON s.`#ID_RSSD` = c.`#ID_RSSD`;
    ```
    <!-- source: facts_extracted.md §10 -->

    > **Expected Result:** 381 keys—the missing rows from Check 1, now identified individually.

10. **Now flip it—is there anything in the cloud that was never on-premises?**

    ```sql
    -- reverse direction: did the cloud invent keys that never existed on-prem?
    SELECT c.`#ID_RSSD`
    FROM training_nic.migrated.institutions AS c
    LEFT ANTI JOIN training_nic.legacy_onprem.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`;
    ```
    <!-- source: facts_extracted.md §10 -->

    > **Expected Result:** Zero rows. The migration lost data; it did not invent any.

11. **Look for a pattern in what is missing**

    A list of missing keys is a symptom. The useful finding is what they have in common. Join the missing keys back to the source and group by the categorical columns one at a time.

    ```sql
    -- what do the missing rows have in common?
    SELECT s.CHTR_TYPE_CD, COUNT(*) AS missing_count
    FROM training_nic.legacy_onprem.institutions AS s
    LEFT ANTI JOIN training_nic.migrated.institutions AS c
      ON s.`#ID_RSSD` = c.`#ID_RSSD`
    GROUP BY s.CHTR_TYPE_CD
    ORDER BY missing_count DESC;
    ```
    <!-- source: facts_extracted.md §10 -->

12. **Read the pattern**

    > **Expected Result:** One row: charter type `250`, missing count **381**. Every single missing row shares one charter type.

    > **What Just Happened?** The missing rows cluster in one category rather than spreading evenly—that is not random loss, that is a filter in the migration job. A far more actionable finding than "381 rows are missing."

13. **Record check 2**

    Missing keys go in `cloud_value`, invented keys in `source_value`; the check passes only when both are zero.

    ```sql
    -- record check 2: missing keys, invented keys — pass only if both are zero
    INSERT INTO training_nic.analyst.validation_runs
    SELECT run_started, 'check_2_key_parity',
           (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions s
              LEFT ANTI JOIN training_nic.migrated.institutions c
              ON s.`#ID_RSSD` = c.`#ID_RSSD`),
           (SELECT COUNT(*) FROM training_nic.migrated.institutions c
              LEFT ANTI JOIN training_nic.legacy_onprem.institutions s
              ON c.`#ID_RSSD` = s.`#ID_RSSD`),
           (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions s
              LEFT ANTI JOIN training_nic.migrated.institutions c
              ON s.`#ID_RSSD` = c.`#ID_RSSD`) = 0
           AND
           (SELECT COUNT(*) FROM training_nic.migrated.institutions c
              LEFT ANTI JOIN training_nic.legacy_onprem.institutions s
              ON c.`#ID_RSSD` = s.`#ID_RSSD`) = 0,
           'cloud_value = keys missing from cloud; source_value = keys the cloud invented';
    ```
    <!-- source: facts_extracted.md §10 -->

    > **Expected Result:** `cloud_value` = 381, `source_value` = 0, `passed` = `false`.

### Task 4: Check 3—Aggregate Parity

14. **Compare sums on a numeric column**

    Financial figures are not in the institution record. NIC's Attributes file holds identification, classification and structure only—no balance-sheet data. The amounts come from a separate reporting table keyed on the same `ID_RSSD`.

    ```sql
    -- CHECK 3: aggregate parity — do the totals agree?
    SELECT
      (SELECT SUM(TOT_ASSETS) FROM training_nic.migrated.financials) AS cloud_total,
      (SELECT SUM(TOT_ASSETS) FROM training_nic.legacy_onprem.financials) AS source_total;
    ```
    <!-- source: facts_extracted.md §12 -->

    > **Note:** This is a normal shape for a migration. The dimension and the facts arrive as separate tables, and each has to be validated on its own. A clean institution table proves nothing about the amounts.

15. **Compare the difference against the row-count gap**

    If rows are missing, some difference is expected. Restrict the comparison to keys present on both sides so the two effects do not mask each other.

    ```sql
    SELECT
      SUM(c.TOT_ASSETS) AS cloud_total,
      SUM(s.TOT_ASSETS) AS source_total,
      SUM(s.TOT_ASSETS) - SUM(c.TOT_ASSETS) AS difference
    FROM training_nic.migrated.financials AS c
    JOIN training_nic.legacy_onprem.financials AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`;
    ```
    <!-- source: facts_extracted.md §12 -->

16. **Compare null counts as a separate figure from empty strings**

    These are not the same value, and a comparison that treats them as interchangeable will mislead you.

    ```sql
    -- NULL and '' are different values — count them separately (cloud side, then source)
    -- NULL and '' are different values — count them separately (cloud side, then source)
    SELECT
      SUM(CASE WHEN CITY IS NULL THEN 1 ELSE 0 END)  AS null_cities,
      SUM(CASE WHEN CITY = ''    THEN 1 ELSE 0 END)  AS empty_cities
    FROM training_nic.migrated.institutions;
    ```
    <!-- source: facts_extracted.md §10 -->

17. **Run the same null and empty-string counts against the source**

    ```sql
    SELECT
      SUM(CASE WHEN CITY IS NULL THEN 1 ELSE 0 END)  AS null_cities,
      SUM(CASE WHEN CITY = ''    THEN 1 ELSE 0 END)  AS empty_cities
    FROM training_nic.legacy_onprem.institutions;
    ```
    <!-- source: facts_extracted.md §10 -->

18. **Record every figure**

    Record the sums restricted to shared keys—the pure value comparison, with the row-count effect removed:

    ```sql
    -- record check 3: sums over shared keys
    INSERT INTO training_nic.analyst.validation_runs
    SELECT run_started, 'check_3_aggregate',
           SUM(c.TOT_ASSETS), SUM(s.TOT_ASSETS),
           SUM(c.TOT_ASSETS) = SUM(s.TOT_ASSETS),
           'SUM(TOT_ASSETS) over keys present on both sides'
    FROM training_nic.migrated.financials AS c
    JOIN training_nic.legacy_onprem.financials AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`;
    ```
    <!-- source: facts_extracted.md §10 -->

    > **Expected Result:** `passed` = `false`—the totals disagree by roughly $32,000 even over identical keys.

    > **Key Insight:** Check 3 catches errors that are invisible row by row. A value that is slightly wrong on every row looks fine in a spot check and only appears when you sum the column.
    <!-- source: facts_extracted.md §10 -->

### Task 5: Check 4—Row-Level Comparison

19. **Run the naive comparison first**

    Do this before normalizing anything. You are meant to see what it produces.

    ```sql
    -- CHECK 4: row-level comparison — naive version first, on purpose
    SELECT c.`#ID_RSSD`, c.NM_LGL AS cloud_name, s.NM_LGL AS source_name
    FROM training_nic.migrated.institutions AS c
    JOIN training_nic.legacy_onprem.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`
    WHERE c.NM_LGL <> s.NM_LGL
    LIMIT 50;
    ```
    <!-- source: facts_extracted.md §10 -->

20. **Count how many rows it reports**

    ```sql
    -- how many rows does the naive comparison flag?
    SELECT COUNT(*) AS naive_mismatches
    FROM training_nic.migrated.institutions AS c
    JOIN training_nic.legacy_onprem.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`
    WHERE c.NM_LGL <> s.NM_LGL;
    ```
    <!-- source: facts_extracted.md §10 -->

    > **What Just Happened?** If that number is implausibly large, stop before reporting it. A result claiming almost every row is wrong is far more likely to be a problem with your comparison than with the migration. Look at the values returned in step 16 and compare them character by character. Your `LENGTH()` observation from Lab 2 is the clue.

21. **Normalize both sides and rerun**

    Apply `TRIM` to remove padding and `NULLIF` to collapse empty strings to null, on **both** sides of the comparison.

    ```sql
    -- normalized: TRIM the padding, collapse '' to NULL, null-safe compare
    SELECT COUNT(*) AS real_mismatches
    FROM training_nic.migrated.institutions AS c
    JOIN training_nic.legacy_onprem.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`
    WHERE NULLIF(TRIM(c.NM_LGL), '') IS DISTINCT FROM NULLIF(TRIM(s.NM_LGL), '');
    ```
    <!-- source: facts_extracted.md §10 -->

    > **Note:** `IS DISTINCT FROM` treats two nulls as equal, which is what you want when comparing. Plain `<>` returns null when either side is null, so those rows silently drop out of your result.

22. **Compare the two counts**

    Record the naive figure and the normalized figure side by side. The gap between them is the noise you were about to report as a defect.

23. **Record check 4**

    The normalized count is the real verdict; the naive count rides along in `source_value` as evidence of the noise you removed.

    ```sql
    -- record check 4: normalized mismatches (real) vs naive count (noise)
    INSERT INTO training_nic.analyst.validation_runs
    SELECT run_started, 'check_4_row_level',
           (SELECT COUNT(*) FROM training_nic.migrated.institutions c
              JOIN training_nic.legacy_onprem.institutions s ON c.`#ID_RSSD` = s.`#ID_RSSD`
              WHERE NULLIF(TRIM(c.NM_LGL), '') IS DISTINCT FROM NULLIF(TRIM(s.NM_LGL), '')),
           (SELECT COUNT(*) FROM training_nic.migrated.institutions c
              JOIN training_nic.legacy_onprem.institutions s ON c.`#ID_RSSD` = s.`#ID_RSSD`
              WHERE c.NM_LGL <> s.NM_LGL),
           (SELECT COUNT(*) FROM training_nic.migrated.institutions c
              JOIN training_nic.legacy_onprem.institutions s ON c.`#ID_RSSD` = s.`#ID_RSSD`
              WHERE NULLIF(TRIM(c.NM_LGL), '') IS DISTINCT FROM NULLIF(TRIM(s.NM_LGL), '')) = 0,
           'cloud_value = normalized NM_LGL mismatches; source_value = naive count';
    ```
    <!-- source: facts_extracted.md §10 -->

    > **Expected Result:** `cloud_value` = 0 for the name column, `source_value` in the thousands—the naive comparison was almost entirely noise.

24. **Compare a date column the same way**

    The name column failed on formatting—padding and empty strings. Dates fail differently: there is nothing to trim, but the null-safe `IS DISTINCT FROM` comparison still applies. Run it against `D_DT_START` and look at the `day_difference` column.

    ```sql
    -- dates: nothing to trim, but the null-safe comparison still applies
    SELECT c.`#ID_RSSD`, c.D_DT_START AS cloud_date, s.D_DT_START AS source_date,
           datediff(c.D_DT_START, s.D_DT_START) AS day_difference
    FROM training_nic.migrated.institutions AS c
    JOIN training_nic.legacy_onprem.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`
    WHERE c.D_DT_START IS DISTINCT FROM s.D_DT_START
    LIMIT 50;
    ```
    <!-- source: facts_extracted.md §5 -->

25. **Check whether any date difference is consistent**

    ```sql
    -- is the date shift the same size on every affected row?
    SELECT datediff(c.D_DT_START, s.D_DT_START) AS day_difference,
           COUNT(*) AS row_count
    FROM training_nic.migrated.institutions AS c
    JOIN training_nic.legacy_onprem.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`
    WHERE c.D_DT_START IS DISTINCT FROM s.D_DT_START
    GROUP BY 1
    ORDER BY row_count DESC;
    ```
    <!-- source: facts_extracted.md §5 -->

    > **Key Insight:** A difference that is the same size on every affected row points at a systematic transformation, not at corrupted data. Ask what process would shift a value by a constant amount.

---

## Part 3: Time Travel and Documentation

### Task 6: Compare Against an Earlier Version

26. **View the table's history**

    ```sql
    -- the migration's own audit log
    DESCRIBE HISTORY training_nic.migrated.institutions;
    ```
    <!-- source: facts_extracted.md §8 -->

    > **Expected Result:** Not one entry—a story. Read the `operation` column bottom-up: a `CREATE TABLE AS SELECT` (the initial load), then a `DELETE`, three `UPDATE`s, and metadata commits for the table and column comments. Every write to a Delta table lands in this log, with who ran it, when, and how.

    > **What Just Happened?** Expand `operationParameters` on the `DELETE` row. The predicate is recorded verbatim: `CHTR_TYPE_CD = '250'`—the migration's own audit log stating what Check 2 made you discover the hard way. On a real migration, reading the target table's history is one of the first things worth doing.

27. **Query the faithful copy at version 0**

    Version 0 is the table as first loaded, before any of the writes that introduced defects.

    ```sql
    -- the table as first loaded, before any defect was introduced
    SELECT COUNT(*) AS rows_at_version
    FROM training_nic.migrated.institutions VERSION AS OF 0;
    ```
    <!-- source: facts_extracted.md §8 -->

    > **Expected Result:** **62,080**—the on-premises count. The 381-row gap from Check 1 did not happen in transit. It happened inside this table's own lifetime, at the `DELETE` you can see in the history.

    > **Note:** History retention is governed by `logRetentionDuration`, 30 days by default, but data files are retained for 7 days by default. In Databricks Runtime 18.0 and above, a time travel query is blocked if it requests a version older than the deleted-file retention period. Use time travel for recent comparisons, not as an archive.
    <!-- source: facts_extracted.md §8 -->

    > **What Just Happened?** If you see
    > `[DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION] Cannot time travel beyond delta.deletedFileRetentionDuration (168 HOURS) set on the table.`
    > the environment was built more than 7 days ago: the history still *lists* version 0, but the data files behind it have aged out, so Databricks blocks the query rather than return incomplete results. Ask your instructor to re-run the setup notebook—`SETUP.md` Part 3 carries the timing rule.
    <!-- source: facts_extracted.md §8 -->

### Task 7: Document and Re-Run

28. **Read back the run history**

    ```sql
    -- every verdict recorded so far
    SELECT run_ts, check_name, cloud_value, source_value, passed
    FROM training_nic.analyst.validation_runs
    ORDER BY run_ts, check_name;
    ```

    > **Expected Result:** Four verdict rows—one per check, all from this run. This table is the raw material for the migration-health page you will build on the Lab 6 dashboard, and the thing the Lab 5 validation alert will watch.

29. **Document the findings as a Markdown cell**

    Add a **%md** cell at the top of the notebook. For every failed check, write four things: which check surfaced it, how many rows it affects, what the affected rows have in common, and what you believe caused it. List separately anything that appeared to be a defect but resolved once you normalized—an engineer needs to know what you ruled out as well as what you found. Close with one sentence per finding saying whether it blocks cutover.

30. **Re-run the whole validation**

    A migration gets fixed and re-attempted, and your validation has to be one click—not an afternoon of pasting. Click **Run all**, then read the history grouped by attempt:

    ```sql
    -- one row per Run All: checks run, checks passed
    SELECT run_ts AS run_attempt,
           COUNT(*) AS checks_run,
           SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS checks_passed
    FROM training_nic.analyst.validation_runs
    GROUP BY run_ts
    ORDER BY run_ts;
    ```

    > **Expected Result:** Two attempts, each running 4 checks with the same pass count: **1 of 4**. Check 4 passes—the name defects were formatting, and normalization proved the data itself matches. Checks 1–3 fail because the migration genuinely dropped rows, truncated cents, and shifted dates. The migration is still broken—but now you can prove it, repeatably, and every attempt stays on the record. When engineering ships a fixed migration, this notebook is how you verify the fix.

---

## Stretch Task

For attendees who finish early.

1. Write a single query that reports all four checks as one result set, one row per check, with a pass or fail column. This is the beginning of a reusable validation framework rather than a one-off investigation.
2. Extend the row-level comparison to every text column at once rather than one at a time. What makes this expensive, and why is it the last check rather than the first?
3. You found a difference. Prove it is not caused by the comparison itself—write the query that demonstrates the defect exists independent of your normalization choices.

---

## Checkpoint: Verify Your Progress

- [ ] I recorded row counts from both the cloud and the source
- [ ] I listed keys present in the source but missing from the cloud
- [ ] I checked the reverse direction for extra rows
- [ ] I grouped the missing keys to look for a shared attribute
- [ ] I compared sums restricted to keys present on both sides
- [ ] I counted nulls and empty strings as separate figures on both sides
- [ ] I ran the row-level comparison **before** normalizing and recorded the count
- [ ] I reran it with `TRIM` and `NULLIF` applied to both sides
- [ ] I can explain the gap between the naive and normalized counts
- [ ] I checked whether any date difference was consistent across rows
- [ ] I viewed table history and queried an earlier version
- [ ] Each of the four checks recorded a verdict row in `validation_runs`
- [ ] My runbook's Markdown cell separates confirmed findings from ruled-out artifacts
- [ ] Each finding has a row count, a shared attribute, and a suspected cause
- [ ] I stated whether each finding blocks cutover
- [ ] I re-ran the whole notebook and saw a second attempt in the run history

---

## Troubleshooting Reference

> **Key Insight:** In a comparison exercise, most surprising results are produced by the comparison rather than by the data. Suspect your own query first, particularly when a result claims that almost everything is wrong.
<!-- source: facts_extracted.md §10 -->

| Issue | Symptom | Solution |
|---|---|---|
| Comparison returns nearly every row | Mismatch count close to total row count | Whitespace or null-versus-empty-string. Normalize both sides with `TRIM` and `NULLIF` before comparing. |
| Rows silently missing from a comparison | Fewer rows than expected in the mismatch list | `<>` returns null when either side is null, dropping those rows. Use `IS DISTINCT FROM`. |
| Federated query fails immediately | Error before any rows return | Connection rather than SQL. The connection is always SSL-encrypted and fails at handshake if the certificate hostname does not match the endpoint. |
| Cannot see `training_nic` (or the foreign catalog, on the federated variant) | Absent from Catalog Explorer | Missing traversal grant—re-run the setup notebook (see `SETUP.md`). |
| Time travel fails with a version error | Version-not-available error | The requested version is older than the retention window, or the table has only one version. Use a recent version from `DESCRIBE HISTORY`. |
| Sums differ but row counts match | Totals disagree with no missing rows | Expected—this is what Check 3 exists to catch. Investigate the column type rather than the row set. |
| Cast error mid-comparison | Statement fails on a value | Strict typing. Use `TRY_CAST` if null is the outcome you want for unparseable input. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Serverless SQL warehouse | Billed while running | Leave auto-stop enabled. |
| Federated queries | Each query reaches the source system over JDBC | The source is shared by the whole class. Avoid `SELECT *` without `LIMIT` against it. |
| Row-level comparison | Most expensive of the four checks | Run it last, and only after the cheaper checks have narrowed the question. |

**Cleanup:** Keep everything. `validation_runs` and the runbook are consumed by Lab 5 (the validation alert) and Lab 6 (the migration-health dashboard page).

---

## Knowledge Check

1. Name the four checks in order and state the question each one answers.
2. Check 1 passes. What have you proven, and what have you not?
3. Your row-level comparison reports that 98% of rows differ. What is the most likely explanation, and what do you do before reporting it?
4. Why use `IS DISTINCT FROM` rather than `<>` when comparing two columns that may contain nulls?
5. A numeric column's sum differs between systems while the row count matches exactly. What kind of error does that suggest?
6. Missing rows all share the same value in one column. Why is that more useful than the count of missing rows?
7. Why is the row-level comparison last rather than first?
8. What is the practical limit on how far back a time travel query can reach, and why?

Answers — including worked stretch task answers — are in [`answers/Lab_3_Answers.md`](../answers/Lab_3_Answers.md). Attempt the questions before opening it. Runnable completed files for this lab live in [`completed/Lab_3/`](../completed/Lab_3/).

---

## Next Steps

Day 2 moves from validating someone else's pipeline to building your own. Lab 4 introduces the DataFrame API and has you produce a summary table that you will publish in Lab 5 and visualize in Lab 6.

---

## Resources

- Run federated queries on Microsoft SQL Server: https://docs.databricks.com/aws/en/query-federation/sql-server
- Connect to external databases and catalogs: https://docs.databricks.com/aws/en/query-federation/
- Work with Delta Lake table history: https://docs.databricks.com/aws/en/delta/history
- datediff function: https://docs.databricks.com/aws/en/sql/language-manual/functions/datediff

---

*Lab 3 Complete*
