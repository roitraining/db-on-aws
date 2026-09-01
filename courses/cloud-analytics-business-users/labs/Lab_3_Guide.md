# Lab 3: UAT — Comparing Cloud Data to On-Premises

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

You are being asked to sign off that the migrated data matches the source. This lab gives you a framework for answering that question properly rather than by spot-checking a few rows. Work through the four checks in order. The data does not agree, and part of the exercise is distinguishing a real defect from noise you created yourself.

---

## Prerequisites

- [ ] Labs 1 and 2 completed
- [ ] A running serverless SQL warehouse selected
- [ ] The name of the source-of-truth catalog, supplied by your instructor
- [ ] A shared notebook created for your findings
- [ ] Your `LENGTH()` observation from Lab 2, step 5

---

## Objectives

- Apply a four-check UAT framework in the order that keeps noise out of your results
- Compare row counts, key sets, and aggregates between two systems
- Normalise both sides before drawing a conclusion from a row-level comparison
- Distinguish a genuine data defect from a comparison artefact
- Query a Delta table as it existed at an earlier version
- Document findings so an engineer can act on them

---

## Part 1: Establish the Comparison

### Task 1: Identify Both Sides

1. **Confirm the cloud side**

    ```sql
    USE CATALOG training_nic;
    USE SCHEMA migrated;
    SELECT COUNT(*) AS cloud_rows FROM institutions;
    ```
    <!-- source: facts_extracted.md §2 -->

2. **Identify the source of truth**

    Your instructor will tell you which catalog **and schema** hold the on-premises source. It is one of two things: a **foreign catalog** federated live over the on-premises SQL Server, or a **snapshot schema** holding an extract taken at cutover. The queries in this lab work against either — substitute both placeholders.

    | Path | `<source_catalog>` | `<source_schema>` |
    |---|---|---|
    | Federated (live SQL Server) | the foreign catalog name | `dbo` |
    | Snapshot (fallback) | `training_nic` | `legacy_onprem` |

    > **Note:** Write both values down now. Every query in this lab uses them, and the schema differs between the two paths — it is `dbo` on the federated path because that is SQL Server's default schema, and `legacy_onprem` on the snapshot path.

    > **Note:** A foreign catalog is created with `CREATE FOREIGN CATALOG ... USING CONNECTION ...` and lets you query the source system directly, without copying data. Creating one requires `CREATE CATALOG` on the metastore plus ownership of, or `CREATE FOREIGN CATALOG` on, the connection — which is why your instructor created it rather than you.
    <!-- source: facts_extracted.md §9 -->

3. **Count the source side**

    Substitute the catalog name your instructor gave you.

    ```sql
    SELECT COUNT(*) AS source_rows
    FROM <source_catalog>.<source_schema>.institutions;
    ```
    <!-- source: facts_extracted.md §9 -->

    > **Troubleshooting:** If a federated query fails immediately rather than returning rows, the cause is usually the connection rather than your SQL. Federated connections are always encrypted with SSL and the certificate hostname must match the endpoint requested, or the connection fails during the handshake.

4. **Record both numbers**

    Write both counts in your shared notebook before going further.

---

## Part 2: The Four Checks

Run these in order. Each answers a different question, and each has a blind spot the next one covers.

### Task 2: Check 1 — Row Count Parity

5. **Compare the totals**

    ```sql
    SELECT
      (SELECT COUNT(*) FROM training_nic.migrated.institutions)   AS cloud_rows,
      (SELECT COUNT(*) FROM <source_catalog>.<source_schema>.institutions)    AS source_rows;
    ```
    <!-- source: facts_extracted.md §10 -->

6. **Record the difference**

    Note the gap, if any, in your notebook.

    > **Key Insight:** Check 1 answers "did everything arrive?" and nothing else. It tells you nothing about whether the rows that *did* arrive are correct. A migration can pass this check and still be badly wrong.
    <!-- source: facts_extracted.md §10 -->

### Task 3: Check 2 — Key Parity

7. **Find keys present in the source but missing from the cloud**

    ```sql
    SELECT s.`#ID_RSSD`
    FROM <source_catalog>.<source_schema>.institutions AS s
    LEFT ANTI JOIN training_nic.migrated.institutions AS c
      ON s.`#ID_RSSD` = c.`#ID_RSSD`;
    ```
    <!-- source: facts_extracted.md §10 -->

8. **Check the reverse direction**

    ```sql
    SELECT c.`#ID_RSSD`
    FROM training_nic.migrated.institutions AS c
    LEFT ANTI JOIN <source_catalog>.<source_schema>.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`;
    ```
    <!-- source: facts_extracted.md §10 -->

9. **Look for a pattern in what is missing**

    A list of missing keys is a symptom. The useful finding is what they have in common. Join the missing keys back to the source and group by the categorical columns one at a time.

    ```sql
    SELECT s.CHTR_TYPE_CD, COUNT(*) AS missing_count
    FROM <source_catalog>.<source_schema>.institutions AS s
    LEFT ANTI JOIN training_nic.migrated.institutions AS c
      ON s.`#ID_RSSD` = c.`#ID_RSSD`
    GROUP BY s.CHTR_TYPE_CD
    ORDER BY missing_count DESC;
    ```
    <!-- source: facts_extracted.md §10 -->

10. **Record what you find**

    > **What Just Happened?** If the missing rows cluster in one category rather than spreading evenly, you are not looking at random loss. You are looking at a filter in the migration job. That is a far more actionable finding than "37 rows are missing."

### Task 4: Check 3 — Aggregate Parity

11. **Compare sums on a numeric column**

    Financial figures are not in the institution record. NIC's Attributes file holds identification, classification and structure only — no balance-sheet data. The amounts come from a separate reporting table keyed on the same `ID_RSSD`.

    ```sql
    SELECT
      (SELECT SUM(TOT_ASSETS) FROM training_nic.migrated.financials) AS cloud_total,
      (SELECT SUM(TOT_ASSETS) FROM <source_catalog>.<source_schema>.financials) AS source_total;
    ```
    <!-- source: facts_extracted.md §12 -->

    > **Note:** This is a normal shape for a migration. The dimension and the facts arrive as separate tables, and each has to be validated on its own. A clean institution table proves nothing about the amounts.

12. **Compare the difference against the row-count gap**

    If rows are missing, some difference is expected. Restrict the comparison to keys present on both sides so the two effects do not mask each other.

    ```sql
    SELECT
      SUM(c.TOT_ASSETS) AS cloud_total,
      SUM(s.TOT_ASSETS) AS source_total,
      SUM(s.TOT_ASSETS) - SUM(c.TOT_ASSETS) AS difference
    FROM training_nic.migrated.financials AS c
    JOIN <source_catalog>.<source_schema>.financials AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`;
    ```
    <!-- source: facts_extracted.md §12 -->

13. **Compare null counts as a separate figure from empty strings**

    These are not the same value, and a comparison that treats them as interchangeable will mislead you.

    ```sql
    SELECT
      SUM(CASE WHEN CITY IS NULL THEN 1 ELSE 0 END)  AS null_cities,
      SUM(CASE WHEN CITY = ''    THEN 1 ELSE 0 END)  AS empty_cities
    FROM training_nic.migrated.institutions;
    ```
    <!-- source: facts_extracted.md §10 -->

14. **Run the same null and empty-string counts against the source**

    ```sql
    SELECT
      SUM(CASE WHEN CITY IS NULL THEN 1 ELSE 0 END)  AS null_cities,
      SUM(CASE WHEN CITY = ''    THEN 1 ELSE 0 END)  AS empty_cities
    FROM <source_catalog>.<source_schema>.institutions;
    ```
    <!-- source: facts_extracted.md §10 -->

15. **Record every figure**

    > **Key Insight:** Check 3 catches errors that are invisible row by row. A value that is slightly wrong on every row looks fine in a spot check and only appears when you sum the column.
    <!-- source: facts_extracted.md §10 -->

### Task 5: Check 4 — Row-Level Comparison

16. **Run the naive comparison first**

    Do this before normalising anything. You are meant to see what it produces.

    ```sql
    SELECT c.`#ID_RSSD`, c.NM_LGL AS cloud_name, s.NM_LGL AS source_name
    FROM training_nic.migrated.institutions AS c
    JOIN <source_catalog>.<source_schema>.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`
    WHERE c.NM_LGL <> s.NM_LGL
    LIMIT 50;
    ```
    <!-- source: facts_extracted.md §10 -->

17. **Count how many rows it reports**

    ```sql
    SELECT COUNT(*) AS naive_mismatches
    FROM training_nic.migrated.institutions AS c
    JOIN <source_catalog>.<source_schema>.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`
    WHERE c.NM_LGL <> s.NM_LGL;
    ```
    <!-- source: facts_extracted.md §10 -->

    > **What Just Happened?** If that number is implausibly large, stop before reporting it. A result claiming almost every row is wrong is far more likely to be a problem with your comparison than with the migration. Look at the values returned in step 16 and compare them character by character. Your `LENGTH()` observation from Lab 2 is the clue.

18. **Normalise both sides and re-run**

    Apply `TRIM` to remove padding and `NULLIF` to collapse empty strings to null, on **both** sides of the comparison.

    ```sql
    SELECT COUNT(*) AS real_mismatches
    FROM training_nic.migrated.institutions AS c
    JOIN <source_catalog>.<source_schema>.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`
    WHERE NULLIF(TRIM(c.NM_LGL), '') IS DISTINCT FROM NULLIF(TRIM(s.NM_LGL), '');
    ```
    <!-- source: facts_extracted.md §10 -->

    > **Note:** `IS DISTINCT FROM` treats two nulls as equal, which is what you want when comparing. Plain `<>` returns null when either side is null, so those rows silently drop out of your result.

19. **Compare the two counts**

    Record the naive figure and the normalised figure side by side. The gap between them is the noise you were about to report as a defect.

20. **Apply the same normalisation to a date column**

    ```sql
    SELECT c.`#ID_RSSD`, c.D_DT_START AS cloud_date, s.D_DT_START AS source_date,
           datediff(c.D_DT_START, s.D_DT_START) AS day_difference
    FROM training_nic.migrated.institutions AS c
    JOIN <source_catalog>.<source_schema>.institutions AS s
      ON c.`#ID_RSSD` = s.`#ID_RSSD`
    WHERE c.D_DT_START IS DISTINCT FROM s.D_DT_START
    LIMIT 50;
    ```
    <!-- source: facts_extracted.md §5 -->

21. **Check whether any date difference is consistent**

    ```sql
    SELECT datediff(c.D_DT_START, s.D_DT_START) AS day_difference,
           COUNT(*) AS row_count
    FROM training_nic.migrated.institutions AS c
    JOIN <source_catalog>.<source_schema>.institutions AS s
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

22. **View the table's history**

    ```sql
    DESCRIBE HISTORY training_nic.migrated.institutions;
    ```
    <!-- source: facts_extracted.md §8 -->

23. **Query an earlier version**

    Substitute a version number from the history output.

    ```sql
    SELECT COUNT(*) AS rows_at_version
    FROM training_nic.migrated.institutions VERSION AS OF <version>;
    ```
    <!-- source: facts_extracted.md §8 -->

    > **Note:** History retention is governed by `logRetentionDuration`, 30 days by default, but data files are retained for 7 days by default. In Databricks Runtime 18.0 and above, a time travel query is blocked if it requests a version older than the deleted-file retention period. Use time travel for recent comparisons, not as an archive.
    <!-- source: facts_extracted.md §8 -->

### Task 7: Document Your Findings

24. **Record each finding in the shared notebook**

    For every discrepancy, write four things: which check surfaced it, how many rows it affects, what the affected rows have in common, and what you believe caused it.

25. **Separate findings from artefacts**

    List separately anything that appeared to be a defect but resolved once you normalised. An engineer needs to know what you ruled out as well as what you found.

26. **State a recommendation**

    Write one sentence per finding saying whether it blocks cutover. Not every difference does.

    > **Expected Result:** A notebook containing your four check results, a numbered list of findings with row counts and suspected causes, a separate list of ruled-out artefacts, and a cutover recommendation.

---

## Stretch Task

For attendees who finish early.

1. Write a single query that reports all four checks as one result set, one row per check, with a pass or fail column. This is the beginning of a reusable validation framework rather than a one-off investigation.
2. Extend the row-level comparison to every text column at once rather than one at a time. What makes this expensive, and why is it the last check rather than the first?
3. You found a difference. Prove it is not caused by the comparison itself — write the query that demonstrates the defect exists independent of your normalisation choices.

---

## Checkpoint: Verify Your Progress

- [ ] I recorded row counts from both the cloud and the source
- [ ] I listed keys present in the source but missing from the cloud
- [ ] I checked the reverse direction for extra rows
- [ ] I grouped the missing keys to look for a shared attribute
- [ ] I compared sums restricted to keys present on both sides
- [ ] I counted nulls and empty strings as separate figures on both sides
- [ ] I ran the row-level comparison **before** normalising and recorded the count
- [ ] I re-ran it with `TRIM` and `NULLIF` applied to both sides
- [ ] I can explain the gap between the naive and normalised counts
- [ ] I checked whether any date difference was consistent across rows
- [ ] I viewed table history and queried an earlier version
- [ ] My notebook separates confirmed findings from ruled-out artefacts
- [ ] Each finding has a row count, a shared attribute, and a suspected cause
- [ ] I stated whether each finding blocks cutover

---

## Troubleshooting Reference

> **Key Insight:** In a comparison exercise, most surprising results are produced by the comparison rather than by the data. Suspect your own query first, particularly when a result claims that almost everything is wrong.
<!-- source: facts_extracted.md §10 -->

| Issue | Symptom | Solution |
|---|---|---|
| Comparison returns nearly every row | Mismatch count close to total row count | Whitespace or null-versus-empty-string. Normalise both sides with `TRIM` and `NULLIF` before comparing. |
| Rows silently missing from a comparison | Fewer rows than expected in the mismatch list | `<>` returns null when either side is null, dropping those rows. Use `IS DISTINCT FROM`. |
| Federated query fails immediately | Error before any rows return | Connection rather than SQL. The connection is always SSL-encrypted and fails at handshake if the certificate hostname does not match the endpoint. |
| Cannot see the source catalog | Catalog absent from Catalog Explorer | Missing traversal grant on the foreign catalog. Ask your instructor. |
| Time travel fails with a version error | Version-not-available error | The requested version is older than the retention window, or the table has only one version. Use a recent version from `DESCRIBE HISTORY`. |
| Sums differ but row counts match | Totals disagree with no missing rows | Expected — this is what Check 3 exists to catch. Investigate the column type rather than the row set. |
| Cast error mid-comparison | Statement fails on a value | Strict typing. Use `TRY_CAST` if null is the outcome you want for unparseable input. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Serverless SQL warehouse | Billed while running | Leave auto-stop enabled. |
| Federated queries | Each query reaches the source system over JDBC | The source is shared by the whole class. Avoid `SELECT *` without `LIMIT` against it. |
| Row-level comparison | Most expensive of the four checks | Run it last, and only after the cheaper checks have narrowed the question. |

**Cleanup:** No tables created. Keep your findings notebook — it is referenced in the Day 2 wrap-up.

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

Answers are held in the Knowledge Check Bank.

---

## Next Steps

Day 2 moves from validating someone else's pipeline to building your own. Lab 4 introduces the DataFrame API and has you produce a summary table that you will publish in Lab 5 and visualise in Lab 6.

---

## Resources

- Run federated queries on Microsoft SQL Server: https://docs.databricks.com/aws/en/query-federation/sql-server
- Connect to external databases and catalogs: https://docs.databricks.com/aws/en/query-federation/
- Work with Delta Lake table history: https://docs.databricks.com/aws/en/delta/history
- datediff function: https://docs.databricks.com/aws/en/sql/language-manual/functions/datediff

---

*Lab 3 Complete*
