# Lab 2: SQL Translation, Cross-Schema Joins, and Parameters

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 45 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

You already write SQL. This lab is not SQL instruction — it is a translation exercise. You will convert T-SQL constructs into Databricks SQL, join tables that live in two different schemas in one statement, and then make the query reusable by a colleague who does not write SQL at all.

---

## Prerequisites

- [ ] Lab 1 completed — you can locate `training_nic.migrated` and run a query
- [ ] A running serverless SQL warehouse selected in the SQL editor
- [ ] GitLab repository URL and credentials supplied by your instructor
- [ ] Your assigned attendee ID

---

## Objectives

- Translate at least four T-SQL constructs into their Databricks SQL equivalents
- Explain why a cast that succeeded in SQL Server may raise an error here
- Join tables across two schemas in a single statement
- Add named parameters so a colleague can run the query without editing SQL
- Commit a saved query to a Databricks Git folder

---

## Part 1: Translate What You Already Know

### Task 1: Set the Session Context

1. **Open the SQL editor and select your warehouse**

    Confirm the compute selector shows a running serverless SQL warehouse.

2. **Set the catalog and schema defaults**

    ```sql
    USE CATALOG training_nic;
    USE SCHEMA migrated;
    ```
    <!-- source: facts_extracted.md §2 -->

### Task 2: Translate T-SQL Constructs

3. **Row limiting**

    In T-SQL you would write `SELECT TOP 100 *`. In Databricks SQL the limit moves to the end of the statement.

    ```sql
    SELECT * FROM institutions LIMIT 100;
    ```
    <!-- source: facts_extracted.md §5 -->

4. **Null handling**

    `ISNULL(a, b)` becomes `COALESCE(a, b)`, which accepts more than two arguments.

    ```sql
    SELECT
      `#ID_RSSD`,
      COALESCE(CITY, 'UNKNOWN') AS CITY_CLEAN
    FROM institutions
    LIMIT 20;
    ```
    <!-- source: facts_extracted.md §5 -->

5. **String length**

    `LEN()` does not exist. Use `LENGTH()`.

    ```sql
    SELECT `#ID_RSSD`, NM_LGL, LENGTH(NM_LGL) AS name_length
    FROM institutions
    LIMIT 20;
    ```
    <!-- source: facts_extracted.md §5 -->

    > **Note:** The key column arrived from the source with a leading `#` in its name, so it must be wrapped in backticks. Without them the statement fails to parse. This is the backtick rule, and it is not optional on this dataset.

6. **Run the same query against the on-premises copy**

    Same query, different schema. Compare the two `name_length` columns.

    ```sql
    SELECT `#ID_RSSD`, NM_LGL, LENGTH(NM_LGL) AS name_length
    FROM training_nic.legacy_onprem.institutions
    LIMIT 20;
    ```
    <!-- source: facts_extracted.md §5 -->

    > **Expected Result:** The cloud copy reports a length matching the visible characters. The on-premises copy reports a **longer** length for the same institution — the name is padded with trailing spaces.

    > **Key Insight:** Fixed-width `CHAR` columns export from SQL Server padded to their declared width. The two systems hold the same name and disagree on its length. Write down what you just saw — in Lab 3 this single difference makes a naive comparison report that almost every row is wrong.

7. **Date arithmetic**

    This is the translation most likely to give you a wrong answer rather than an error. In T-SQL you write `DATEDIFF(day, start, end)`. In Databricks SQL the function is `datediff(endDate, startDate)` — **end date first**, no unit argument, and the result is always in days.

    ```sql
    SELECT datediff('2009-07-31', '2009-07-30') AS forward,
           datediff('2009-07-30', '2009-07-31') AS backward;
    ```
    <!-- source: facts_extracted.md §5 -->

    > **Expected Result:** `forward` returns `1` and `backward` returns `-1`.

    > **Common Pitfall:** Both the T-SQL order and the Databricks order compile and return a number. Only the sign differs. A mistake here does not raise an error — it silently produces a plausible wrong answer.

8. **Strict type casting**

    Run the following and observe that it raises rather than returning null.

    ```sql
    SELECT CAST('not-a-number' AS INT);
    ```
    <!-- source: facts_extracted.md §5 -->

9. **Use TRY_CAST when null is what you want**

    ```sql
    SELECT TRY_CAST('not-a-number' AS INT) AS safe_cast;
    ```
    <!-- source: facts_extracted.md §5 -->

    > **Key Insight:** SQL Server would silently coerce or truncate. Databricks refuses. During a migration this is a feature, not an obstacle — silent coercion is exactly how bad data reaches a report unnoticed.

---

## Part 2: Join Across Schemas

### Task 3: Query Two Schemas in One Statement

10. **Inspect the reference schema**

    The reference data lives in a different schema from the migrated tables.

    ```sql
    SHOW TABLES IN training_nic.reference;
    ```
    <!-- source: facts_extracted.md §1 -->

11. **Write the cross-schema join**

    Because the two tables are in different schemas, at least one side must be fully qualified. This is the equivalent of a cross-database join in SQL Server.

    ```sql
    SELECT
      i.STATE_ABBR_NM,
      COUNT(*)              AS institution_count,
      MAX(c.population)     AS state_population
    FROM training_nic.migrated.institutions  AS i
    JOIN training_nic.reference.state_population AS c
      ON i.STATE_ABBR_NM = c.state_abbr
    GROUP BY i.STATE_ABBR_NM
    ORDER BY institution_count DESC;
    ```
    <!-- source: facts_extracted.md §12 -->

    > **Expected Result:** One row per state, ordered by institution count descending.

    > **Key Insight:** Look at the join condition. `STATE_ABBR_NM` on the left is the native NIC column name, carried through from the source files untouched. `state_abbr` on the right is a curated reference table we control, so it uses a clean name. Joining a raw-named table to a curated one is the normal state of affairs, not a mistake — and it is why the Silver layer exists.

12. **Confirm what made the join possible**

    Note that you did not need a linked server, a synonym, or a cross-database permission. Both schemas sit under the same catalog and you hold the traversal grants on it.

    > **Key Insight:** Reading either table needs three privileges — `SELECT` on the table, `USE CATALOG` on the catalog, and `USE SCHEMA` on each schema. Joining across schemas simply means you need `USE SCHEMA` on both.
    <!-- source: facts_extracted.md §3 -->

---

## Part 3: Make It Reusable

### Task 4: Add Named Parameters

13. **Add a date range parameter pair**

    A parameter marker is a colon followed by the parameter name. Adding one makes a widget appear above the editor where the type and value are set.

    ```sql
    SELECT
      i.STATE_ABBR_NM,
      COUNT(*) AS institution_count
    FROM training_nic.migrated.institutions AS i
    WHERE i.D_DT_START BETWEEN :start_date AND :end_date
    GROUP BY i.STATE_ABBR_NM
    ORDER BY institution_count DESC;
    ```
    <!-- source: facts_extracted.md §6 -->

14. **Set the parameter types**

    Click the gear icon beside each parameter widget and set both `start_date` and `end_date` to type **Date**. The Date type presents a calendar picker and defaults to the current date.
    <!-- source: facts_extracted.md §6 -->

15. **Apply values and run**

    Choose a start and end date that span several years, then select **Apply changes** and run the query.

    > **Expected Result:** The result set changes when you change the dates, without editing any SQL.

16. **Try an invalid range**

    Set `start_date` later than `end_date` and run again.

    > **What Just Happened?** You get zero rows rather than an error. A parameter protects a colleague from editing SQL; it does not protect them from asking a nonsensical question. If this query is going to a non-technical colleague, say so in the query description.

### Task 5: Commit to a Git Folder

17. **Save the query**

    Use **Save** and name the query using your attendee ID, for example `lab2_state_summary_<id>`.

18. **Open the Git folder**

    In the left navigation, select **Workspace**, then open the Git folder your instructor linked to the GitLab repository. Databricks Git folders is a visual Git client that integrates Git repositories directly in the workspace.
    <!-- source: facts_extracted.md §7 -->

19. **Add your query to the folder and commit**

    Create a new file in the Git folder named `lab2_state_summary_<id>.sql`, paste your query text into it, and save. Then use **Commit and Push**. Write a commit message that says what the query answers, not what you changed.

    > **Note:** A saved query in the SQL editor and a file in a Git folder are two different objects. The saved query is convenient for you; the `.sql` file in the repository is what a colleague can pull, review, and change. Version control needs the file.

    > **Note:** This replaces emailing `.sql` files. A colleague pulls the repository and gets your query with its history, rather than a file called `final_v3_USE_THIS.sql`.

20. **Verify the commit**

    Confirm the commit appears in the Git folder history with your message.

---

## Stretch Task

For attendees who finish early.

1. Rewrite the Task 3 join using `USE SCHEMA` so that only one side of the join needs qualification. Which form would you rather hand to a colleague, and why?
2. Add a third parameter for a minimum institution count and apply it with a `HAVING` clause.
3. Using `LENGTH()` and `TRIM()`, find how many rows in `institutions` have a `NM_LGL` whose length changes when trimmed. Record the number — you will need it in Lab 3.

---

## Checkpoint: Verify Your Progress

- [ ] I set the session catalog and schema with `USE CATALOG` and `USE SCHEMA`
- [ ] I converted `SELECT TOP` to `LIMIT`
- [ ] I used `COALESCE` in place of `ISNULL`
- [ ] I used `LENGTH` in place of `LEN`
- [ ] I ran `datediff` and can state which argument comes first
- [ ] I saw `CAST` raise an error and `TRY_CAST` return null
- [ ] I joined `migrated` and `reference` tables in one statement
- [ ] I can name the three privileges required to read a table
- [ ] I added `:start_date` and `:end_date` parameters and set both to type Date
- [ ] The result set changed when I changed parameter values, with no SQL edit
- [ ] I saved the query with my attendee ID in the name
- [ ] I committed the query to the Git folder and saw it in the history

---

## Troubleshooting Reference

> **Key Insight:** Most failures in this lab are dialect differences, not mistakes. A T-SQL construct that errors here usually has a direct equivalent — check the translation reference before rewriting the query.
<!-- source: facts_extracted.md §5 -->

| Issue | Symptom | Solution |
|---|---|---|
| `TOP` rejected | Syntax error near `TOP` | Move the limit to the end as `LIMIT n`. |
| `LEN` not found | Unresolved function | Use `LENGTH`. |
| `ISNULL` behaves unexpectedly | Wrong argument count | Use `COALESCE`, which accepts more than two arguments. |
| Date difference has the wrong sign | Result is negative when you expected positive | Argument order. Databricks takes the end date first. |
| Cast raises an error | Statement fails on a bad value | Expected behaviour. Use `TRY_CAST` if null is the outcome you want. |
| Table not found in the join | Error naming one side | Cross-schema joins need at least one side fully qualified, or `USE SCHEMA` on both. |
| Parameter widget does not appear | No widget above the editor | The marker must be a colon immediately followed by the name, with no space. |
| Parameter returns no rows | Empty result | Check the date range is the right way round and that the type is set to Date, not String. |
| Commit rejected | Push fails | Confirm your Git credentials are configured and that you are on a branch you may write to. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Serverless SQL warehouse | Billed while running; auto-stops when idle | Leave auto-stop enabled. |
| Repeated full scans | Re-running the join while experimenting | Keep `LIMIT` on exploratory queries; remove it only for the final aggregate. |

**Cleanup:** No tables were created. Your saved query and commit are intentional and should be left in place — Lab 5 publishes from work you build on later.

---

## Knowledge Check

1. Write the Databricks SQL equivalent of `SELECT TOP 50 * FROM dbo.Institutions`.
2. `DATEDIFF(day, '2026-01-01', '2026-01-10')` returns 9 in T-SQL. What is the Databricks equivalent, and what happens if you keep the T-SQL argument order?
3. Why does `CAST('abc' AS INT)` raise here but return null in SQL Server, and which is safer during a migration?
4. You joined two tables in different schemas without a linked server. What made that possible, and which privileges did you need?
5. A colleague who does not write SQL needs to run your query for any date range. What did you add, and what does it not protect them from?
6. Why is committing a query to a Git folder better than emailing the `.sql` file?

Answers are held in the Knowledge Check Bank.

---

## Next Steps

Lab 3 is the first real investigation. You will compare the migrated data against the on-premises source of truth using a four-check framework, and you will find that they do not agree. The trailing-whitespace observation from step 5 and the stretch task will both matter.

---

## Resources

- Query parameters: https://docs.databricks.com/aws/en/sql/user/queries/query-parameters
- datediff function: https://docs.databricks.com/aws/en/sql/language-manual/functions/datediff
- Databricks Git folders: https://docs.databricks.com/aws/en/repos/
- Unity Catalog privileges: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges

---

*Lab 2 Complete*
