# Lab 1: Orientation and Your First Query

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 45 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

This data lived in an on-premises SQL Server for years. It now lives in Databricks. This lab answers the only question that matters on day one: where is everything, and can I still find it? You will locate the tables your team owns, run a query against them, and compare one figure to the number you already know from on-premises.

---

## Prerequisites

- [ ] Workspace URL, username, and password provided by your instructor
- [ ] Your assigned attendee ID (used to name your personal schema)
- [ ] A browser signed out of any other Databricks workspace
- [ ] **[`SETUP.md`](../../../SETUP.md) Parts 1 and 2 completed**—course material added as a Git folder, SQL warehouse selected
- [ ] **[Lab 0: Set Up Your Workspace](Lab_0_Guide.md) completed**—your `training_nic` catalog is built (verify: `SELECT COUNT(*) FROM training_nic.migrated.institutions` returns 61,699)

> **Note:** If you have not added the course repository to your workspace yet, do that first—it takes about two minutes and puts every lab guide inside Databricks, next to the query you are running. `SETUP.md` Part 1 walks through it. The repository is public, so you will not be asked for a token or a password.

---

## Objectives

- Sign in to a Databricks workspace and start a SQL warehouse
- Navigate Unity Catalog using Catalog Explorer to locate a migrated table
- Read the three-level namespace and explain how it maps to the two-level naming most databases use
- Run a `SELECT` query in the SQL editor
- Export a result set and compare one value against the on-premises figure
- Locate Recents, Search, your home folder, Compute, and Query History

---

## Part 1: Find Your Data

### Task 1: Sign In and Start Compute

1. **Sign in to the workspace**

    Open the workspace URL supplied by your instructor and sign in. You land on the workspace home page.

2. **Open the SQL editor**

    In the left navigation, select **SQL Editor**.

3. **Select a SQL warehouse**

    In the compute selector at the top right of the editor, choose **Serverless Starter Warehouse** (your instructor will say if this class uses a different one). If it shows as stopped, selecting it starts it.

    > **Note:** A SQL warehouse is the compute that runs your queries. Nothing runs without one selected. Serverless warehouses start in seconds; a classic warehouse can take several minutes.

    > **Troubleshooting:** If queries appear to hang and never start, check this selector before assuming a problem with your SQL. A stopped or still-starting warehouse is the most common cause.

### Task 2: Browse Unity Catalog

4. **Open Catalog Explorer**

    In the left navigation, select **Catalog**. This opens Catalog Explorer, which provides a UI to explore and manage data, schemas, tables, and other assets.
    <!-- source: facts_extracted.md §1 -->

5. **Locate the training catalog**

    In the catalog list, expand **`training_nic`**. You will see several schemas. The one holding the data migrated from SQL Server is **`migrated`**.

    > **Don't see `training_nic`?** You have not run the setup yet. Go back and complete [Lab 0: Set Up Your Workspace](Lab_0_Guide.md), then refresh Catalog Explorer.

    <!-- source: facts_extracted.md §1 -->

6. **Expand the migrated schema**

    Expand `migrated` and review the tables listed. These are the cloud copies of the tables your team queried on-premises.

7. **Inspect a table's schema**

    Select the **institutions** table and review the **Columns** tab. Note the column names and data types.

    > **Note:** If there is no **institutions** table, the setup has not been run—go back to [Lab 0](Lab_0_Guide.md). Every remaining step in this lab refers to this table.

    > **Key Insight:** Unity Catalog uses a three-level namespace—`catalog.schema.table`—where most databases use two—`schema.table` in PostgreSQL or MySQL, `dbo.TableName` in SQL Server. The catalog level is new. Everything you reference will have three parts unless you set a session default.

    <!-- source: facts_extracted.md §2 -->

8. **Note the full name of the table**

    The fully qualified name is `training_nic.migrated.institutions`. You will use it in the next task.

---

## Part 2: Query and Compare

### Task 3: Run Your First Query

9. **Return to the SQL editor**

    Select **SQL Editor** in the left navigation. Confirm your warehouse is still selected.

10. **Set a session default**

    Run the following so you do not have to type the catalog and schema on every query.

    ```sql
    USE CATALOG training_nic;
    USE SCHEMA migrated;
    ```

    <!-- source: facts_extracted.md §2 -->

    > **Note:** This is the equivalent of `USE database` in most SQL dialects, except it takes two statements because there is one more level in the namespace.

11. **Run a simple SELECT**

    Retrieve the first rows so you can see the shape of the data.

    ```sql
    SELECT * FROM institutions LIMIT 100;
    ```

    <!-- source: facts_extracted.md §5 -->

    > **Common Pitfall:** `SELECT TOP 100 *` is T-SQL and will fail here. `LIMIT` goes at the end of the statement, not the beginning.

12. **Count the rows**

    ```sql
    SELECT COUNT(*) AS row_count FROM institutions;
    ```

    <!-- source: facts_extracted.md §5 -->

    > **Expected Result:** A single row with one column, `row_count`, containing **61,699**.

### Task 4: Export and Compare

13. **Export the result**

    Above the result grid, click the **arrow on the Table tab** (left of the **+** icon) and select **Download CSV**.

14. **Compare against the on-premises figure**

    On-premises, this table had **62,080** rows. Your count is **61,699**. They do not match. Do not try to explain the difference yet.

    > **What Just Happened?** One count, two systems—the smallest possible unit of migration validation. The gap is real and deliberate. Lab 3 is where you find out what the migration did wrong.

15. **Save your query**

    Click the query name in the header, rename it to **`lab1_row_count_<id>`** using your attendee ID, then click **Save**. When asked where to move the file, keep your home folder. Saved queries persist and can be shared with colleagues, which replaces emailing `.sql` files.

    > **Note:** Every lab names saved work `lab<number>_<purpose>_<id>`. Stick to the convention—six labs of ad-hoc names cannot be sorted.

---

## Part 3: Know Your Way Around

### Task 5: The Ten-Minute Tour

Everything you just did left traces. This task shows you where they landed—and where you will work for the rest of the course.

16. **Home and Recents**

    Click the Databricks logo (top left). The **Recents** list shows everything you touched today, including your saved query. This is the fastest way back tomorrow morning.

17. **Search**

    Click the search bar at the top and type `institutions`. Tables, queries, and dashboards all surface here. Once you know a name, search beats navigation.

18. **Workspace**

    In the left navigation, select **Workspace**, then **Home**. Your `lab1_row_count_<id>` file is here. Deleted items go to **Trash** (also in this panel), recoverable for 30 days.

19. **Compute**

    Select **Compute**, then the **SQL warehouses** tab. Find your warehouse and its state. This page answers two questions you will eventually ask: "is anything running?" and "who is paying for it?"

20. **Query History**

    Select **Query History**. Every query you ran today is listed with its status and duration. Check here before rerunning anything that "didn't work"—it usually did.

21. **Read the signposts**

    Find these in the left navigation, but do not click through yet: **Dashboards** and **Genie** (you build these in Lab 6), **Alerts** (Lab 5), and **Marketplace** (third-party datasets you can browse without asking IT).

---

## Stretch Task

For attendees who finish early.

Explore the other schemas in `training_nic`. Using Catalog Explorer, answer:

- Which schema appears to hold data that came out of the on-premises SQL Server, rather than the cloud copy?
- Which schema holds reference or lookup data?
- Open the **History** tab on the institutions table. How many versions does it have, and what does that suggest about how it was created?

Keep your answers in mind—they will be relevant in Lab 3.

---

## Checkpoint: Verify Your Progress

- [ ] I signed in to the workspace and selected a running SQL warehouse
- [ ] I opened Catalog Explorer and expanded the `training_nic` catalog
- [ ] I located the `migrated` schema and listed its tables
- [ ] I inspected a table's columns and data types
- [ ] I can state what the three levels of `training_nic.migrated.institutions` mean
- [ ] I ran `USE CATALOG` and `USE SCHEMA` successfully
- [ ] I ran a `SELECT` with `LIMIT` and saw results
- [ ] I produced a row count for the institutions table (61,699)
- [ ] I exported the result set as CSV
- [ ] I saw that the cloud count (61,699) does not match the on-premises figure (62,080)
- [ ] I saved my query as `lab1_row_count_<id>`
- [ ] I can find Recents, Search, my home folder, Compute, and Query History without help

---

## Troubleshooting Reference

> **Key Insight:** Reading a table needs three privileges, not one—`SELECT` on the table, `USE CATALOG` on the parent catalog, and `USE SCHEMA` on the parent schema. A `SELECT` grant on its own produces an access-denied error that looks like a bug but is not.
<!-- source: facts_extracted.md §3 -->

| Issue | Symptom | Solution |
|---|---|---|
| Cannot see the catalog | `training_nic` does not appear in Catalog Explorer | Missing traversal grant. Ask your instructor to rerun the grants. |
| Query never starts | Query sits queued and no results appear | The SQL warehouse is stopped or still starting. Check the compute selector at the top right of the editor. |
| `SELECT TOP` fails | Syntax error near `TOP` | `TOP` is T-SQL. Use `LIMIT n` at the end of the statement. |
| Table not found | Error naming the table | You are missing a namespace level. Either fully qualify as `training_nic.migrated.<table>` or run both `USE CATALOG` and `USE SCHEMA`. |
| A cast that worked in your old database errors | Cast raises rather than returning null | Expected. Databricks does not silently coerce invalid values. Use `TRY_CAST` if null is the outcome you want. |
| Export control not visible | No download option above results | The query must have completed and returned rows. Rerun and wait for the result grid. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Serverless SQL warehouse | Billed while running; auto-stops when idle | Leave the warehouse to auto-stop. Do not disable auto-stop. |
| Query volume | `SELECT *` on a large table scans more than needed | Use `LIMIT` while exploring, and select only the columns you need. |

**Cleanup:** Nothing to remove. You created no tables in this lab. Leave the warehouse to stop on its own.

---

## Knowledge Check

1. Most databases use two-level names—`schema.table` or `dbo.TableName`. What are the three parts of the equivalent name in Unity Catalog, and what does the extra level represent?
2. You have been granted `SELECT` on a table but still cannot query it. What is the most likely missing privilege?
3. A colleague's query has been "running" for four minutes with no result. What should you check before looking at their SQL?
4. Why does `SELECT TOP 100 * FROM institutions` fail, and what is the correct form?
5. Your cloud row count does not match the on-premises figure. Name two things that could cause this that are not a data problem.

Answers — including worked stretch task answers — are in [`answers/Lab_1_Answers.md`](../answers/Lab_1_Answers.md). Attempt the questions before opening it. Runnable completed files for this lab live in [`completed/Lab_1/`](../completed/Lab_1/).

---

## Next Steps

Lab 2 builds directly on the navigation and querying you just did. You will translate T-SQL constructs into Databricks SQL, join tables across two schemas in a single statement, and add parameters so a colleague can run your query without editing it.

---

## Resources

- Catalog Explorer: https://docs.databricks.com/aws/en/catalog-explorer/
- Databricks technical terminology glossary: https://docs.databricks.com/aws/en/resources/glossary
- Unity Catalog privileges: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
- Databricks on AWS documentation: https://docs.databricks.com/aws/en/

---

*Lab 1 Complete*
