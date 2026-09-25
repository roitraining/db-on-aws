# Lab 5: Publishing Datasets for Self-Service

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 40 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

You have a summary table. A colleague needs it. The old answer was to email a spreadsheet, or ask an engineer to build a view and grant access. This lab is the cloud version: publish it yourself, grant access deliberately, and set an alert so you find out before your stakeholders do when something breaks.

---

## Prerequisites

- [ ] Lab 3 completed—`training_nic.analyst.validation_runs` holds at least one run
- [ ] Lab 4 completed—`training_nic.analyst.institution_summary` exists
- [ ] The serverless SQL warehouse available—this lab's notebook runs on it
- [ ] Your own email address for the alert notification

---

## Objectives

- Build the publish sequence as a re-runnable SQL notebook on a SQL warehouse
- Create a view over a table you own
- Grant the three privileges a colleague needs to read it
- Audit a complete access chain across catalog, schema, and view with `SHOW GRANTS`
- Explain when a materialized view is worth its refresh cost
- Create an alert that fires on a row-count threshold
- Create a second alert that watches the latest validation run for failures
- Schedule the validation runbook as a daily Lakeflow Job from the notebook's Schedule button

---

## Part 1: Publish a View

### Task 1: Create the View

1. **Create a SQL notebook for this lab**

    Lab 4's pipeline lived in a notebook so it could be re-run. Publishing deserves the same: the view and its grants are something you will want to recreate on demand—after an environment rebuild, a permissions drift, or for the next colleague—so this lab builds them as a saveable, re-runnable notebook rather than one-off statements in the SQL editor.

    1. In the left sidebar, click **Workspace**, open **Users → your.email**, click **Create** at the top right, and choose **Notebook**.
    2. Rename it `Lab 5 - Publish Institution Summary View` by clicking the title. Name notebooks for what they do—a colleague finding this in your folder should know its job without opening it.
    3. In the language selector next to the title, choose **SQL**.
    4. In the compute selector at the top right, attach the **serverless SQL warehouse**—a notebook can run on a warehouse when every cell is SQL, and it is the same warehouse Labs 1–3 used.

    Each numbered step below is a **new cell**. Add them in order, top to bottom—by the end of Part 2 the notebook is a runbook that rebuilds your published view and its permissions in one click.

2. **Set your session context**

    ```sql
    -- work in your schema
    USE CATALOG training_nic;
    USE SCHEMA analyst;
    ```

    > **Troubleshooting:** If `USE SCHEMA analyst` fails, the schema does not exist yet—it is created in Lab 4, Part 3 (the `CREATE SCHEMA` cell). Finish that step first; this whole lab builds on the `institution_summary` table that lives there.

3. **Create a view over your Lab 4 table**

    A view is a saved query. It stores no data and is always current.

    ```sql
    -- the published interface: a view over your Lab 4 summary table
    CREATE OR REPLACE VIEW institution_summary_published AS
    SELECT CHTR_TYPE_CD, start_month, institution_count, distinct_cities
    FROM training_nic.analyst.institution_summary
    WHERE institution_count > 0;
    ```
    <!-- source: facts_extracted.md §12 -->

4. **Query it to confirm**

    ```sql
    SELECT * FROM institution_summary_published
    ORDER BY start_month DESC
    LIMIT 20;
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Key Insight:** The view gives colleagues a live window onto your logic. If you fix the underlying query, everyone sees the fix immediately—there is no copy to go stale, and no second version circulating by email.

### Task 2: Grant Access—and Audit Why SELECT Alone Fails

5. **Grant SELECT on the view**

    You are the only person in this workspace, so the stand-in for a colleague is the built-in **`account users`** group—every user in the account. The grant mechanics are identical to granting one person.

    ```sql
    -- read on the view alone — deliberately incomplete
    GRANT SELECT ON VIEW institution_summary_published TO `account users`;
    ```
    <!-- source: facts_extracted.md §3 -->

6. **Audit what that grant actually allows**

    Reading a table needs three privileges, not one: `SELECT` on the object, `USE CATALOG` on the catalog, and `USE SCHEMA` on the schema. Audit all three levels:

    ```sql
    SHOW GRANTS ON VIEW institution_summary_published;
    ```

    ```sql
    SHOW GRANTS ON SCHEMA training_nic.analyst;
    ```

    ```sql
    SHOW GRANTS ON CATALOG training_nic;
    ```
    <!-- source: facts_extracted.md §3 -->

    > **Expected Result:** `account users` holds `SELECT` at the view level and appears **nowhere** at the schema or catalog level.

    > **What Just Happened?** A colleague holding that `SELECT` would still get access denied—the traversal privileges are missing. This is the single most common Unity Catalog support ticket, and it looks like a bug. It is not.
    <!-- source: facts_extracted.md §3 -->

7. **Grant the traversal privileges**

    ```sql
    -- the traversal privileges that complete the chain
    GRANT USE CATALOG ON CATALOG training_nic TO `account users`;
    GRANT USE SCHEMA  ON SCHEMA  training_nic.analyst TO `account users`;
    ```
    <!-- source: facts_extracted.md §3 -->

8. **Re-audit all three levels**

    Re-run the three `SHOW GRANTS` statements from step 6.

    > **Expected Result:** `account users` now appears at every level—view, schema, and catalog. That is a complete access chain: a reader holding all three can query the view; a reader missing any one of them cannot.

    > **Key Insight:** `USE CATALOG` does not grant access to anything by itself. It is a traversal privilege—permission to walk through the namespace. That is why granting `SELECT` alone leaves a colleague stuck.
    <!-- source: facts_extracted.md §3 -->

    > **Note:** In a shared workspace you would prove this from the other side—a colleague runs the query, watches it fail on `SELECT` alone, and succeed after the traversal grants. Everyone here runs an isolated account, so that cross-check is not possible; your instructor may demonstrate it in the shared class workspace.

9. **Prove the runbook: run it top to bottom**

    Click **Run all** at the top of the notebook. Every cell should succeed in order with no edits—including the grants, which are idempotent: granting a privilege a principal already holds is a no-op, not an error.

    > **Key Insight:** This is what the notebook buys you over pasting statements into the SQL editor one at a time: the publish sequence is now an artifact. If the view is dropped, permissions drift, or a new colleague needs the same access, **Run all** rebuilds everything. And like the query in Lab 2, this notebook can be committed to a Git folder.

---

## Part 2: When a View Is Not Enough

### Task 3: Build and Compare a Materialized View

10. **Create a separate scratch notebook for the comparison**

    This comparison does not belong in your publish runbook—the runbook is a durable artifact that rebuilds the view and its grants, while this is an experiment whose output you will delete at the end of the lab. Keeping experiments out of runbooks is what keeps runbooks trustworthy.

    Create a second SQL notebook the same way as step 1, name it `Lab 5 - Compare Materialized View`, attach the same serverless SQL warehouse, and set the context in its first cell:

    ```sql
    -- work in your schema
    USE CATALOG training_nic;
    USE SCHEMA analyst;
    ```

    > **Note:** A scratch notebook is not the only defensible surface for one-off work like this. A saved query in the SQL editor fits too—especially if the result is headed for a dashboard, since dashboard datasets are authored as queries (Lab 6 does exactly that). The working taxonomy: **repeatable process → notebook runbook; quick exploration → scratch notebook or editor query; dashboard feed → dataset query.** Choosing the surface on purpose is the skill.

11. **Create a materialized view over the same query**

    ```sql
    -- same query, precomputed and stored — the freshness-vs-speed trade
    CREATE OR REPLACE MATERIALIZED VIEW institution_summary_mv AS
    SELECT CHTR_TYPE_CD, start_month, institution_count, distinct_cities
    FROM training_nic.analyst.institution_summary
    WHERE institution_count > 0;
    ```
    <!-- source: facts_extracted.md §14 -->

12. **Query it and compare response time against the plain view**

    ```sql
    SELECT * FROM institution_summary_mv ORDER BY start_month DESC LIMIT 20;
    ```
    <!-- source: facts_extracted.md §14 -->

13. **Refresh it explicitly**

    ```sql
    -- recompute the stored results
    REFRESH MATERIALIZED VIEW institution_summary_mv;
    ```
    <!-- source: facts_extracted.md §14 -->

    > **Note:** A materialized view is a Unity Catalog managed table that physically stores query results. Databricks automatically creates and runs a serverless pipeline to process the refresh, and the size of your warehouse does not limit that compute—cost scales with data volume, not warehouse size.
    <!-- source: facts_extracted.md §14 -->

14. **Decide which one you would publish**

    Write down which you would give a colleague and why.

    > **Key Insight:** A view costs nothing until queried and is never stale. A materialized view costs compute on every refresh and can be stale between them, but returns fast. That trade—freshness against query latency—is the entire decision. For a dashboard refreshed hourly and read hundreds of times, the materialized view wins. For a query run twice a week, it does not.
    <!-- source: facts_extracted.md §14 -->

    > **Common Pitfall:** Materialized views do not support time travel. If someone needs to query the published dataset as it looked last Tuesday, a materialized view cannot answer that.
    <!-- source: facts_extracted.md §14 -->

---

## Part 3: Know Before Your Stakeholders Do

### Task 4: Create a Row-Count Alert

15. **Open the alert editor**

    Leave your notebook—alerts cannot live in it, because each alert owns its own query definition (next step). In the left sidebar, click **Alerts**, then select **Create Alert**. The new alert opens with a placeholder title—click it and rename the alert `lab5_row_count_alert`, the same name-it-for-what-it-does rule as your notebooks.
    <!-- source: facts_extracted.md §15 -->

16. **Author the query inside the alert**

    You cannot point an alert at a query you saved earlier—each alert owns its own query definition, authored in the alert editor.

    ```sql
    -- the alert's query: current size of the published summary
    SELECT COUNT(*) AS row_count
    FROM training_nic.analyst.institution_summary;
    ```
    <!-- source: facts_extracted.md §15 -->

    > **Common Pitfall:** Attendees routinely try to reuse the saved query from Lab 2 or the view from Task 1. The alert editor will not let you. Write the query here.

17. **Test the query**

    Click **Run all (1000)** and confirm a single row returns.
    <!-- source: facts_extracted.md §15 -->

18. **Select a warehouse**

    Use the compute selector to choose the serverless SQL warehouse that will run the alert on schedule.
    <!-- source: facts_extracted.md §15 -->

19. **Configure the condition**

    Your summary table holds **1,309** rows—every attendee's does, because the training data is generated deterministically. In the **Condition** section, set exactly:

    | Setting | Value |
    |---|---|
    | Trigger when | **First row** of `row_count` |
    | Operator | `<` (less than) |
    | Threshold value | `1200` |

    The aggregation dropdown (**Count**, **Sum**, **First row**, **Count distinct**, ...) exists because an alert query can return many rows. Yours returns exactly one, so **First row** is the value itself. Read back: *trigger the alert when the first row of `row_count` < 1,200*. That is safely under the real count, so the alert stays quiet until something actually removes rows.
    <!-- source: facts_extracted.md §15 -->

20. **Test the condition**

    Click **Test condition** and confirm the preview behaves as you expect.
    <!-- source: facts_extracted.md §15 -->

21. **Add yourself as a recipient**

    In the **Notifications** section, search for and select your username.
    <!-- source: facts_extracted.md §15 -->

22. **Set the schedule**

    Click the calendar icon and set a frequency. For the lab, choose the shortest interval available so you can observe a run.
    <!-- source: facts_extracted.md §15 -->

    > **Note:** Checking **Show cron syntax** in the schedule dialog lets you edit the schedule directly using Quartz Cron syntax, which is what you would use for anything more specific than a fixed interval.

23. **Save the alert**

    Click **View alert** to save and review it.
    <!-- source: facts_extracted.md §15 -->

24. **Confirm the status**

    An alert reports one of three states: `OK`, `TRIGGERED`, or `ERROR`. Note which one yours shows and why.
    <!-- source: facts_extracted.md §15 -->

    > **Expected Result:** A saved alert with your query, a threshold condition, yourself as recipient, a schedule, and a current status.

### Task 5: A Second Alert—Watch the Validation Itself

25. **Create the validation-failure alert**

    The row-count alert guards one table. The higher-value watch is on the validation: fire whenever the latest run has failing checks. Create a second alert (**Alerts → Create Alert**), rename it `lab5_validation_failures_alert`, and author its query:

    ```sql
    -- failed checks in the latest validation attempt
    SELECT COUNT(*) AS failed_checks
    FROM training_nic.analyst.validation_runs
    WHERE NOT passed
      AND run_ts = (SELECT MAX(run_ts) FROM training_nic.analyst.validation_runs);
    ```
    <!-- source: facts_extracted.md §15 -->

26. **Set its condition and watch it fire**

    Same mechanics as before: condition = **First row** of `failed_checks`, operator **>** (greater than), threshold **0**. Select the serverless SQL warehouse, add yourself under **Notifications**, set a schedule, and save.

    > **Expected Result:** Unlike the row-count alert, this one goes **TRIGGERED** on its first evaluation—your latest Lab 3 run has failing checks because the migration is genuinely broken. That is the alert doing its job. When engineering ships a fixed migration and your runbook passes, this alert goes quiet on its own.

---

### Task 6: Schedule the Validation—Lakeflow Jobs Without Writing One

27. **Schedule the Lab 3 runbook**

    A published dataset stays trustworthy only if the thing that checks it keeps running. Open your **Lab 3 - Migration Validation Runbook** notebook, click **Schedule** at the top right, then **Add schedule**: name it `daily_migration_validation`, pick a daily cadence, leave the compute serverless, and click **Create**.

    > **What Just Happened?** That button created a **Lakeflow Job**—the same orchestration engineers build pipelines with, wrapped in one click. Every scheduled run executes the whole runbook: four checks, four new verdict rows in `validation_runs`.

28. **Connect the chain**

    Tomorrow, without you touching anything: the job runs, a new attempt lands in `validation_runs`, the migration-health page you will build in Lab 6 grows a bar, and this lab's validation alert re-evaluates. Runbook → schedule → table → alert → dashboard: a monitored, self-refreshing publication chain, and you built every link of it.

    > **Note:** Leave the schedule running during the course—one run a day costs little. After the course, **pause** it from the same Schedule dialog.

---

---

## Stretch Task

For attendees who finish early.

1. Raise the threshold above 1,309 so the alert fires, and observe the state change to `TRIGGERED`. What would a stakeholder have seen instead if you had no alert?
2. Revoke `USE SCHEMA` from `account users` while leaving `SELECT` in place. Re-run the three-level audit and state exactly which grant a reader now lacks and what error they would see.
3. Write the `SHOW GRANTS` statements needed to audit all three levels—catalog, schema, and view—and describe how you would find a permission gap using only their output.

---

## Checkpoint: Verify Your Progress

- [ ] I created a view over my Lab 4 summary table
- [ ] I queried the view successfully myself
- [ ] I granted `SELECT` alone and audited why it is not sufficient
- [ ] I can explain why that failure is expected rather than a bug
- [ ] I granted `USE CATALOG` and `USE SCHEMA`
- [ ] I granted the traversal privileges and re-audited all three levels
- [ ] I built the materialized view in a separate scratch notebook, not in the runbook
- [ ] I refreshed the materialized view explicitly
- [ ] I recorded which of the two I would publish, and why
- [ ] I ran the publish notebook top to bottom and every cell succeeded
- [ ] I named the alert `lab5_row_count_alert` and authored its query inside the alert editor
- [ ] I configured a threshold condition and tested it
- [ ] I added myself as a notification recipient and set a schedule
- [ ] I recorded the alert's current status
- [ ] I created `lab5_validation_failures_alert` and saw it go `TRIGGERED` on the broken migration
- [ ] I scheduled the Lab 3 runbook as a daily Lakeflow Job

---

## Troubleshooting Reference

> **Key Insight:** Almost every access failure in Unity Catalog is a missing traversal grant rather than a missing `SELECT`. Check `USE SCHEMA` first—it is the one people forget.
<!-- source: facts_extracted.md §3 -->
<!-- source: facts_extracted.md §14 -->
<!-- source: facts_extracted.md §15 -->

| Issue | Symptom | Solution |
|---|---|---|
| A reader cannot query the view | Access denied despite a `SELECT` grant | Missing `USE CATALOG` or `USE SCHEMA`. Run `SHOW GRANTS` at each level to find the gap. |
| Grant statement fails | Error naming the principal | Check the exact identity spelling with your instructor; user and group names must match precisely. |
| Materialized view creation is slow | Long-running statement | Expected. It creates and runs a serverless pipeline to populate results. |
| Materialized view is stale | Figures do not match the base table | It stores results and updates on refresh. Run `REFRESH MATERIALIZED VIEW`, or use a plain view if you need live data. |
| Time travel on a materialized view fails | Version query rejected | Materialized views do not support time travel. Use the underlying table. |
| Cannot select a saved query in the alert editor | No option to reuse a query | Expected. Each alert owns its query definition; author it in the editor. |
| Alert shows `ERROR` | Status is neither `OK` nor `TRIGGERED` | The query itself failed. Run it in the editor and fix it before saving. |
| No notification arrives | Alert triggers but no email | Confirm you selected yourself under **Notifications** and that the schedule has actually run at least once. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Serverless SQL warehouse | Billed while running | Leave auto-stop enabled. |
| Materialized view refresh | Runs a serverless pipeline; cost scales with **data volume**, not warehouse size | Refresh on a schedule matched to how often the data actually changes, not as often as possible. |
| Alert schedule | Runs its query on every interval | The lab uses a short interval to demonstrate. In production, match the interval to how quickly you need to know. |

**Cleanup:** Keep the view—Lab 6 builds a dashboard on it. **Delete the materialized view, set the alerts to a long interval or pause them, and pause the runbook schedule after the course**, so neither keeps consuming after class.

---

## Knowledge Check

1. Name the three privileges required for a colleague to read your view, and state which one is most often forgotten.
2. A colleague has `SELECT` and still gets access denied. What do you check, and in what order?
3. When is a materialized view worth its refresh cost, and when is a plain view the better answer?
4. What can a plain view do that a materialized view cannot?
5. Why can you not point an alert at a query you saved earlier?
6. What three statuses can an alert report, and what does each mean?
7. Publishing a view replaced emailing a spreadsheet. Name two things that improved beyond convenience.

Answers — including worked stretch task answers — are in [`answers/Lab_5_Answers.md`](../answers/Lab_5_Answers.md). Attempt the questions before opening it. Runnable completed files for this lab live in [`completed/Lab_5/`](../completed/Lab_5/).

---

## Next Steps

Lab 6 puts a dashboard on the view you just published, shares it with the same partner, and then asks Genie two questions about the data to see whether it can be trusted.

---

## Resources

- Materialized views: https://docs.databricks.com/aws/en/views/materialized
- Databricks SQL alerts: https://docs.databricks.com/aws/en/sql/user/alerts/
- Create an alert: https://docs.databricks.com/aws/en/sql/user/alerts/create
- Unity Catalog privileges: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges

---

*Lab 5 Complete*
