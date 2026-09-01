# Lab 5: Publishing Datasets for Self-Service

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 40 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

You have a summary table. A colleague needs it. The old answer was to email a spreadsheet, or ask an engineer to build a view and grant access. This lab is the cloud version: publish it yourself, grant access deliberately, and set an alert so you find out before your stakeholders do when something breaks.

---

## Prerequisites

- [ ] Lab 4 completed — `training_nic.analyst_<id>.institution_summary` exists
- [ ] A running serverless SQL warehouse selected in the SQL editor
- [ ] The attendee ID of a **partner** in the room, who will verify your grant
- [ ] Your own email address for the alert notification

---

## Objectives

- Create a view over a table you own
- Grant the three privileges a colleague needs to read it
- Verify access from the other side rather than assuming it works
- Explain when a materialized view is worth its refresh cost
- Create an alert that fires on a row-count threshold

---

## Part 1: Publish a View

### Task 1: Create the View

1. **Set your session context**

    ```sql
    USE CATALOG training_nic;
    USE SCHEMA analyst_<id>;
    ```
    <!-- source: facts_extracted.md §2 -->

2. **Create a view over your Lab 4 table**

    A view is a saved query. It stores no data and is always current.

    ```sql
    CREATE OR REPLACE VIEW institution_summary_published AS
    SELECT CHTR_TYPE_CD, start_year, institution_count, distinct_cities
    FROM training_nic.analyst_<id>.institution_summary
    WHERE institution_count > 0;
    ```
    <!-- source: facts_extracted.md §12 -->

3. **Query it to confirm**

    ```sql
    SELECT * FROM institution_summary_published
    ORDER BY start_year DESC
    LIMIT 20;
    ```
    <!-- source: facts_extracted.md §2 -->

    > **Key Insight:** The view gives colleagues a live window onto your logic. If you fix the underlying query, everyone sees the fix immediately — there is no copy to go stale, and no second version circulating by email.

### Task 2: Grant Access — and Watch It Fail First

4. **Grant SELECT on the view only**

    Do this first, deliberately, even though it is not sufficient. Substitute your partner's identity.

    ```sql
    GRANT SELECT ON VIEW institution_summary_published TO `<partner>`;
    ```
    <!-- source: facts_extracted.md §3 -->

5. **Ask your partner to query it**

    Have your partner run the query below. It will fail.

    ```sql
    SELECT * FROM training_nic.analyst_<id>.institution_summary_published LIMIT 5;
    ```
    <!-- source: facts_extracted.md §3 -->

    > **What Just Happened?** Your partner has `SELECT` on the object and still cannot read it. This is the single most common Unity Catalog support ticket, and it looks like a bug. It is not.
    <!-- source: facts_extracted.md §3 -->

6. **Grant the traversal privileges**

    Reading a table needs three privileges, not one: `SELECT` on the object, `USE CATALOG` on the catalog, and `USE SCHEMA` on the schema.

    ```sql
    GRANT USE CATALOG ON CATALOG training_nic TO `<partner>`;
    GRANT USE SCHEMA  ON SCHEMA  training_nic.analyst_<id> TO `<partner>`;
    ```
    <!-- source: facts_extracted.md §3 -->

7. **Have your partner retry**

    > **Expected Result:** The same query now returns rows, with no change to the `SELECT` grant.

8. **Inspect the grants**

    ```sql
    SHOW GRANTS ON VIEW institution_summary_published;
    ```
    <!-- source: facts_extracted.md §3 -->

9. **Act as the partner for someone else**

    Swap roles and verify your partner's view from your own account. Confirming from the other side is the only way to know a grant actually worked.

    > **Key Insight:** `USE CATALOG` does not grant access to anything by itself. It is a traversal privilege — permission to walk through the namespace. That is why granting `SELECT` alone leaves a colleague stuck.
    <!-- source: facts_extracted.md §3 -->

---

## Part 2: When a View Is Not Enough

### Task 3: Consider a Materialized View

10. **Create a materialized view over the same query**

    ```sql
    CREATE OR REPLACE MATERIALIZED VIEW institution_summary_mv AS
    SELECT CHTR_TYPE_CD, start_year, institution_count, distinct_cities
    FROM training_nic.analyst_<id>.institution_summary
    WHERE institution_count > 0;
    ```
    <!-- source: facts_extracted.md §14 -->

11. **Query it and compare response time against the plain view**

    ```sql
    SELECT * FROM institution_summary_mv ORDER BY start_year DESC LIMIT 20;
    ```
    <!-- source: facts_extracted.md §14 -->

12. **Refresh it explicitly**

    ```sql
    REFRESH MATERIALIZED VIEW institution_summary_mv;
    ```
    <!-- source: facts_extracted.md §14 -->

    > **Note:** A materialized view is a Unity Catalog managed table that physically stores query results. Databricks automatically creates and runs a serverless pipeline to process the refresh, and the size of your warehouse does not limit that compute — cost scales with data volume, not warehouse size.
    <!-- source: facts_extracted.md §14 -->

13. **Decide which one you would publish**

    Write down which you would give a colleague and why.

    > **Key Insight:** A view costs nothing until queried and is never stale. A materialized view costs compute on every refresh and can be stale between them, but returns fast. That trade — freshness against query latency — is the entire decision. For a dashboard refreshed hourly and read hundreds of times, the materialized view wins. For a query run twice a week, it does not.
    <!-- source: facts_extracted.md §14 -->

    > **Common Pitfall:** Materialized views do not support time travel. If someone needs to query the published dataset as it looked last Tuesday, a materialized view cannot answer that.
    <!-- source: facts_extracted.md §14 -->

---

## Part 3: Know Before Your Stakeholders Do

### Task 4: Create a Row-Count Alert

14. **Open the alert editor**

    Click the **Alerts** icon in the sidebar and select **Create Alert**.
    <!-- source: facts_extracted.md §15 -->

15. **Author the query inside the alert**

    You cannot point an alert at a query you saved earlier — each alert owns its own query definition, authored in the alert editor.

    ```sql
    SELECT COUNT(*) AS row_count
    FROM training_nic.analyst_<id>.institution_summary;
    ```
    <!-- source: facts_extracted.md §15 -->

    > **Common Pitfall:** Attendees routinely try to reuse the saved query from Lab 2 or the view from Task 1. The alert editor will not let you. Write the query here.

16. **Test the query**

    Click **Run all (1000)** and confirm a single row returns.
    <!-- source: facts_extracted.md §15 -->

17. **Select a warehouse**

    Use the compute selector to choose the serverless SQL warehouse that will run the alert on schedule.
    <!-- source: facts_extracted.md §15 -->

18. **Configure the condition**

    In the **Condition** field, set the alert to trigger when the row count falls below a threshold you choose — pick a number just under the current count so you can see it work.
    <!-- source: facts_extracted.md §15 -->

19. **Test the condition**

    Click **Test condition** and confirm the preview behaves as you expect.
    <!-- source: facts_extracted.md §15 -->

20. **Add yourself as a recipient**

    In the **Notifications** section, search for and select your username.
    <!-- source: facts_extracted.md §15 -->

21. **Set the schedule**

    Click the calendar icon and set a frequency. For the lab, choose the shortest interval available so you can observe a run.
    <!-- source: facts_extracted.md §15 -->

    > **Note:** Ticking **Show cron syntax** in the schedule dialog lets you edit the schedule directly using Quartz Cron syntax, which is what you would use for anything more specific than a fixed interval.

22. **Save the alert**

    Click **View alert** to save and review it.
    <!-- source: facts_extracted.md §15 -->

23. **Confirm the status**

    An alert reports one of three states: `OK`, `TRIGGERED`, or `ERROR`. Note which one yours shows and why.
    <!-- source: facts_extracted.md §15 -->

    > **Expected Result:** A saved alert with your query, a threshold condition, yourself as recipient, a schedule, and a current status.

---

## Stretch Task

For attendees who finish early.

1. Set the threshold deliberately so the alert fires, and observe the state change to `TRIGGERED`. What would a stakeholder have seen instead if you had no alert?
2. Revoke your partner's `USE SCHEMA` while leaving `SELECT` in place. Confirm the failure returns, then explain to them exactly which of the three grants is missing.
3. Write the `SHOW GRANTS` statements needed to audit all three levels — catalog, schema, and view — and describe how you would find a permission gap using only their output.

---

## Checkpoint: Verify Your Progress

- [ ] I created a view over my Lab 4 summary table
- [ ] I queried the view successfully myself
- [ ] I granted `SELECT` alone and confirmed my partner still could not read it
- [ ] I can explain why that failure is expected rather than a bug
- [ ] I granted `USE CATALOG` and `USE SCHEMA`
- [ ] My partner successfully queried my view after the traversal grants
- [ ] I verified a partner's view from my own account
- [ ] I ran `SHOW GRANTS` and read the output
- [ ] I created a materialized view over the same query
- [ ] I refreshed the materialized view explicitly
- [ ] I recorded which of the two I would publish, and why
- [ ] I authored an alert query inside the alert editor
- [ ] I configured a threshold condition and tested it
- [ ] I added myself as a notification recipient and set a schedule
- [ ] I recorded the alert's current status

---

## Troubleshooting Reference

> **Key Insight:** Almost every access failure in Unity Catalog is a missing traversal grant rather than a missing `SELECT`. Check `USE SCHEMA` first — it is the one people forget.
<!-- source: facts_extracted.md §3 -->
<!-- source: facts_extracted.md §14 -->
<!-- source: facts_extracted.md §15 -->

| Issue | Symptom | Solution |
|---|---|---|
| Partner cannot query the view | Access denied despite a `SELECT` grant | Missing `USE CATALOG` or `USE SCHEMA`. Run `SHOW GRANTS` at each level to find the gap. |
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

**Cleanup:** Keep the view — Lab 6 builds a dashboard on it. **Delete the materialized view and set the alert to a long interval or pause it**, so neither keeps consuming after class.

---

## Knowledge Check

1. Name the three privileges required for a colleague to read your view, and state which one is most often forgotten.
2. Your partner has `SELECT` and still gets access denied. What do you check, and in what order?
3. When is a materialized view worth its refresh cost, and when is a plain view the better answer?
4. What can a plain view do that a materialized view cannot?
5. Why can you not point an alert at a query you saved earlier?
6. What three statuses can an alert report, and what does each mean?
7. Publishing a view replaced emailing a spreadsheet. Name two things that improved beyond convenience.

Answers are held in the Knowledge Check Bank.

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
