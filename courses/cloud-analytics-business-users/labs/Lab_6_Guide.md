# Lab 6: Dashboards and Genie

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 40 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

The last step is the one stakeholders actually see. You will build a dashboard on the view you published in Lab 5, make it interactive, share it with a colleague, and then ask Genie two questions about the same data—including one where you check whether its answer can be trusted.

---

## Prerequisites

- [ ] Lab 5 completed—`institution_summary_published` exists and your partner can query it
- [ ] A running serverless SQL warehouse
- [ ] Lab 3 completed—`training_nic.analyst.validation_runs` holds at least two run attempts
- [ ] Genie enabled on the workspace

---

## Objectives

- Build an AI/BI Dashboard with two charts over a published view
- Add a filter that cross-filters both charts
- Build a migration-health page over the Lab 3 validation runs, with a KPI counter
- Publish with shared credentials and explain what that means for viewers
- Schedule an email delivery of the published dashboard
- Ask Genie two business questions and verify the SQL behind one answer

---

## Part 1: Build the Dashboard

### Task 1: Create and Connect

1. **Create a new dashboard**

    In the left navigation, select **Dashboards**, then click **Create dashboard**. Click the title to rename it `lab6_dashboard_<id>`.

    The editor has two tabs at the top: **Canvas**, where widgets are laid out, and **Data**, where the datasets behind them are defined.

2. **Add a dataset**

    Open the **Data** tab, choose **Create from SQL**, and paste the query below so the dataset reads from the view you published in Lab 5. Run it to confirm it returns rows.

    ```sql
    SELECT CHTR_TYPE_CD, start_month, institution_count, distinct_cities
    FROM training_nic.analyst.institution_summary_published;
    ```
    <!-- source: facts_extracted.md §12 -->

3. **Confirm the data loads**

    > **Expected Result:** A preview showing rows from your published view. If it is empty, the view is empty—go back to Lab 5 before continuing.

### Task 2: Build Two Charts

4. **Add a bar chart**

    Back on the **Canvas** tab, pick the **visualization widget** from the toolbar at the bottom of the canvas and drag a rectangle where the chart should sit. In the configuration panel on the right, select your dataset, set the visualization type to **Bar**, and put `CHTR_TYPE_CD` on one axis and `institution_count` on the other.

5. **Add a line chart**

    Add a second visualization widget the same way. Set the type to **Line**, with `start_month` on the horizontal axis and `institution_count` on the vertical.

6. **Give both charts titles a stakeholder would understand**

    Rename them in business language rather than column names. `CHTR_TYPE_CD` means nothing to the person reading your dashboard.

    > **Key Insight:** This is the moment the native NIC column names stop being an academic point. They were correct to preserve through raw and Bronze, but nobody outside this room knows what `CHTR_TYPE_CD` is. Presentation is where you translate.

7. **Reuse your Lab 2 report as a second dataset**

    Dashboards are where the queries you have been saving all course pay off. Open your saved query `lab2_state_summary` in another browser tab, copy its SQL (without the parameters), and on the **Data** tab choose **Create from SQL** again:

    ```sql
    SELECT i.STATE_ABBR_NM,
           COUNT(*)          AS institution_count,
           MAX(c.population) AS state_population
    FROM training_nic.migrated.institutions AS i
    JOIN training_nic.reference.state_population AS c
      ON i.STATE_ABBR_NM = c.state_abbr
    GROUP BY i.STATE_ABBR_NM;
    ```

8. **Chart it**

    Back on the **Canvas**, add a third visualization widget on this dataset: a bar chart of `institution_count` by `STATE_ABBR_NM`, renamed into business language. The recurring report you rebuilt from SQL Server in Lab 2 is now a live dashboard tile instead of an emailed result set.


---

## Part 2: Make It Interactive

### Task 3: Add a Cross-Filter

9. **Add a date-range filter**

    Pick the **filter widget** from the same canvas toolbar, place it above the charts, and set its field to `start_month` in the right-hand panel. Dashboards support global, page-level, and widget-level filters.
    <!-- source: facts_extracted.md §16 -->

10. **Scope the filter to both charts**

    Configure the filter so it applies to both widgets rather than one.
    <!-- source: facts_extracted.md §16 -->

11. **Test the interaction**

    Change the filter range and confirm both charts respond together.

    > **Expected Result:** Both charts update from a single filter change, with no editing and no SQL.

12. **Try cross-filtering from a chart**

    Select a bar in the bar chart and observe the effect on the line chart.
    <!-- source: facts_extracted.md §16 -->

    > **What Just Happened?** A stakeholder can now slice your analysis themselves. That is the difference between a report you rerun on request and one that answers follow-up questions without you.

---

## Part 3: The Migration Health Page

### Task 4: Chart the Validation Runs

13. **Add a page**

    At the bottom of the canvas, click the **+** next to the page tab and rename the new page **Migration Health**. One dashboard, two audiences: page one answers business questions, this page answers "can we trust the migration yet?"

14. **Add the validation datasets**

    On the **Data** tab, **Create from SQL** twice. First, the run history—every validation attempt from your Lab 3 runbook:

    ```sql
    SELECT run_ts AS run_attempt,
           SUM(CASE WHEN passed THEN 1 ELSE 0 END)     AS checks_passed,
           SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS checks_failed
    FROM training_nic.analyst.validation_runs
    GROUP BY run_ts;
    ```

    Second, the latest attempt's failure count—the same logic your Lab 5 alert watches:

    ```sql
    SELECT COUNT(*) AS failed_checks
    FROM training_nic.analyst.validation_runs
    WHERE NOT passed
      AND run_ts = (SELECT MAX(run_ts) FROM training_nic.analyst.validation_runs);
    ```

15. **Chart the run history**

    On the **Migration Health** page, add a bar chart on the run-history dataset: `run_attempt` on the horizontal axis, with `checks_passed` and `checks_failed` as two measures. Every Lab 3 **Run all** shows up as one bar group.

16. **Add the failure counter**

    Add a **counter** widget on the latest-failures dataset showing `failed_checks`, titled **Failing checks (latest run)**.

    > **Key Insight:** The alert and this page read the same `validation_runs` table—the alert interrupts you when it breaks, the dashboard shows stakeholders the history. One validation runbook now feeds monitoring and reporting, which is what "repeatable" buys you.

---

## Part 4: Publish, Schedule, and Share

### Task 5: Publish with Shared Credentials

17. **Publish the dashboard**

    Click **Publish** at the top right of the editor. In the publish dialog, keep credentials **embedded**—that is the shared-credentials option. Dashboards can be published with shared or individual data permissions.
    <!-- source: facts_extracted.md §16 -->

    > **Key Insight:** With shared credentials, viewers see the data through your access rather than their own, so everyone sees consistent figures. With individual permissions, each viewer sees only what their own grants allow—which can mean two people looking at the same dashboard and seeing different numbers. Choose deliberately.

18. **Open the published version as a viewer**

    Use the dropdown next to the dashboard title to switch from **Draft** to the **published** version. The filters still work; the editing controls are gone. This is what consumers see.

    > **Note:** Verifying view-only access from a genuinely different user needs a second person in the same workspace—everyone here runs an isolated account, so your instructor may demonstrate it in the shared class workspace.

19. **Schedule an email delivery**

    On the published dashboard, click **Schedule**, then **Add schedule**. Pick a daily cadence, and on the **Subscribers** tab add yourself. Each scheduled run refreshes the dashboard and emails a snapshot to every subscriber—the live replacement for mailing a spreadsheet every Monday. In a shared workspace you would subscribe colleagues; the mechanics are identical.

20. **Note the reach of publishing**

    A published dashboard can be shared with anyone registered to your Databricks account, even if they do not have access to the workspace.
    <!-- source: facts_extracted.md §16 -->

    > **Common Pitfall:** That reach is wider than most people expect. Before publishing anything with shared credentials, be sure the data behind it is data you are entitled to show that broadly.

---

## Part 5: Ask Genie

### Task 6: Two Questions and a Verification

21. **Create a Genie space on the same data**

    In the left sidebar, click **Genie**, then click the **New** button on the Genie page. Name the space `lab6_genie_<id>`, select your published view `training_nic.analyst.institution_summary_published` as its data, and choose the serverless SQL warehouse when prompted. The space opens with a chat box—this is where you ask your questions.

22. **Ask your first business question**

    Ask something a stakeholder would genuinely ask, in plain English—for example, which charter type has grown the most in the last twenty years.
    <!-- source: facts_extracted.md §16 -->

23. **Read the generated SQL, not just the answer**

    Expand the SQL Genie produced. Check that it queries the columns you expect and applies the filter you meant.

24. **Ask a second question that is harder to answer**

    Ask something ambiguous or requiring a judgement—for example, which states are underserved relative to population.

25. **Verify that answer against your own query**

    Write the SQL yourself and compare results.
    <!-- source: facts_extracted.md §16 -->

    > **What Just Happened?** If the two disagree, Genie is not broken and neither are you. It answered the question it understood, which may not be the question you asked. "Underserved" has no definition in the data—Genie had to invent one.

26. **Record when you would and would not trust it**

    Write two sentences: one describing a question you would let Genie answer unsupervised, and one describing a question you would always verify.

    > **Key Insight:** Genie is trustworthy in proportion to how well-defined the question is. A question with an unambiguous answer in the data is safe. A question requiring a definition—"best", "underserved", "at risk"—is where it will confidently supply its own.

---

## Stretch Task

For attendees who finish early.

1. Republish the dashboard using individual rather than shared permissions and open the published version again. You still see data—explain why, and describe exactly what a viewer holding none of your grants would see on each page.
2. Add a KPI tile showing total institutions, and make it respond to the same filter as the charts.
3. Ask Genie a question you already know the answer to but phrase it ambiguously. Record what it assumed, and how you would rewrite the underlying dataset description so it assumes better next time.

---

## Checkpoint: Verify Your Progress

- [ ] I created a dashboard connected to my published view
- [ ] I built a bar chart and a line chart
- [ ] I renamed both charts into business language
- [ ] I added a filter on `start_month`
- [ ] The filter applies to both charts, not just one
- [ ] I tested cross-filtering by selecting a value in one chart
- [ ] I added a Migration Health page charting every validation attempt
- [ ] The failing-checks counter shows the latest run's failures
- [ ] I charted my Lab 2 report as a dashboard tile
- [ ] I published the dashboard with shared credentials
- [ ] I can explain what shared credentials mean for what a viewer sees
- [ ] I scheduled an email delivery and subscribed myself
- [ ] I opened the published version and confirmed the edit controls are gone
- [ ] I asked Genie two business questions
- [ ] I read the SQL Genie generated for at least one answer
- [ ] I verified one Genie answer against my own query
- [ ] I recorded when I would and would not trust it

---

## Troubleshooting Reference

> **Key Insight:** A dashboard that shows no data almost always points at the dataset query or the underlying grants, not at the chart configuration. Check the dataset preview first.
<!-- source: facts_extracted.md §16 -->

| Issue | Symptom | Solution |
|---|---|---|
| Dashboard shows no data | Empty charts | The dataset query returned nothing. Run it in the SQL editor first. |
| A viewer cannot open the dashboard | Access denied | Publishing and sharing are separate steps. Confirm you did both. |
| A viewer sees different numbers | Figures disagree between viewers | You published with individual rather than shared permissions, so each viewer sees only their own grants. |
| Filter only affects one chart | One chart responds, the other does not | The filter is scoped to a single widget. Change its scope to both. |
| Genie gives a confidently wrong answer | Plausible answer, wrong figures | Working as intended for this lab. Read the generated SQL—it answered a different question from the one you asked. |
| Genie cannot find a column | Question returns nothing useful | The space may not be scoped to your view, or the column names are opaque. Native NIC names are hard for it too. |
| Charts are slow | Long load times | The dashboard runs its dataset query on a warehouse. Confirm one is running and consider a materialized view for a heavily-read dashboard. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Serverless SQL warehouse | Dashboards run their dataset query against a warehouse | Leave auto-stop enabled. |
| Scheduled dashboard refresh | Each refresh runs the dataset query | Match the refresh interval to how often the data actually changes. |
| Genie queries | Each question runs generated SQL | Fine for a lab; be aware that an open Genie space is an open query surface. |

**Cleanup:** Unshare the dashboard from your partner and pause any scheduled refresh. Keep the dashboard itself—it is referenced in the course wrap-up.

---

## Knowledge Check

1. What is the practical difference for a viewer between shared credentials and individual permissions?
2. Who can a published dashboard be shared with, and why is that reach worth thinking about before you publish?
3. Your filter changes one chart but not the other. What is misconfigured?
4. You renamed `CHTR_TYPE_CD` on the dashboard. Why was preserving that name upstream still the right decision?
5. Genie answered a question confidently and wrongly. Describe how you would establish that, and what you would conclude.
6. Give one question you would let Genie answer unsupervised and one you would always verify. What distinguishes them?
7. Your dashboard is read hundreds of times a day over a large table. What would you change about how the data is published?

Answers — including worked stretch task answers — are in [`answers/Lab_6_Answers.md`](../answers/Lab_6_Answers.md). Attempt the questions before opening it.

---

## Next Steps

This is the final lab. In the wrap-up, map what you built—a validated dataset, a summary table, a published view, an alert, and a dashboard—back onto your team's migration roadmap, and note which questions to take to the engineering team.

Attendees continuing to **Databricks on AWS: Advanced Data Engineering** will rebuild this same pipeline at two further levels of sophistication, starting from the summary table you created in Lab 4.

---

## Resources

<!-- source: facts_extracted.md §16 -->

- Dashboards: https://docs.databricks.com/aws/en/dashboards/
- Version control dashboards with Git: https://docs.databricks.com/aws/en/dashboards/automate/git-support
- Materialized views: https://docs.databricks.com/aws/en/views/materialized
- Unity Catalog privileges: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges

---

*Lab 6 Complete*
