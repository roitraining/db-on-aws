# Lab 6: Dashboards and Genie

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 40 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

The last step is the one stakeholders actually see. You will build a dashboard on the view you published in Lab 5, make it interactive, share it with a colleague, and then ask Genie two questions about the same data — including one where you check whether its answer can be trusted.

---

## Prerequisites

- [ ] Lab 5 completed — `institution_summary_published` exists and your partner can query it
- [ ] A running serverless SQL warehouse
- [ ] The same partner from Lab 5, to receive the shared dashboard
- [ ] Genie enabled on the workspace

---

## Objectives

- Build an AI/BI Dashboard with two charts over a published view
- Add a filter that cross-filters both charts
- Publish with shared credentials and explain what that means for viewers
- Share the dashboard and verify view-only access from the other side
- Ask Genie two business questions and verify the SQL behind one answer

---

## Part 1: Build the Dashboard

### Task 1: Create and Connect

1. **Create a new dashboard**

    In the left navigation, select **Dashboards**, then create a new dashboard. Name it with your attendee ID.

2. **Add a dataset**

    Point the dashboard's dataset at the view you published in Lab 5.

    ```sql
    SELECT CHTR_TYPE_CD, start_year, institution_count, distinct_cities
    FROM training_nic.analyst_<id>.institution_summary_published;
    ```
    <!-- source: facts_extracted.md §12 -->

3. **Confirm the data loads**

    > **Expected Result:** A preview showing rows from your published view. If it is empty, the view is empty — go back to Lab 5 before continuing.

### Task 2: Build Two Charts

4. **Add a bar chart**

    Create a bar chart with `CHTR_TYPE_CD` on one axis and `institution_count` on the other.

5. **Add a line chart**

    Create a line chart with `start_year` on the horizontal axis and `institution_count` on the vertical.

6. **Give both charts titles a stakeholder would understand**

    Rename them in business language rather than column names. `CHTR_TYPE_CD` means nothing to the person reading your dashboard.

    > **Key Insight:** This is the moment the native NIC column names stop being an academic point. They were correct to preserve through raw and Bronze, but nobody outside this room knows what `CHTR_TYPE_CD` is. Presentation is where you translate.

---

## Part 2: Make It Interactive

### Task 3: Add a Cross-Filter

7. **Add a date-range filter**

    Add a filter widget on `start_year`. Dashboards support global, page-level, and widget-level filters.
    <!-- source: facts_extracted.md §16 -->

8. **Scope the filter to both charts**

    Configure the filter so it applies to both widgets rather than one.
    <!-- source: facts_extracted.md §16 -->

9. **Test the interaction**

    Change the filter range and confirm both charts respond together.

    > **Expected Result:** Both charts update from a single filter change, with no editing and no SQL.

10. **Try cross-filtering from a chart**

    Select a bar in the bar chart and observe the effect on the line chart.
    <!-- source: facts_extracted.md §16 -->

    > **What Just Happened?** A stakeholder can now slice your analysis themselves. That is the difference between a report you re-run on request and one that answers follow-up questions without you.

---

## Part 3: Publish and Share

### Task 4: Publish with Shared Credentials

11. **Publish the dashboard**

    Publish it, choosing the shared-credentials option. Dashboards can be published with shared or individual data permissions.
    <!-- source: facts_extracted.md §16 -->

    > **Key Insight:** With shared credentials, viewers see the data through your access rather than their own, so everyone sees consistent figures. With individual permissions, each viewer sees only what their own grants allow — which can mean two people looking at the same dashboard and seeing different numbers. Choose deliberately.

12. **Share with your partner**

    Share the published dashboard with the partner from Lab 5, granting view access only.
    <!-- source: facts_extracted.md §16 -->

13. **Have your partner open it**

    > **Expected Result:** Your partner can view the dashboard and use the filter, but cannot edit it.

14. **Have your partner confirm they cannot edit**

    Ask them to try. Confirming the limit is as important as confirming the access.

15. **Note the reach of publishing**

    A published dashboard can be shared with anyone registered to your Databricks account, even if they do not have access to the workspace.
    <!-- source: facts_extracted.md §16 -->

    > **Common Pitfall:** That reach is wider than most people expect. Before publishing anything with shared credentials, be sure the data behind it is data you are entitled to show that broadly.

---

## Part 4: Ask Genie

### Task 5: Two Questions and a Verification

16. **Open a Genie space on the same data**

    Open Genie against a space scoped to the published view.

17. **Ask your first business question**

    Ask something a stakeholder would genuinely ask, in plain English — for example, which charter type has grown the most in the last twenty years.
    <!-- source: facts_extracted.md §16 -->

18. **Read the generated SQL, not just the answer**

    Expand the SQL Genie produced. Check that it queries the columns you expect and applies the filter you meant.

19. **Ask a second question that is harder to answer**

    Ask something ambiguous or requiring a judgement — for example, which states are underserved relative to population.

20. **Verify that answer against your own query**

    Write the SQL yourself and compare results.
    <!-- source: facts_extracted.md §16 -->

    > **What Just Happened?** If the two disagree, Genie is not broken and neither are you. It answered the question it understood, which may not be the question you asked. "Underserved" has no definition in the data — Genie had to invent one.

21. **Record when you would and would not trust it**

    Write two sentences: one describing a question you would let Genie answer unsupervised, and one describing a question you would always verify.

    > **Key Insight:** Genie is trustworthy in proportion to how well-defined the question is. A question with an unambiguous answer in the data is safe. A question requiring a definition — "best", "underserved", "at risk" — is where it will confidently supply its own.

---

## Stretch Task

For attendees who finish early.

1. Republish the dashboard using individual rather than shared permissions and have your partner open it again. Do they see the same numbers? Explain why.
2. Add a KPI tile showing total institutions, and make it respond to the same filter as the charts.
3. Ask Genie a question you already know the answer to but phrase it ambiguously. Record what it assumed, and how you would rewrite the underlying dataset description so it assumes better next time.

---

## Checkpoint: Verify Your Progress

- [ ] I created a dashboard connected to my published view
- [ ] I built a bar chart and a line chart
- [ ] I renamed both charts into business language
- [ ] I added a filter on `start_year`
- [ ] The filter applies to both charts, not just one
- [ ] I tested cross-filtering by selecting a value in one chart
- [ ] I published the dashboard with shared credentials
- [ ] I can explain what shared credentials mean for what a viewer sees
- [ ] I shared it with my partner with view-only access
- [ ] My partner opened it and confirmed they cannot edit
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
| Partner cannot open the dashboard | Access denied | Publishing and sharing are separate steps. Confirm you did both. |
| Partner sees different numbers | Figures disagree between viewers | You published with individual rather than shared permissions, so each viewer sees only their own grants. |
| Filter only affects one chart | One chart responds, the other does not | The filter is scoped to a single widget. Change its scope to both. |
| Genie gives a confidently wrong answer | Plausible answer, wrong figures | Working as intended for this lab. Read the generated SQL — it answered a different question from the one you asked. |
| Genie cannot find a column | Question returns nothing useful | The space may not be scoped to your view, or the column names are opaque. Native NIC names are hard for it too. |
| Charts are slow | Long load times | The dashboard runs its dataset query on a warehouse. Confirm one is running and consider a materialized view for a heavily-read dashboard. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Serverless SQL warehouse | Dashboards run their dataset query against a warehouse | Leave auto-stop enabled. |
| Scheduled dashboard refresh | Each refresh runs the dataset query | Match the refresh interval to how often the data actually changes. |
| Genie queries | Each question runs generated SQL | Fine for a lab; be aware that an open Genie space is an open query surface. |

**Cleanup:** Unshare the dashboard from your partner and pause any scheduled refresh. Keep the dashboard itself — it is referenced in the course wrap-up.

---

## Knowledge Check

1. What is the practical difference for a viewer between shared credentials and individual permissions?
2. Who can a published dashboard be shared with, and why is that reach worth thinking about before you publish?
3. Your filter changes one chart but not the other. What is misconfigured?
4. You renamed `CHTR_TYPE_CD` on the dashboard. Why was preserving that name upstream still the right decision?
5. Genie answered a question confidently and wrongly. Describe how you would establish that, and what you would conclude.
6. Give one question you would let Genie answer unsupervised and one you would always verify. What distinguishes them?
7. Your dashboard is read hundreds of times a day over a large table. What would you change about how the data is published?

Answers are held in the Knowledge Check Bank.

---

## Next Steps

This is the final lab. In the wrap-up, map what you built — a validated dataset, a summary table, a published view, an alert, and a dashboard — back onto your team's migration roadmap, and note which questions to take to the engineering team.

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
