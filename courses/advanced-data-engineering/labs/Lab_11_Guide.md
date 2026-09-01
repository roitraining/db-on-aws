# Lab 11: Orchestration with a Quality Gate

**Course:** Databricks on AWS: Advanced Data Engineering
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

The pipeline works when you run it. This lab makes it work when you are not there. You will wrap it in a Job, read the pipeline's own event log to count quality violations, gate promotion to Gold on that count, alert on failure, and move execution off your personal identity onto a service principal.

---

## Prerequisites

- [ ] Lab 10 completed — the Medallion pipeline exists and runs
- [ ] A service principal with grants on `eng_<id>` and the training catalog
- [ ] An email destination configured for notifications
- [ ] Permission to create Lakeflow Jobs

---

## Objectives

- Build a multi-task Job DAG from an existing pipeline
- Query the pipeline event log as a Delta table to source a quality metric
- Pass a value between tasks using Task Values
- Gate Gold promotion with a Condition Task
- Configure a failure alert
- Set Run As to a service principal and explain why that matters
- Recover a failed run with repair-and-rerun

---

## Part 1: Wrap the Pipeline

### Task 1: Build the Job DAG

1. **Create a new Lakeflow Job**

    Name it for your engineer ID. This is the container the pipeline will run inside.
    <!-- source: facts_extracted.md §7 -->

2. **Add the pipeline as the first task**

    Add a task of type pipeline, pointing at the Lab 10 pipeline. This runs Bronze and Silver.
    <!-- source: facts_extracted.md §7 -->

3. **Add a second task that reads the event log**

    This task must be a **Python notebook** task, not a SQL task — see step 4 for why.

    The event log is read through the `event_log()` table-valued function, which takes either a table name or a `pipeline_id`. The pipeline records its own quality outcomes; orchestration reads them rather than recomputing from the data.

    ```python
    dropped = spark.sql("""
        SELECT SUM(CAST(details:flow_progress.data_quality.dropped_records AS BIGINT)) AS dropped
        FROM event_log(TABLE(eng_<id>.work.branches_silver))
        WHERE event_type = 'flow_progress'
    """).collect()[0]["dropped"] or 0
    print(f"dropped records: {dropped}")
    ```
    <!-- source: facts_extracted.md §7 -->

    > **Note:** `details` is a JSON column and the `flow_progress` event type carries expectation metrics. The colon syntax navigates the JSON directly.

    > **Troubleshooting:** `event_log()` can only be called by the **owner** of the streaming table or materialized view you pass it. If this task fails with a permission error, check ownership before checking your SQL — and remember this when you change Run As in Task 5.
    <!-- source: facts_extracted.md §7 -->

4. **Emit the count as a Task Value**

    ```python
    dbutils.jobs.taskValues.set(key = "dropped_records", value = int(dropped))
    ```
    <!-- source: facts_extracted.md §7 -->

    > **Common Pitfall:** Task values **can only be set from a Python notebook**. They can be *referenced* from any task type that supports parameters, but a SQL task cannot emit one. That is why step 3 is a notebook rather than a SQL query. The JSON representation of a value cannot exceed 48 KiB.
    <!-- source: facts_extracted.md §7 -->

5. **Set the task dependency**

    The event-log task must depend on the pipeline task, or it will read a stale log.

    > **Key Insight:** A Job is a DAG, not a script. Dependencies are declared, not implied by ordering on screen. A task with no declared dependency can start immediately, which is exactly the bug that produces "it worked yesterday."

---

## Part 2: The Quality Gate

### Task 2: The If/else Task

6. **Add an If/else task**

    Add a task of type **If/else condition**. It takes a boolean operator and a pair of operands, and either operand may reference job state, parameters, or a task value.

    Available operators: `EQUAL_TO`, `NOT_EQUAL`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`.
    <!-- source: facts_extracted.md §7 -->

    > **Note:** The approved course outline calls this a *Condition Task*. The current product name is the **If/else task**. You will meet both terms.

7. **Reference the upstream Task Value as the left operand**

    ```
    {{tasks.read_event_log.values.dropped_records}}
    ```
    <!-- source: facts_extracted.md §7 -->

    Set the operator to `LESS_THAN` and the right operand to your chosen threshold.

    > **Common Pitfall:** Operands accept numeric, string and boolean values only, and anything non-numeric is serialized to a string and compared **as a string**. The comparison operators do compare numerically — `"12.0" >= "12"` evaluates true — but only when both sides are genuinely numeric. A value emitted as a string is why a gate silently always takes one branch.
    <!-- source: facts_extracted.md §7 -->

8. **Add the Gold task on the true branch**

    A downstream task's **Depends on** field defaults to `<task-name> (true)`. Attach the Gold materialized view refresh there so it runs only when the gate passes. To attach a task to the failing outcome instead, select `<task-name> (false)`.
    <!-- source: facts_extracted.md §7 -->

9. **Decide what happens on the false branch**

    Write down whether a failed gate should fail the run loudly or exit quietly, and why.

    > **Key Insight:** The gate is the difference between a pipeline that stops and a pipeline that publishes bad data on schedule. Silver already dropped the invalid rows — the gate exists because *how many* were dropped is itself a signal. One bad row is noise; ten thousand is an upstream change nobody told you about.

10. **Run the Job and confirm the gate evaluated**

    > **Expected Result:** The pipeline task succeeds, the event-log task emits a count, and the Condition Task routes to Gold or halts based on your threshold.

---

## Part 3: Operate It

### Task 3: Failure Alerting

11. **Configure a failure notification**

    Add an email destination on job failure.
    <!-- source: facts_extracted.md §7 -->

12. **Force a failure and confirm the alert**

    Temporarily point a task at a non-existent table, run, and confirm the notification arrives.

13. **Restore the task**

    Put the correct reference back.

### Task 4: Repair and Rerun

14. **Trigger a mid-pipeline failure**

    Cause the Gold task to fail while leaving upstream tasks succeeding.

15. **Use repair-and-rerun rather than re-running the Job**

    Repair re-executes only the failed tasks.
    <!-- source: facts_extracted.md §7 -->

16. **Compare what re-ran against what did not**

    > **Key Insight:** A full re-run reprocesses Bronze and Silver you already paid for and already validated. Repair restarts from the failure. On a nightly job with a two-hour ingest, that difference is the difference between recovering before the business day and not.

### Task 5: Run As a Service Principal

17. **Change Run As to the service principal**

    Set the job's Run As identity to the service principal supplied by your instructor.
    <!-- source: facts_extracted.md §7 -->

18. **Run the Job and observe what happens**

    It will very likely fail, and probably at the event-log task rather than anywhere obvious. That is the point of the exercise.

    > **Key Insight:** `event_log()` can only be called by the **owner** of the streaming table you pass it. You own it; the service principal does not. Every other task may succeed while this one fails, which is exactly how implicit permissions announce themselves — not as a permissions error at the start, but as one failure deep in a DAG that worked yesterday.
    <!-- source: facts_extracted.md §7 -->

19. **Grant the service principal what it needs**

    ```sql
    GRANT USE CATALOG ON CATALOG eng_<id> TO `<service-principal>`;
    GRANT USE SCHEMA  ON SCHEMA  eng_<id>.work TO `<service-principal>`;
    GRANT SELECT, MODIFY ON SCHEMA eng_<id>.work TO `<service-principal>`;
    ```
    <!-- source: facts_extracted.md §1 -->

20. **Re-run and confirm success**

    > **What Just Happened?** The job ran under your identity for two days and worked. The moment it ran under an identity that is not you, every permission you had implicitly became a permission that had to be granted explicitly. That is the point of the exercise.

21. **State why this matters for a regulated migration**

    Write two sentences.

    > **Key Insight:** Separating developer identity from execution identity means a job does not stop working when you leave, does not inherit permissions you happen to hold, and produces an audit trail that names a service rather than a person. In a regulated environment the third reason is usually the one that matters.

---

## Stretch Task

1. Add a For Each task that runs the Silver step once per state, parameterised from a list. What did that change about the DAG, and what would it cost at fifty states?
2. Configure a file-arrival trigger so the Job runs when new files land rather than on a schedule. Which upstream assumption does that remove?
3. Query `system.query.history` to attribute this Job's compute cost. Who would you send that figure to, and what decision would it inform?

---

## Checkpoint: Verify Your Progress

- [ ] I created a Job containing the Lab 10 pipeline as a task
- [ ] I added a task that queries the pipeline event log
- [ ] I declared the dependency between them explicitly
- [ ] I emitted a violation count as a Task Value
- [ ] I added a Condition Task evaluating that value
- [ ] The Gold task runs only on the passing branch
- [ ] I decided and recorded what the failing branch should do
- [ ] I ran the Job and saw the gate evaluate
- [ ] I configured a failure email notification
- [ ] I forced a failure and received the alert
- [ ] I used repair-and-rerun and confirmed only failed tasks re-ran
- [ ] I set Run As to a service principal
- [ ] I granted the service principal the privileges it needed
- [ ] The Job succeeded under the service principal
- [ ] I wrote two sentences on why execution identity matters

---

## Troubleshooting Reference

> **Key Insight:** Nearly every Run As failure is a missing grant, and it will not resemble a permissions error at first glance — it usually surfaces as a table-not-found. The service principal cannot see what you can see.
<!-- source: facts_extracted.md §1 -->

| Issue | Symptom | Solution |
|---|---|---|
| Task reads a stale event log | Violation count is from a previous run | The dependency was not declared. Tasks without dependencies start immediately. |
| Task Value not found | Condition cannot resolve the reference | Check the task name in `{{tasks.<name>.values.<key>}}` matches exactly. |
| Cannot set a task value | No value emitted | Task values can only be **set from a Python notebook**. A SQL task cannot emit one. |
| Task value rejected | Error on `set` | The JSON representation exceeds 48 KiB. Emit a scalar, not a result set. |
| Condition always takes one branch | Gate never flips | The operand is a string rather than a number. Cast to `int` before emitting. |
| `event_log()` permission error | Event-log task fails, others succeed | It can only be called by the **owner** of the streaming table or materialized view. Common the moment Run As changes. |
| Job fails under Run As | Table or schema not found | The service principal lacks `USE CATALOG`, `USE SCHEMA`, or `SELECT` — or does not own the streaming table the event log is read from. |
| Repair re-runs everything | Whole job re-executes | You triggered a new run rather than repairing the failed one. |
| No failure notification | Job failed silently | The notification is configured on the wrong event, or the destination is unset. |
| Gold refreshes despite violations | Gate did not hold | The Gold task has a dependency on the pipeline task rather than the Condition Task. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Job cluster | Started per run, terminated after | Preferred over an all-purpose cluster for scheduled work. |
| Full re-runs | Reprocesses work already done | Use repair-and-rerun on failure. |
| Schedule frequency | Every trigger costs compute | Match to how often data actually arrives; a file-arrival trigger avoids empty runs. |

**Cleanup:** Keep the Job — Lab 12 declares it as a bundle resource. Pause its schedule so it does not run after class.

---

## Knowledge Check

1. Why is a Job a DAG rather than a sequence, and what bug does confusing the two produce?
2. Where does the violation count come from, and why not recompute it from the data?
3. What are Task Values for, and what are they not suitable for?
4. Silver already drops invalid rows. Why gate on how many were dropped?
5. What does repair-and-rerun re-execute, and when is the saving largest?
6. Your Job worked for two days and failed the moment you set Run As. Why, and what does that reveal?
7. Give the strongest argument for a service principal in a regulated migration.

Answers are held in the Knowledge Check Bank.

---

## Next Steps

Lab 12 is the last step: the pipeline and Job you built by hand become code in `databricks.yml`, deployable to dev or prod from the GitLab repository you linked in Lab 7.

---

## Resources

- Spark Declarative Pipelines: https://docs.databricks.com/aws/en/ldp/
- Expectations: https://docs.databricks.com/aws/en/ldp/expectations
- Unity Catalog privileges: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
- Databricks on AWS documentation: https://docs.databricks.com/aws/en/

---

*Lab 11 Complete*
