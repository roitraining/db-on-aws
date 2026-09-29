# Databricks notebook source
# MAGIC %md
# MAGIC # DE Module 6 · Demo 2: Adopt a Running Resource
# MAGIC
# MAGIC **Eight minutes. Terminal demo.** *"No downtime. The job keeps running; now it is also code."*
# MAGIC
# MAGIC **Setup:** the Lab 11 job already deployed and running in the workspace. Syntax below
# MAGIC confirmed on CLI **v1.18** (2026-09-29) — re-confirm after any CLI upgrade.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — `generate`: read the running job, write the YAML for you
# MAGIC Get the job id from the UI (or `databricks jobs list`), then:
# MAGIC ```bash
# MAGIC databricks bundle generate job \
# MAGIC   --existing-job-id <JOB_ID> \
# MAGIC   --key adopted_medallion_job \
# MAGIC   --config-dir resources --source-dir src
# MAGIC ```
# MAGIC It downloads the job's configuration into `resources/adopted_medallion_job.yml` and pulls
# MAGIC the task notebooks into `src/`. *"It reads the running job and writes the YAML for you."*
# MAGIC
# MAGIC ### Step 2 — compare generated vs hand-written
# MAGIC Open the generated YAML next to `bundles/solutions/lab-11-job/databricks.yml`. Same shape —
# MAGIC tasks, If/else condition, branch outcomes — the generator just spells out defaults the
# MAGIC hand-written one left implicit.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — `bind`: adopt in place, no teardown
# MAGIC ```bash
# MAGIC databricks bundle deployment bind adopted_medallion_job <JOB_ID>
# MAGIC databricks bundle deploy -t dev     # next deploy now UPDATES the existing job
# MAGIC ```
# MAGIC The job id does not change; run history is preserved; nothing was deleted or recreated.
# MAGIC (`unbind` reverses it.) *"`bind` is the migration-honest path: adopt in place."*
# MAGIC
# MAGIC ### Step 4 — sketch the GitLab CI flow
# MAGIC The course repo's reference: `bundles/solutions/lab-12-reference/.gitlab-ci.yml`
# MAGIC - **validate** on merge request
# MAGIC - **deploy dev** on merge to main
# MAGIC - **promote prod** on release tag
# MAGIC - authenticating as **the Module 5 service principal** from masked GitLab CI variables
# MAGIC   (`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` — OAuth M2M; the
# MAGIC   CLI reads those names automatically, so there is no `databricks configure` step).
# MAGIC   No personal credentials in CI.
# MAGIC
# MAGIC GitLab remote for the class: `gitlab.com/jesseroi/db-on-aws` (mirror of the GitHub course repo).

# COMMAND ----------

# MAGIC %md
# MAGIC **Transition:** *"That closes the loop — governed storage to deployable code. Let us map it
# MAGIC back to the migration roadmap."*
