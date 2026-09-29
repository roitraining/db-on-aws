# Databricks notebook source
# MAGIC %md
# MAGIC # DE Module 6 · Demo 1: From Manifest to Deploy
# MAGIC
# MAGIC **Ten minutes. Terminal demo** — this notebook is the presenter script with the exact
# MAGIC commands; run them in a terminal beside the workspace UI.
# MAGIC *"Two days of clicking, now one command against any environment."*
# MAGIC
# MAGIC **Setup:** Databricks CLI **v1.x** installed and authenticated (older 0.2xx builds fail
# MAGIC to download Terraform with an expired-key error — verified 2026-09-29, upgrade first);
# MAGIC the course repo checked out.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — initialise a bundle, open `databricks.yml`
# MAGIC ```bash
# MAGIC databricks bundle init default-python
# MAGIC ```
# MAGIC Walk the four top-level mappings: **`bundle`** (name), **`targets`** (environments),
# MAGIC **`resources`** (what gets deployed), **`variables`** (per-target knobs).
# MAGIC
# MAGIC ### Step 2 — the minimal manifest, then dev/prod targets
# MAGIC Use the real one from the course repo — `bundles/solutions/lab-10-pipeline/databricks.yml`:
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: db_on_aws_lab10_solution
# MAGIC variables:
# MAGIC   catalog:        { default: training_nic }
# MAGIC   target_schema:  { default: lab10_solution }
# MAGIC resources:
# MAGIC   pipelines:
# MAGIC     medallion_pipeline:
# MAGIC       name: "[${bundle.target}] db-on-aws · Lab 10 medallion (solution)"
# MAGIC       catalog: ${var.catalog}
# MAGIC       schema: ${var.target_schema}
# MAGIC       serverless: true
# MAGIC       libraries: [ notebook: { path: ./src/medallion.py } ]
# MAGIC targets:
# MAGIC   dev:  { default: true, mode: development }
# MAGIC   prod: { mode: production, variables: { catalog: training_nic_prod, target_schema: lab10 } }
# MAGIC ```
# MAGIC **`dev` is `default: true` so a bare `deploy` is safe — it never touches prod.** The same
# MAGIC manifest deploys to prod by naming the target; the only difference is a catalog override.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps 3–5 — validate, deploy, show the result
# MAGIC ```bash
# MAGIC cd bundles/solutions/lab-10-pipeline
# MAGIC databricks bundle validate          # manifest checked before anything deploys
# MAGIC databricks bundle deploy -t dev     # -> "Created pipelines.medallion_pipeline"
# MAGIC databricks bundle run medallion_pipeline
# MAGIC ```
# MAGIC Then in the UI: **Jobs & Pipelines → Pipelines** — the pipeline exists, named
# MAGIC `[dev] db-on-aws · Lab 10 medallion (solution)`, and its tables land in
# MAGIC `training_nic.lab10_solution`. Deployed files live under
# MAGIC `/Workspace/Users/<you>/.bundle/db_on_aws_lab10_solution/dev/`.

# COMMAND ----------

# Live proof for the room: the pipeline's output tables, deployed and built entirely from code
display(spark.sql("SHOW TABLES IN training_nic.lab10_solution"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Transition:** *"That deploys resources you declared. What about the job that is already
# MAGIC running from yesterday?"* → Demo 2.
