# Databricks notebook source
# MAGIC %md
# MAGIC # DE Module 5 · Demo 2: The Event Log Meets Run As
# MAGIC
# MAGIC **Nine minutes. The failure is the lesson** — it is the implicit-permission class that
# MAGIC Run As introduces, sequenced so attendees meet it here and not in production.
# MAGIC
# MAGIC **Setup:** the Lab 11 job from Demo 1; a **service principal** created in workspace admin
# MAGIC settings, with grants on the target schemas (but NOT ownership of the streaming table).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — as yourself, the owner, the event-log query works

# COMMAND ----------

display(spark.sql("""
    SELECT event_type,
           details:flow_progress.data_quality.dropped_records AS dropped
    FROM event_log(TABLE(training_nic.lab10_solution.branches_silver))
    WHERE event_type = 'flow_progress'
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — set the job's Run As to the service principal
# MAGIC Job page → **⋯ → Edit permissions / Run as** → pick the service principal. (Or in the
# MAGIC bundle: `run_as: {service_principal_name: <application-id>}` and re-deploy.)
# MAGIC
# MAGIC ### Step 3 — re-run. The `read_event_log` task **fails**; every other task keeps succeeding.
# MAGIC
# MAGIC ### Step 4 — sit in it
# MAGIC *"The query is correct. The identity is not the owner. `event_log()` can only be called by
# MAGIC the owner of the streaming table or materialized view."* The Lab 11 notebook even prints
# MAGIC this diagnosis when it catches the error — show its output.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 — resolve, then repair (not restart)
# MAGIC 1. Make the principal the owner of the streaming table:
# MAGIC    ```sql
# MAGIC    ALTER TABLE training_nic.lab10_solution.branches_silver
# MAGIC      SET OWNER TO `<service-principal-application-id>`;
# MAGIC    ```
# MAGIC 2. On the failed run: **Repair run** — it reruns only the failed task and what depends on
# MAGIC    it, not the whole job. *"Repair-and-rerun, not restart."* (Confirm the button label on
# MAGIC    the training workspace beforehand — UI names drift.)
# MAGIC
# MAGIC **What to highlight:** Run As means the job's identity is not a person — no credential dies
# MAGIC when someone leaves. The cost is exactly this class of implicit permission, and the event
# MAGIC log's owner-only rule is the canonical example.
# MAGIC
# MAGIC **Transition:** *"The pipeline runs, gates, and runs as a principal. The last step is putting
# MAGIC all of it under version control so any environment can deploy it."*
