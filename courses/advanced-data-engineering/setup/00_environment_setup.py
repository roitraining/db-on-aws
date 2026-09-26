# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Setup — Advanced Data Engineering
# MAGIC
# MAGIC **Superseded as a separate build.** The foundation notebook now builds everything the
# MAGIC advanced labs need — including the 2M-row skewed `perf` tables for Lab 8 and the real
# MAGIC NIC `branches/` landing files for Lab 10 — so this wrapper simply runs it. Re-running
# MAGIC is safe and idempotent.
# MAGIC
# MAGIC Attach serverless compute and **Run All**.

# COMMAND ----------

# MAGIC %run ../../../bundles/00-foundation/src/build_environment
