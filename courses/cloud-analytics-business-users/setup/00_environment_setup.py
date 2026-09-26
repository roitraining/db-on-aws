# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Setup — Cloud Analytics for Business Users
# MAGIC
# MAGIC This notebook is a thin wrapper: the **single source of truth** for the environment is
# MAGIC `bundles/00-foundation/src/build_environment.py`, which this cell runs in place.
# MAGIC
# MAGIC It builds everything from the **real FFIEC NIC data** pinned in this repository at
# MAGIC `data/nic/`: the raw volume (including `branches/` for the Advanced course's Auto
# MAGIC Loader), full-width bronze tables, `legacy_onprem` (62,080 rows), the staged
# MAGIC `migrated` copy with its five planted defects (61,699 rows), reference tables, the
# MAGIC 2M-row perf tables, and the classic cluster where the workspace allows one.
# MAGIC
# MAGIC Attach serverless compute and **Run All**. A green run ends with every check passing.

# COMMAND ----------

# MAGIC %run ../../../bundles/00-foundation/src/build_environment
