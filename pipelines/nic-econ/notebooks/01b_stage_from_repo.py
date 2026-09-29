# Databricks notebook source
# MAGIC %md
# MAGIC # 01b · Stage landing files from this Git folder
# MAGIC Run **after `01_setup`** and **before `03c_bronze_staged`**.
# MAGIC
# MAGIC Copies the pinned data files that ship with this repo (`../data/`) into the landing volume.
# MAGIC This is the same pattern Lab 0 uses for the NIC zips: the repo is the data distribution channel,
# MAGIC because a cloned Git folder works on serverless with no credentials and no open internet.
# MAGIC
# MAGIC Files already present in the volume are never overwritten.
# MAGIC
# MAGIC Not shipped in the repo (too large): NOAA weather (`noaa/*.csv.gz`, ~130 MB) — upload those with
# MAGIC `databricks fs cp` if you want the weather tables; otherwise they come up empty and everything
# MAGIC else still works.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import os, shutil

REPO_DATA = os.path.abspath(os.path.join(os.getcwd(), "..", "data"))
print("repo data dir:", REPO_DATA)
assert os.path.isdir(REPO_DATA), "no ../data directory — is this notebook running inside the cloned Git folder?"

copied, skipped = [], []
for sub in sorted(os.listdir(REPO_DATA)):
    src_dir = os.path.join(REPO_DATA, sub)
    if not os.path.isdir(src_dir):
        continue
    dst_dir = f"{LANDING}/{sub}"
    os.makedirs(dst_dir, exist_ok=True)
    for fname in sorted(os.listdir(src_dir)):
        dst = f"{dst_dir}/{fname}"
        if os.path.exists(dst):
            skipped.append(f"{sub}/{fname}")
            continue
        shutil.copyfile(os.path.join(src_dir, fname), dst)
        copied.append(f"{sub}/{fname}")

print(f"copied {len(copied)}:", copied)
print(f"already present (untouched) {len(skipped)}:", skipped)
