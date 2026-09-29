# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Connectivity probe
# MAGIC Which hosts can serverless job compute actually reach?
# MAGIC S3 public buckets are expected to work (AWS-internal path); general internet is expected to fail.

# COMMAND ----------

import urllib.request, socket

TARGETS = {
    "roi S3 bucket (us-east-1)": "https://roi-databricks-demo-data.s3.amazonaws.com/nic_econ/fred/fred_series_meta.csv",
    "NOAA GHCN S3 (us-east-1)": "https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt",
    "generic S3 endpoint": "https://s3.amazonaws.com/",
    "census.gov (non-S3)": "https://www2.census.gov/geo/docs/reference/state.txt",
    "FRED API (non-S3)": "https://api.stlouisfed.org/",
    "GitHub raw (non-S3)": "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv",
}

for label, url in TARGETS.items():
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "probe/1.0", "Range": "bytes=0-99"})
        with urllib.request.urlopen(req, timeout=15) as r:
            print(f"OK    {label}: HTTP {r.status}, {len(r.read())} bytes")
    except Exception as e:
        print(f"FAIL  {label}: {type(e).__name__}: {str(e)[:120]}")
