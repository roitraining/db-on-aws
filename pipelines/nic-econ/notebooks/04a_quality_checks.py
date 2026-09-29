# Databricks notebook source
# MAGIC %md
# MAGIC # 04a · Bronze quality gate
# MAGIC Runs after all bronze sources land, before silver. For every bronze table:
# MAGIC row count, duplicate count on the table's natural key, and null rate on key columns.
# MAGIC Results land in `bronze._audit_quality` (append, stamped per run) so quality drift is queryable.
# MAGIC The gate **fails the job** only on hard violations (empty required table, >5% key nulls,
# MAGIC or duplicates on a strict key); everything else is a logged warning.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# table -> (natural key cols, strict_dedupe, required)
CHECKS = {
    "ref_states":            (["state"], True, True),
    "ref_county_gazetteer":  (["geoid"], True, True),
    "ref_cbsa_delineation":  (["fips_state_code", "fips_county_code"], False, True),
    "fred_series_meta":      (["series_id"], True, True),
    "fred_observations":     (["series_id", "date", "realtime_start"], False, True),
    "census_pep":            (["state", "county", "vintage"], False, True),
    "census_acs5":           (["state", "county", "acs_year"], False, False),
    "bls_laus_county":       (["series_id", "year", "period"], True, True),
    "fdic_sod":              (["uninumbr", "year"], False, True),
    "fdic_financials":       (["cert", "repdte"], True, False),
    "noaa_ghcn_stations":    (["station_id"], True, False),
    "noaa_ghcn_daily":       (["id", "date", "element"], False, False),
    "nflverse_games":        (["game_id"], True, True),
    "oi_spending_county_daily": (["countyfips", "year", "month", "day"], True, True),
    "nic_attributes_active": (["id_rssd", "_snapshot_date"], True, True),
    "nic_attributes_closed": (["id_rssd", "_snapshot_date"], True, True),
    "nic_attributes_branches": (["id_rssd", "_snapshot_date"], False, True),
    "nic_transformations":   (["id_rssd_predecessor", "id_rssd_successor", "dt_trans"], False, False),
}

run_ts = datetime.datetime.now().isoformat()
results, hard_failures = [], []

for table, (keys, strict, required) in CHECKS.items():
    name = tbl("bronze", table)
    if not spark.catalog.tableExists(name):
        status = "FAIL_MISSING" if required else "SKIP_OPTIONAL"
        results.append((run_ts, table, status, 0, 0, 0.0, "table does not exist"))
        if required:
            hard_failures.append(f"{table}: missing")
        continue
    df = spark.table(name)
    have = [k for k in keys if k in df.columns]
    n = df.count()
    if n == 0:
        empty_ok = not required or df.limit(1).count() == 0 and not required
        status = "FAIL_EMPTY" if required else "WARN_EMPTY"
        results.append((run_ts, table, status, 0, 0, 0.0, "0 rows"))
        if required:
            hard_failures.append(f"{table}: empty")
        continue
    dupes = n - df.select(*have).distinct().count() if have else 0
    null_keys = df.where(" OR ".join(f"`{k}` IS NULL" for k in have)).count() if have else 0
    null_pct = round(null_keys / n * 100, 2)
    notes = []
    status = "OK"
    if dupes > 0:
        notes.append(f"{dupes} duplicate keys ({'+'.join(have)})")
        if strict:
            status = "FAIL_DUPES"
            hard_failures.append(f"{table}: {dupes} dupes on strict key")
        else:
            status = "WARN_DUPES"
    if null_pct > 5.0:
        notes.append(f"{null_pct}% null keys")
        status = "FAIL_NULL_KEYS"
        hard_failures.append(f"{table}: {null_pct}% null keys")
    elif null_keys:
        notes.append(f"{null_keys} rows with null key cols")
    results.append((run_ts, table, status, n, dupes, null_pct, "; ".join(notes)))

audit = spark.createDataFrame(results,
    "run_ts string, table_name string, status string, row_count long, duplicate_keys long, null_key_pct double, notes string")
audit.write.mode("append").saveAsTable(tbl("bronze", "_audit_quality"))
display(audit.orderBy("table_name"))

if hard_failures:
    raise RuntimeError(f"Quality gate failed: {hard_failures}")
print(f"Quality gate passed: {len(results)} tables checked, "
      f"{sum(1 for r in results if r[2].startswith('WARN'))} warnings")
