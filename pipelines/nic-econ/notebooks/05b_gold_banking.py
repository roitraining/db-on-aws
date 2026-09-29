# Databricks notebook source
# MAGIC %md
# MAGIC # 05b · Gold: banking profitability
# MAGIC FDIC Call Report profitability joined to each bank's HQ county (via Summary of Deposits
# MAGIC main-office row) and rolled up the aggregation ladder: **bank → county → metro → state**.
# MAGIC Tables: `gold.institution_profitability` (leaf), `gold.metro_banking_summary`,
# MAGIC `gold.state_banking_summary`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

if not spark.catalog.tableExists(tbl("bronze", "fdic_financials")):
    print("WARNING: bronze.fdic_financials not present — skipping banking gold tables")
    dbutils.notebook.exit("skipped")

# COMMAND ----------

hq = (spark.table(tbl("bronze", "fdic_sod"))
      .where("try_cast(year as int) = 2024")
      .groupBy("cert")
      .agg(F.max("namefull").alias("bank_name"),
           F.max(F.when(F.expr("try_cast(brnum as int) = 0"),
                        F.lpad(F.regexp_replace("stcntybr", r"\.0$", ""), 5, "0"))).alias("hq_county_fips")))

geo = spark.table(tbl("silver", "dim_geography")).select(
    "county_fips", "county_name", "state_abbr", "state_name", "msa_code", "msa_title", "is_metro")

prof = (spark.table(tbl("bronze", "fdic_financials"))
        .join(hq, "cert")
        .join(geo, hq.hq_county_fips == geo.county_fips, "left")
        .select("cert", "bank_name", "hq_county_fips", "county_name", "state_abbr", "state_name",
                "msa_code", "msa_title", "is_metro",
                "netinc_thousands", "roa", "roe", "assets_thousands", "deposits_thousands"))
write_table(prof, tbl("gold", "institution_profitability"))

# COMMAND ----------

# Rollups: re-divide sums, never average averages. ROA re-derived as sum(netinc)/sum(assets).
def banking_rollup(keys, out):
    df = (spark.table(tbl("gold", "institution_profitability"))
          .where(" AND ".join(f"{k} IS NOT NULL" for k in keys))
          .groupBy(*keys)
          .agg(F.count("*").alias("banks_hq"),
               F.round(F.sum("netinc_thousands") / 1e6, 2).alias("net_income_busd"),
               F.round(F.sum("assets_thousands") / 1e6, 1).alias("assets_busd"),
               F.round(F.sum("deposits_thousands") / 1e6, 1).alias("deposits_busd"),
               F.round(F.expr("try_divide(sum(netinc_thousands), sum(assets_thousands)) * 100"), 3)
                .alias("aggregate_roa_pct")))
    write_table(df, tbl("gold", out))


banking_rollup(["msa_code", "msa_title", "is_metro"], "metro_banking_summary")
banking_rollup(["state_abbr", "state_name"], "state_banking_summary")

for t in ["institution_profitability", "metro_banking_summary", "state_banking_summary"]:
    print(t, spark.table(tbl("gold", t)).count())
