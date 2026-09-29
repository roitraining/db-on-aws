# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup
# MAGIC Creates the catalog, bronze/silver/gold schemas, and the landing volume.
# MAGIC
# MAGIC **One-time secrets** (Databricks CLI):
# MAGIC ```
# MAGIC databricks secrets create-scope nic_econ
# MAGIC databricks secrets put-secret nic_econ fred_api_key --string-value <FRED key>
# MAGIC databricks secrets put-secret nic_econ census_api_key --string-value <Census key>   # optional
# MAGIC ```
# MAGIC Free keys: FRED (fred.stlouisfed.org/docs/api/api_key.html), Census (api.census.gov/data/key_signup.html).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")

for folder in ["nic", "bls", "noaa", "oi"]:
    os.makedirs(f"{LANDING}/{folder}", exist_ok=True)

print(f"Ready. Upload your unzipped NIC CSVs to {LANDING}/nic/")
print("Secrets found:", {k: bool(secret(k)) for k in ["fred_api_key", "census_api_key"]})
