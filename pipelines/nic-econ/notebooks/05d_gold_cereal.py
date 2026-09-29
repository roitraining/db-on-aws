# Databricks notebook source
# MAGIC %md
# MAGIC # 05d · Gold: cereal vs macro (the Rice Krispies memorial table)
# MAGIC General Mills monthly close + FRED unemployment + fed funds, staged as a CSV in the landing
# MAGIC volume (Ferrero is private; WK Kellogg and Kellanova are both delisted — GIS is the last
# MAGIC public Big Cereal stock). Output: `gold.cereal_stock_macro` + printed correlations.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

path = f"{LANDING}/fun/cereal_stock_macro.csv"
if not os.path.exists(path):
    print("WARNING: cereal_stock_macro.csv not staged — skipping")
    dbutils.notebook.exit("skipped")

df = (spark.read.option("header", True).csv(path)
      .select(F.col("month").cast("date").alias("month"),
              num("gis_close").alias("gis_close"),
              num("unemployment_rate").alias("unemployment_rate"),
              num("fed_funds_rate").alias("fed_funds_rate")))
write_table(df, tbl("gold", "cereal_stock_macro"))

display(spark.sql(f"""
    SELECT COUNT(*) AS months,
           ROUND(CORR(gis_close, fed_funds_rate), 3) AS corr_gis_fedfunds,
           ROUND(CORR(gis_close, unemployment_rate), 3) AS corr_gis_unemployment
    FROM {tbl('gold', 'cereal_stock_macro')}
"""))
