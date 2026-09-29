# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gold: analytics tables
# MAGIC
# MAGIC | Table | Grain | Built from |
# MAGIC |---|---|---|
# MAGIC | `county_metrics` | county × year | institutions, branches, LAUS, ACS, PEP, SOD |
# MAGIC | `state_metrics` | state × year | rolled up from county (sums re-divided, HHI recomputed) |
# MAGIC | `msa_metrics` | MSA (or state non-metro) × year | same |
# MAGIC | `national_macro_monthly` | month | FRED national series |
# MAGIC | `spending_by_weather` | county × month × temp bucket × rain × weekend | OI spending + weather |
# MAGIC | `game_result_spending` | team × game | NFL results + home-county spending |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

years = spark.range(START_YEAR, END_YEAR + 1).select(F.col("id").cast("int").alias("year"))
geo = spark.table(tbl("silver", "dim_geography"))
GEO_COLS = ["county_fips", "county_name", "state_fips", "state_abbr", "state_name",
            "msa_code", "msa_title", "is_metro", "land_sqmi"]

inst = spark.table(tbl("silver", "dim_institution"))
institutions = inst.where("record_type = 'institution'")
if INSTITUTION_ENTITY_TYPES:
    institutions = institutions.where(F.col("entity_type").isin(INSTITUTION_ENTITY_TYPES))
branches = inst.where("record_type = 'branch'")

# COMMAND ----------

# MAGIC %md ## County building blocks

# COMMAND ----------

def active_at_year_end(df, label):
    ye = F.expr("make_date(year, 12, 31)")
    return (df.crossJoin(years)
            .where((F.col("open_date") <= ye) & (F.col("close_date").isNull() | (F.col("close_date") > ye)))
            .groupBy("county_fips", "year").agg(F.countDistinct("rssd_id").alias(label)))


inst_active = active_at_year_end(institutions, "institutions_active")
branch_active = active_at_year_end(branches, "branches_active")

# Closures include charters ended by mergers, not just failures
flows = (institutions.where("open_date is not null")
         .select("county_fips", F.year("open_date").alias("year"), F.lit(1).alias("o"), F.lit(0).alias("c"))
         .unionByName(institutions.where("close_date is not null")
                      .select("county_fips", F.year("close_date").alias("year"), F.lit(0).alias("o"), F.lit(1).alias("c")))
         .groupBy("county_fips", "year")
         .agg(F.sum("o").alias("institutions_opened"), F.sum("c").alias("institutions_closed")))

econ = (spark.table(tbl("silver", "fact_econ")).where("geo_level = 'county'")
        .withColumn("year", F.year("date")).withColumnRenamed("geo_id", "county_fips"))

pop = (econ.where("metric = 'population' and source = 'Census PEP'")
       .groupBy("county_fips", "year").agg(F.first("value").alias("population_pep")))

acs = (econ.where("source = 'Census ACS5'")
       .groupBy("county_fips", "year").pivot("metric", list(ACS_VARS.values())).agg(F.first("value")))

# Annual average of monthly LAUS values
laus = (econ.where("source = 'BLS LAUS' and metric in ('labor_force', 'unemployed')")
        .groupBy("county_fips", "year").pivot("metric", ["labor_force", "unemployed"]).agg(F.avg("value")))

dep = spark.table(tbl("silver", "fact_branch_deposits")).join(geo.select(GEO_COLS), "county_fips")


def deposits_hhi(keys):
    """Deposits and Herfindahl index (0-10,000) by banking organization within each geography."""
    org = dep.groupBy(*keys, "year", "org_id").agg(F.sum("deposits_thousands").alias("org_dep"))
    return (org.withColumn("total", F.sum("org_dep").over(Window.partitionBy(*keys, "year")))
               .withColumn("share", F.expr("try_divide(org_dep, total)"))
               .groupBy(*keys, "year")
               .agg(F.sum("org_dep").alias("deposits_thousands"),
                    F.countDistinct("org_id").alias("banking_orgs"),
                    F.round(F.sum(F.col("share") ** 2) * 10000, 0).alias("deposit_hhi")))


def add_ratios(df):
    return (df
        .withColumn("unemployment_rate", F.expr("round(try_divide(unemployed, labor_force) * 100, 2)"))
        .withColumn("institutions_per_100k", F.expr("round(try_divide(institutions_active, population) * 100000, 2)"))
        .withColumn("branches_per_100k", F.expr("round(try_divide(branches_active, population) * 100000, 2)"))
        .withColumn("deposits_per_capita", F.expr("round(try_divide(deposits_thousands * 1000, population), 0)"))
        .withColumn("poverty_rate", F.expr("round(try_divide(poverty_count, poverty_universe) * 100, 2)")))

# COMMAND ----------

# MAGIC %md ## county_metrics

# COMMAND ----------

keys = ["county_fips", "year"]
county = (geo.select(GEO_COLS).crossJoin(years)
          .join(pop, keys, "left").join(acs, keys, "left").join(laus, keys, "left")
          .join(inst_active, keys, "left").join(branch_active, keys, "left").join(flows, keys, "left")
          .join(deposits_hhi(["county_fips"]), keys, "left")
          .withColumn("population", F.coalesce("population_pep", "population_acs"))
          .fillna(0, subset=["institutions_active", "branches_active", "institutions_opened", "institutions_closed"]))
write_table(add_ratios(county), tbl("gold", "county_metrics"))

# COMMAND ----------

# MAGIC %md ## state_metrics + msa_metrics
# MAGIC Rates are recomputed from summed numerators/denominators (never averaged across counties).
# MAGIC Median income is a population-weighted average of county medians (an approximation).

# COMMAND ----------

SUM_COLS = ["population", "institutions_active", "branches_active", "institutions_opened", "institutions_closed",
            "labor_force", "unemployed", "poverty_count", "poverty_universe", "land_sqmi"]


def rollup(keys):
    c = spark.table(tbl("gold", "county_metrics"))
    agg = (c.groupBy(*keys, "year")
           .agg(*[F.sum(x).alias(x) for x in SUM_COLS],
                F.expr("round(try_divide(sum(median_hh_income * population), "
                       "sum(case when median_hh_income is not null then population end)), 0)").alias("median_hh_income_popwtd"),
                F.count("*").alias("county_count")))
    return add_ratios(agg.join(deposits_hhi(keys), [*keys, "year"], "left"))


write_table(rollup(["state_fips", "state_abbr", "state_name"]), tbl("gold", "state_metrics"))
write_table(rollup(["msa_code", "msa_title", "is_metro"]), tbl("gold", "msa_metrics"))

# COMMAND ----------

# MAGIC %md ## national_macro_monthly

# COMMAND ----------

macro = (spark.table(tbl("silver", "fact_econ")).where("geo_level = 'national'")
         .groupBy(F.trunc("date", "month").alias("month"))
         .pivot("metric", list(FRED_NATIONAL.values())).agg(F.round(F.avg("value"), 3)))
write_table(macro, tbl("gold", "national_macro_monthly"))

# COMMAND ----------

# MAGIC %md ## spending_by_weather
# MAGIC Stores sums + day counts so the dashboard can re-aggregate correctly at any level.
# MAGIC Keep `month` in your comparisons: cold days are also December days.

# COMMAND ----------

dd = spark.table(tbl("silver", "dim_date")).select("date", "month", "is_weekend")
sw = (spark.table(tbl("silver", "fact_spending_daily"))
      .join(spark.table(tbl("silver", "fact_weather_daily")), ["county_fips", "date"])
      .join(dd, "date")
      .join(geo.select("county_fips", "county_name", "state_abbr"), "county_fips")
      .where("temp_bucket is not null")
      .withColumn("rain_day", F.coalesce(F.col("prcp_mm") >= 1, F.lit(False)))
      .groupBy("state_abbr", "county_fips", "county_name", "month", "temp_bucket", "rain_day", "is_weekend")
      .agg(F.count("*").alias("days"),
           F.sum("spend_all_vs_jan2020").alias("spend_sum"),
           F.round(F.avg("tavg_c"), 1).alias("avg_tavg_c")))
write_table(sw, tbl("gold", "spending_by_weather"))

# COMMAND ----------

# MAGIC %md ## game_result_spending
# MAGIC Spending is a 7-day trailing average, so:
# MAGIC - `next_day_change` = (day after) − (day before): a small, diluted signal
# MAGIC - `week_change` = (6 days after) − (day before): compares the full week after the game to the week before
# MAGIC
# MAGIC Compare **wins vs losses**, not before vs after, so weekday effects cancel out (games are mostly Sundays).

# COMMAND ----------

team_map = spark.createDataFrame(list(TEAM_HOME_COUNTY.items()), "team string, county_fips string")
games = spark.table(tbl("silver", "fact_games")).join(team_map, "team")
s = spark.table(tbl("silver", "fact_spending_daily"))


def spend_shifted(days_after_game, label):
    # spend on (game date + n)  ->  keyed by game date
    return s.select("county_fips", F.date_sub("date", days_after_game).alias("date"),
                    F.col("spend_all_vs_jan2020").alias(label))


gs = (games
      .join(spend_shifted(-1, "spend_day_before"), ["county_fips", "date"], "left")
      .join(spend_shifted(0, "spend_game_day"), ["county_fips", "date"], "left")
      .join(spend_shifted(1, "spend_day_after"), ["county_fips", "date"], "left")
      .join(spend_shifted(6, "spend_week_after"), ["county_fips", "date"], "left")
      .where("spend_day_before is not null")
      .withColumn("next_day_change", F.col("spend_day_after") - F.col("spend_day_before"))
      .withColumn("week_change", F.col("spend_week_after") - F.col("spend_day_before")))
write_table(gs, tbl("gold", "game_result_spending"))

# COMMAND ----------

for name in ["county_metrics", "state_metrics", "msa_metrics", "national_macro_monthly",
             "spending_by_weather", "game_result_spending"]:
    print(f"{name}: {spark.table(tbl('gold', name)).count():,} rows")
