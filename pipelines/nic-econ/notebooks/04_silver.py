# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver: conformed tables
# MAGIC Typed, cleaned, and keyed to shared dimensions (`county_fips`, `date`).
# MAGIC
# MAGIC | Table | Grain |
# MAGIC |---|---|
# MAGIC | `dim_geography` | county (with state + MSA/non-metro rollup keys) |
# MAGIC | `dim_date` | day |
# MAGIC | `dim_institution` | NIC entity (institutions + branches, latest snapshot) |
# MAGIC | `fact_institution_events` | NIC transformation (merger, charter change, ...) |
# MAGIC | `fact_econ` | geo × date × metric (FRED, LAUS, ACS, PEP), long format |
# MAGIC | `map_station_county`, `fact_weather_daily` | county × day |
# MAGIC | `fact_games` | team × game (two rows per game) |
# MAGIC | `fact_spending_daily` | county × day |
# MAGIC | `fact_branch_deposits` | branch × year |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md ## dim_geography

# COMMAND ----------

states = (spark.table(tbl("bronze", "ref_states"))
          .select(F.lpad("state", 2, "0").alias("state_fips"), F.col("stusab").alias("state_abbr"), "state_name"))

gaz = (spark.table(tbl("bronze", "ref_county_gazetteer"))
       .select(F.lpad("geoid", 5, "0").alias("county_fips"),
               F.col("name").alias("county_name"),
               num("aland_sqmi").alias("land_sqmi"),
               num("intptlat").alias("lat"),
               num("intptlong").alias("lon")))

cbsa = (spark.table(tbl("bronze", "ref_cbsa_delineation"))
        .where("fips_county_code is not null")
        .select(F.concat(F.lpad("fips_state_code", 2, "0"), F.lpad("fips_county_code", 3, "0")).alias("county_fips"),
                "cbsa_code", "cbsa_title",
                F.col("metropolitan_micropolitan_statistical_area").alias("cbsa_type"))
        .dropDuplicates(["county_fips"]))

geo = (gaz.withColumn("state_fips", F.substring("county_fips", 1, 2))
       .join(states, "state_fips")
       .join(cbsa, "county_fips", "left")
       .withColumn("is_metro", F.coalesce(F.col("cbsa_type") == "Metropolitan Statistical Area", F.lit(False)))
       .withColumn("msa_code", F.when(F.col("is_metro"), F.col("cbsa_code"))
                                .otherwise(F.concat(F.lit("NM"), F.col("state_fips"))))
       .withColumn("msa_title", F.when(F.col("is_metro"), F.col("cbsa_title"))
                                 .otherwise(F.concat(F.col("state_name"), F.lit(" (non-metro)")))))

write_table(geo, tbl("silver", "dim_geography"))

# COMMAND ----------

# MAGIC %md ## dim_date

# COMMAND ----------

dim_date = (spark.sql(f"SELECT explode(sequence(DATE'{START_YEAR}-01-01', DATE'{END_YEAR}-12-31', INTERVAL 1 DAY)) AS date")
            .select("date",
                    F.year("date").alias("year"),
                    F.quarter("date").alias("quarter"),
                    F.month("date").alias("month"),
                    F.date_format("date", "EEEE").alias("weekday"),
                    F.dayofweek("date").isin(1, 7).alias("is_weekend")))
write_table(dim_date, tbl("silver", "dim_date"))

# COMMAND ----------

# MAGIC %md ## dim_institution + fact_institution_events
# MAGIC NIC attribute files are a current snapshot, so this uses the latest one. Open/close dates give
# MAGIC point-in-time activity; location history builds up as you load more snapshots in bronze.

# COMMAND ----------

def latest_snapshot(table):
    df = spark.table(tbl("bronze", table))
    snap = df.agg(F.max("_snapshot_date")).first()[0]
    return df.where(F.col("_snapshot_date") == F.lit(snap))


def nic_date(col_name):
    d = parse_date(col_name, NIC_DATE_FORMAT)
    return F.when(d >= F.lit("9999-01-01").cast("date"), F.lit(None).cast("date")).otherwise(d)


def nic_frame(table, record_type):
    c = NIC_COLS
    return latest_snapshot(table).select(
        F.col(c["rssd_id"]).alias("rssd_id"),
        F.col(c["name"]).alias("name"),
        F.col(c["entity_type"]).alias("entity_type"),
        F.col(c["city"]).alias("city"),
        F.concat(F.lpad(c["state_code"], 2, "0"), F.lpad(c["county_code"], 3, "0")).alias("county_fips"),
        nic_date(c["open_date"]).alias("open_date"),
        nic_date(c["close_date"]).alias("close_date"),
        F.lit(record_type).alias("record_type"))


dim_inst = (nic_frame("nic_attributes_active", "institution")
            .unionByName(nic_frame("nic_attributes_closed", "institution"))
            .unionByName(nic_frame("nic_attributes_branches", "branch"))
            .dropDuplicates(["rssd_id", "record_type"]))
write_table(dim_inst, tbl("silver", "dim_institution"))
dim_inst = spark.table(tbl("silver", "dim_institution"))

tc = NIC_TRANS_COLS
loc = dim_inst.where("record_type = 'institution'").select("rssd_id", "county_fips")
events = (latest_snapshot("nic_transformations")
          .select(F.col(tc["predecessor"]).alias("predecessor_rssd"),
                  F.col(tc["successor"]).alias("successor_rssd"),
                  parse_date(tc["date"], NIC_DATE_FORMAT).alias("event_date"),
                  F.col(tc["code"]).alias("transformation_code"))
          .join(loc.toDF("predecessor_rssd", "predecessor_county_fips"), "predecessor_rssd", "left")
          .join(loc.toDF("successor_rssd", "successor_county_fips"), "successor_rssd", "left"))
write_table(events, tbl("silver", "fact_institution_events"))

# COMMAND ----------

# Data-quality check: NIC rows whose county doesn't match dim_geography
# (expect some: foreign entities, territories, pre-2022 Connecticut counties)
display(dim_inst.join(spark.table(tbl("silver", "dim_geography")).select("county_fips", F.lit(1).alias("m")),
                      "county_fips", "left")
        .groupBy("record_type")
        .agg(F.count("*").alias("rows"), F.round(F.avg(F.coalesce("m", F.lit(0))) * 100, 1).alias("pct_matched")))

# COMMAND ----------

# MAGIC %md ## fact_econ (long format)

# COMMAND ----------

ECON_COLS = ["geo_level", "geo_id", "date", "metric", "value", "source"]

fred = (spark.table(tbl("bronze", "fred_observations"))
        .join(spark.table(tbl("bronze", "fred_series_meta")).select("series_id", "geo_level", "geo_id", "metric"), "series_id")
        .select("geo_level", "geo_id", F.to_date("date").alias("date"), "metric", num("value").alias("value"))
        .withColumn("value", F.when(F.col("metric") == "population_k", F.col("value") * 1000).otherwise(F.col("value")))
        .withColumn("metric", F.when(F.col("metric") == "population_k", F.lit("population")).otherwise(F.col("metric")))
        .withColumn("source", F.lit("FRED")))

laus = (spark.table(tbl("bronze", "bls_laus_county"))
        .where(F.col("series_id").startswith("LAUCN") & F.col("period").rlike("^M(0[1-9]|1[0-2])$"))
        .withColumn("measure", F.substring("series_id", 19, 2))
        .where(F.col("measure").isin(list(LAUS_MEASURES)))
        .select(F.lit("county").alias("geo_level"),
                F.substring("series_id", 6, 5).alias("geo_id"),
                F.expr("make_date(try_cast(year as int), try_cast(substring(period, 2, 2) as int), 1)").alias("date"),
                map_values(LAUS_MEASURES, F.col("measure")).alias("metric"),
                num("value").alias("value"),
                F.lit("BLS LAUS").alias("source")))

acs_raw = spark.table(tbl("bronze", "census_acs5"))
acs = (acs_raw.select(F.concat(F.lpad("state", 2, "0"), F.lpad("county", 3, "0")).alias("geo_id"),
                      F.expr("try_cast(acs_year as int)").alias("year"),
                      F.explode(F.create_map(*[x for v, m in ACS_VARS.items() for x in (F.lit(m), num(v.lower()))]))
                       .alias("metric", "value"))
       .select(F.lit("county").alias("geo_level"), "geo_id",
               F.expr("make_date(year, 1, 1)").alias("date"), "metric",
               F.when(F.col("value") < 0, F.lit(None)).otherwise(F.col("value")).alias("value"),  # ACS sentinels
               F.lit("Census ACS5").alias("source")))

pep_raw = spark.table(tbl("bronze", "census_pep")).where("sumlev = '050'")
year_cols = [c for c in pep_raw.columns if re.fullmatch(r"popestimate\d{4}", c)]
pep = (pep_raw.select(F.concat(F.lpad("state", 2, "0"), F.lpad("county", 3, "0")).alias("geo_id"),
                      F.expr("try_cast(vintage as int)").alias("vintage"),
                      F.explode(F.create_map(*[x for c in year_cols for x in (F.lit(int(c[-4:])), num(c))]))
                       .alias("year", "value"))
       .where("value is not null")
       .withColumn("rn", F.row_number().over(Window.partitionBy("geo_id", "year").orderBy(F.desc("vintage"))))
       .where("rn = 1")
       .select(F.lit("county").alias("geo_level"), "geo_id", F.expr("make_date(year, 1, 1)").alias("date"),
               F.lit("population").alias("metric"), "value", F.lit("Census PEP").alias("source")))

fact_econ = (fred.select(ECON_COLS).unionByName(laus.select(ECON_COLS))
             .unionByName(acs.select(ECON_COLS)).unionByName(pep.select(ECON_COLS))
             .where("date is not null"))
write_table(fact_econ, tbl("silver", "fact_econ"))

# COMMAND ----------

# MAGIC %md ## Weather: station → county map, then county-day facts
# MAGIC Each county averages stations within 50 km of its centroid; if none, it uses the nearest one.
# MAGIC Temperature and precipitation are mapped separately because many stations only report rain.

# COMMAND ----------

RADIUS_KM = 50

ghcn = (spark.table(tbl("bronze", "noaa_ghcn_daily")).where("q_flag is null")
        .select(F.col("id").alias("station_id"), parse_date("date", "yyyyMMdd").alias("date"),
                "element", num("data_value").alias("v")))


def map_stations(station_ids, group):
    st = (spark.table(tbl("bronze", "noaa_ghcn_stations")).join(station_ids, "station_id")
          .select("station_id", num("lat").alias("s_lat"), num("lon").alias("s_lon")))
    cty = spark.table(tbl("silver", "dim_geography")).select("county_fips", "lat", "lon")
    pairs = (cty.join(F.broadcast(st),
                      (F.abs(F.col("lat") - F.col("s_lat")) <= 1.0) & (F.abs(F.col("lon") - F.col("s_lon")) <= 1.5))
             .withColumn("dist_km", haversine_km(F.col("lat"), F.col("lon"), F.col("s_lat"), F.col("s_lon"))))
    return (pairs.withColumn("rank", F.row_number().over(Window.partitionBy("county_fips").orderBy("dist_km")))
            .where((F.col("dist_km") <= RADIUS_KM) | (F.col("rank") == 1))
            .select("county_fips", "station_id", F.round("dist_km", 1).alias("dist_km"),
                    F.lit(group).alias("element_group")))


station_map = (map_stations(ghcn.where("element = 'TMAX'").select("station_id").distinct(), "temp")
               .unionByName(map_stations(ghcn.where("element = 'PRCP'").select("station_id").distinct(), "prcp")))
write_table(station_map, tbl("silver", "map_station_county"))
station_map = spark.table(tbl("silver", "map_station_county"))

temp = (ghcn.where("element in ('TMAX', 'TMIN')")
        .join(station_map.where("element_group = 'temp'").select("station_id", "county_fips"), "station_id")
        .groupBy("county_fips", "date")
        .agg((F.avg(F.when(F.col("element") == "TMAX", F.col("v"))) / 10).alias("tmax_c"),
             (F.avg(F.when(F.col("element") == "TMIN", F.col("v"))) / 10).alias("tmin_c"),
             F.countDistinct("station_id").alias("n_temp_stations")))

prcp = (ghcn.where("element = 'PRCP'")
        .join(station_map.where("element_group = 'prcp'").select("station_id", "county_fips"), "station_id")
        .groupBy("county_fips", "date")
        .agg((F.avg("v") / 10).alias("prcp_mm"), F.countDistinct("station_id").alias("n_prcp_stations")))

t = F.col("tavg_c")
weather = (temp.join(prcp, ["county_fips", "date"], "full")
           .withColumn("tavg_c", F.round((F.col("tmax_c") + F.col("tmin_c")) / 2, 1))
           .withColumn("temp_bucket",
                       F.when(t.isNull(), F.lit(None).cast("string"))
                        .when(t < -10, "a. below -10°C").when(t < 0, "b. -10 to 0°C")
                        .when(t < 10, "c. 0 to 10°C").when(t < 20, "d. 10 to 20°C")
                        .when(t < 30, "e. 20 to 30°C").otherwise("f. 30°C+")))
write_table(weather, tbl("silver", "fact_weather_daily"))

# COMMAND ----------

# MAGIC %md ## fact_games (two rows per game: one per team)

# COMMAND ----------

g = (spark.table(tbl("bronze", "nflverse_games"))
     .select("game_id", F.expr("try_cast(season as int)").alias("season"), "game_type",
             F.expr("try_cast(week as int)").alias("week"), parse_date("gameday", "yyyy-MM-dd").alias("date"),
             "home_team", "away_team", num("home_score").alias("home_pts"), num("away_score").alias("away_pts"))
     .where("home_pts is not null and away_pts is not null"))


def side(team, opp, pf, pa, is_home):
    return g.select("game_id", "season", "game_type", "week", "date",
                    F.col(team).alias("team"), F.col(opp).alias("opponent"), F.lit(is_home).alias("is_home"),
                    F.col(pf).alias("points_for"), F.col(pa).alias("points_against"))


games = (side("home_team", "away_team", "home_pts", "away_pts", True)
         .unionByName(side("away_team", "home_team", "away_pts", "home_pts", False))
         .withColumn("margin", F.col("points_for") - F.col("points_against"))
         .withColumn("result", F.when(F.col("margin") > 0, "W").when(F.col("margin") < 0, "L").otherwise("T")))
write_table(games, tbl("silver", "fact_games"))

# COMMAND ----------

# MAGIC %md ## fact_spending_daily
# MAGIC `spend_all` = card spending vs. Jan 2020 baseline (a fraction: -0.05 = 5% below). It's a 7-day moving average.

# COMMAND ----------

spend = (spark.table(tbl("bronze", "oi_spending_county_daily"))
         .select(F.lpad("countyfips", 5, "0").alias("county_fips"),
                 F.expr("make_date(try_cast(year as int), try_cast(month as int), try_cast(day as int))").alias("date"),
                 num("spend_all").alias("spend_all_vs_jan2020"))
         .where("date is not null and spend_all_vs_jan2020 is not null"))
write_table(spend, tbl("silver", "fact_spending_daily"))

# COMMAND ----------

# MAGIC %md ## fact_branch_deposits (FDIC SOD, as of June 30 each year)

# COMMAND ----------

deposits = (spark.table(tbl("bronze", "fdic_sod"))
            .select(F.expr("try_cast(year as int)").alias("year"),
                    F.lpad(F.regexp_replace("stcntybr", r"\.0$", ""), 5, "0").alias("county_fips"),
                    "cert", "rssdid", "uninumbr", F.col("namefull").alias("bank_name"),
                    F.when(F.col("rssdhcr").isNull() | F.col("rssdhcr").isin("0", ""), F.col("rssdid"))
                     .otherwise(F.col("rssdhcr")).alias("org_id"),   # top holding company, else the bank
                    num("depsumbr").alias("deposits_thousands")))
write_table(deposits, tbl("silver", "fact_branch_deposits"))
