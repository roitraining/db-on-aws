# Databricks notebook source
# MAGIC %md
# MAGIC # 03d · Bronze: one source per task
# MAGIC Parameterized bronze loader — the job DAG runs one task per source with `source` set to:
# MAGIC `refs | fred | census | bls | fdic | noaa | github | nic`.
# MAGIC Same loaders as `03c_bronze_staged`, split so each source is its own DAG node.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("source", "refs", "Source to load")
SOURCE = dbutils.widgets.get("source")
SNAP = str(datetime.date.today())
NIC_SRC_ZIPS = "/Volumes/training_nic/raw/landing/nic_zips"
GHCN_SCHEMA = ("id string, date string, element string, data_value string, "
               "m_flag string, q_flag string, s_flag string, obs_time string")


def load_csv_bronze(table, path, source, sep=",", encoding="UTF-8"):
    df = clean_columns(spark.read.option("header", True).option("sep", sep)
                       .option("encoding", encoding).csv(path))
    df = df.select(*[F.trim(F.col(c)).cast("string").alias(c) for c in df.columns])
    write_bronze(df, table, source)

# COMMAND ----------

def src_refs():
    df = clean_columns(spark.read.option("header", True).option("sep", "|").csv(f"{LANDING}/ref/state.txt"))
    write_bronze(df.select(*[F.col(c).cast("string") for c in df.columns]), "ref_states", "Census state.txt (staged)")
    df = clean_columns(spark.read.option("header", True).option("sep", "\t")
                       .option("encoding", "ISO-8859-1").csv(f"{LANDING}/ref/gazetteer_counties.txt"))
    write_bronze(df.select(*[F.trim(F.col(c)).cast("string").alias(c) for c in df.columns]),
                 "ref_county_gazetteer", f"Census Gazetteer {GAZETTEER_YEAR} (staged)")
    load_csv_bronze("ref_cbsa_delineation", f"{LANDING}/ref/cbsa_delineation.csv", "OMB delineation 2023 (staged)")


def src_fred():
    load_csv_bronze("fred_series_meta", f"{LANDING}/fred/fred_series_meta.csv", "FRED (staged)")
    load_csv_bronze("fred_observations", f"{LANDING}/fred/fred_observations.csv", "FRED API (staged)")


def src_census():
    files = [f for f in dbutils.fs.ls(f"{LANDING}/census/") if "census_pep_" in f.name]
    dfs = [clean_columns(spark.read.option("header", True).csv(f.path)) for f in files]
    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d, allowMissingColumns=True)
    write_bronze(df.select(*[F.col(c).cast("string") for c in df.columns]), "census_pep", "Census PEP (staged)")
    if os.path.exists(f"{LANDING}/census/census_acs5.csv"):
        load_csv_bronze("census_acs5", f"{LANDING}/census/census_acs5.csv", "Census ACS5 (staged)")
    elif not spark.catalog.tableExists(tbl("bronze", "census_acs5")):
        print("WARNING: ACS not staged (Census API key pending) — creating empty fallback")
        schema = ("name string, b01003_001e string, b19013_001e string, b17001_001e string, "
                  "b17001_002e string, b25077_001e string, b23025_003e string, b23025_005e string, "
                  "state string, county string, acs_year string")
        (spark.createDataFrame([], schema)
         .withColumn("_source", F.lit("EMPTY FALLBACK")).withColumn("_ingested_at", F.current_timestamp())
         .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl("bronze", "census_acs5")))


def src_bls():
    load_csv_bronze("bls_laus_county", f"{LANDING}/bls/bls_laus_county.tsv*", "BLS LAUS (staged)", sep="\t")


def src_fdic():
    load_csv_bronze("fdic_sod", f"{LANDING}/fdic/fdic_sod.csv*", "FDIC SOD API (staged)")
    fin = f"{LANDING}/fdic/fdic_financials_2024q4.csv"
    if os.path.exists(fin):
        df = clean_columns(spark.read.option("header", True).csv(fin))
        df = (df.select(F.col("cert").cast("string").alias("cert"),
                        F.col("repdte").cast("string").alias("repdte"),
                        num("netinc").alias("netinc_thousands"),
                        num("roa").alias("roa"), num("roe").alias("roe"),
                        num("asset").alias("assets_thousands"),
                        num("dep").alias("deposits_thousands")))
        write_bronze(df, "fdic_financials", "FDIC financials API (staged)")


def src_noaa():
    lines = spark.read.text(f"{LANDING}/noaa/noaa_stations_us.txt")
    df = lines.select(
        F.trim(F.expr("substring(value, 1, 11)")).alias("station_id"),
        F.trim(F.expr("substring(value, 13, 8)")).alias("lat"),
        F.trim(F.expr("substring(value, 22, 9)")).alias("lon"),
        F.trim(F.expr("substring(value, 32, 6)")).alias("elevation"),
        F.trim(F.expr("substring(value, 39, 2)")).alias("state"),
        F.trim(F.expr("substring(value, 42, 30)")).alias("name"))
    write_bronze(df, "noaa_ghcn_stations", "NOAA GHCN stations (staged)")
    files = [f for f in dbutils.fs.ls(f"{LANDING}/noaa/") if f.name.startswith("noaa_ghcn_") and f.name.endswith(".csv.gz")]
    for fobj in files:
        year = int(fobj.name.replace("noaa_ghcn_", "").replace(".csv.gz", ""))
        df = (spark.read.schema(GHCN_SCHEMA).option("header", True).csv(fobj.path)
              .withColumn("year", F.lit(year)))
        write_bronze(df, "noaa_ghcn_daily", "NOAA GHCN-Daily (staged)", replace_where=f"year = {year}")


def src_github():
    url = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
    pdf = pd.read_csv(io.BytesIO(http_get(url).content), dtype=str)
    write_bronze(pandas_to_spark(pdf), "nflverse_games", "nflverse games.csv")
    url = ("https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/main/data/"
           "Affinity%20-%20County%20-%20Daily.csv")
    path = download_to_volume(url, "oi/affinity_county_daily.csv")
    df = clean_columns(spark.read.option("header", True).csv(path))
    write_bronze(df, "oi_spending_county_daily", "Opportunity Insights Economic Tracker")


def src_nic():
    os.makedirs(f"{LANDING}/nic", exist_ok=True)
    zip_sources = []
    for src in [NIC_SRC_ZIPS, f"{LANDING}/nic_zips"]:
        try:
            zip_sources += [f.path.replace("dbfs:", "") for f in dbutils.fs.ls(src) if f.name.endswith(".zip")]
        except Exception:
            pass
    for zpath in zip_sources:
        with zipfile.ZipFile(zpath) as z:
            for name in z.namelist():
                if name.upper().endswith(".CSV"):
                    target = f"{LANDING}/nic/{os.path.basename(name)}"
                    if not os.path.exists(target):
                        with z.open(name) as s, open(target, "wb") as d:
                            shutil.copyfileobj(s, d)
    present = [f.name.upper() for f in dbutils.fs.ls(f"{LANDING}/nic/")]
    patterns = {"nic_attributes_active": "ATTRIBUTES_ACTIVE", "nic_attributes_closed": "ATTRIBUTES_CLOSED",
                "nic_attributes_branches": "ATTRIBUTES_BRANCHES", "nic_relationships": "RELATIONSHIPS",
                "nic_transformations": "TRANSFORMATIONS"}
    for table, pattern in patterns.items():
        if not [n for n in present if pattern in n]:
            print(f"WARNING: no *{pattern}* file — empty fallback for {table}")
            continue
        df = (spark.read.option("header", True).option("inferSchema", False).csv(f"{LANDING}/nic/*{pattern}*"))
        df = clean_columns(df).withColumn("_snapshot_date", F.lit(SNAP).cast("date"))
        write_bronze(df, table, "FFIEC NIC", replace_where=f"_snapshot_date = DATE'{SNAP}'")
    fallbacks = {
        "nic_relationships": "id_rssd_parent string, id_rssd_offspring string, _snapshot_date date",
        "nic_transformations": "id_rssd_predecessor string, id_rssd_successor string, dt_trans string, trnsfm_cd string, _snapshot_date date",
    }
    for table, schema in fallbacks.items():
        if not spark.catalog.tableExists(tbl("bronze", table)):
            (spark.createDataFrame([], schema)
             .withColumn("_source", F.lit("EMPTY FALLBACK")).withColumn("_ingested_at", F.current_timestamp())
             .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl("bronze", table)))

# COMMAND ----------

DISPATCH = {"refs": src_refs, "fred": src_fred, "census": src_census, "bls": src_bls,
            "fdic": src_fdic, "noaa": src_noaa, "github": src_github, "nic": src_nic}
if SOURCE not in DISPATCH:
    raise ValueError(f"unknown source '{SOURCE}' — expected one of {sorted(DISPATCH)}")
print(f"Loading source: {SOURCE}")
DISPATCH[SOURCE]()
print(f"Source {SOURCE}: done")
