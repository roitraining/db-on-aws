# Databricks notebook source
# MAGIC %md
# MAGIC # 03c · Bronze from staged files + GitHub-native sources
# MAGIC Serverless egress here allows GitHub but blocks S3 and general internet, so:
# MAGIC - **nflverse + Opportunity Insights**: ingested directly from raw.githubusercontent.com
# MAGIC - **NIC zips**: copied from the `training_nic` landing volume (staged by the course repo) and unzipped
# MAGIC - **Everything else** (delineation, ACS, PEP, LAUS, SOD, NOAA): loaded from files fetched
# MAGIC   locally and uploaded to `/Volumes/<catalog>/bronze/landing/`
# MAGIC
# MAGIC Missing optional sources (NIC relationships/transformations, NOAA) get **empty tables with the
# MAGIC right schema** so `04_silver` and `05_gold` run unmodified — their outputs are just empty.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import glob as globmod

SNAP = str(datetime.date.today())
NIC_SRC_ZIPS = "/Volumes/training_nic/raw/landing/nic_zips"


def load_csv_bronze(table, path, source, sep=",", encoding="UTF-8"):
    df = clean_columns(spark.read.option("header", True).option("sep", sep)
                       .option("encoding", encoding).csv(path))
    df = df.select(*[F.trim(F.col(c)).cast("string").alias(c) for c in df.columns])
    write_bronze(df, table, source)


# --- Reference: CBSA delineation ---
def load_delineation():
    load_csv_bronze("ref_cbsa_delineation", f"{LANDING}/ref/cbsa_delineation.csv", "OMB delineation 2023 (local fetch)")


# --- Census ACS + PEP ---
def load_acs():
    if not os.path.exists(f"{LANDING}/census/census_acs5.csv"):
        print("WARNING: census_acs5.csv not staged (Census API key pending) — skipping (empty fallback later)")
        return
    load_csv_bronze("census_acs5", f"{LANDING}/census/census_acs5.csv", "Census ACS5 API (local fetch)")


def load_pep():
    files = [f for f in dbutils.fs.ls(f"{LANDING}/census/") if "census_pep_" in f.name]
    dfs = [clean_columns(spark.read.option("header", True).csv(f.path)) for f in files]
    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d, allowMissingColumns=True)
    df = df.select(*[F.col(c).cast("string") for c in df.columns])
    write_bronze(df, "census_pep", "Census PEP county totals (local fetch)")


# --- BLS LAUS ---
def load_laus():
    # .tsv or .tsv.gz both match (repo-staged copies are gzipped)
    load_csv_bronze("bls_laus_county", f"{LANDING}/bls/bls_laus_county.tsv*", "BLS LAUS la.data.64.County (local fetch)", sep="\t")


# --- FDIC SOD ---
def load_sod():
    load_csv_bronze("fdic_sod", f"{LANDING}/fdic/fdic_sod.csv*", "FDIC SOD API (local fetch)")


# --- NOAA ---
def load_noaa_stations():
    lines = spark.read.text(f"{LANDING}/noaa/noaa_stations_us.txt")
    df = lines.select(
        F.trim(F.expr("substring(value, 1, 11)")).alias("station_id"),
        F.trim(F.expr("substring(value, 13, 8)")).alias("lat"),
        F.trim(F.expr("substring(value, 22, 9)")).alias("lon"),
        F.trim(F.expr("substring(value, 32, 6)")).alias("elevation"),
        F.trim(F.expr("substring(value, 39, 2)")).alias("state"),
        F.trim(F.expr("substring(value, 42, 30)")).alias("name"))
    write_bronze(df, "noaa_ghcn_stations", "NOAA GHCN stations (local fetch)")


GHCN_SCHEMA = ("id string, date string, element string, data_value string, "
               "m_flag string, q_flag string, s_flag string, obs_time string")


def load_noaa_daily():
    try:
        files = [f for f in dbutils.fs.ls(f"{LANDING}/noaa/") if f.name.startswith("noaa_ghcn_") and f.name.endswith(".csv.gz")]
    except Exception:
        files = []
    if not files:
        print("WARNING: no noaa_ghcn_*.csv.gz in landing/noaa — skipping (empty fallback later)")
        return
    for fobj in files:
        year = int(fobj.name.replace("noaa_ghcn_", "").replace(".csv.gz", ""))
        df = (spark.read.schema(GHCN_SCHEMA).option("header", True).csv(fobj.path)
              .withColumn("year", F.lit(year)))
        write_bronze(df, "noaa_ghcn_daily", "NOAA GHCN-Daily (local fetch)", replace_where=f"year = {year}")


# --- GitHub-native: nflverse + Opportunity Insights ---
def ingest_nflverse():
    url = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
    pdf = pd.read_csv(io.BytesIO(http_get(url).content), dtype=str)
    write_bronze(pandas_to_spark(pdf), "nflverse_games", "nflverse games.csv")


def ingest_oi_spending():
    url = ("https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/main/data/"
           "Affinity%20-%20County%20-%20Daily.csv")
    path = download_to_volume(url, "oi/affinity_county_daily.csv")
    df = clean_columns(spark.read.option("header", True).csv(path))
    write_bronze(df, "oi_spending_county_daily", "Opportunity Insights Economic Tracker")


# --- NIC: unzip from training_nic staged zips + any extras uploaded to our landing ---
NIC_TABLE_PATTERNS = {
    "nic_attributes_active": "ATTRIBUTES_ACTIVE",
    "nic_attributes_closed": "ATTRIBUTES_CLOSED",
    "nic_attributes_branches": "ATTRIBUTES_BRANCHES",
    "nic_relationships": "RELATIONSHIPS",
    "nic_transformations": "TRANSFORMATIONS",
}


def stage_nic_zips():
    os.makedirs(f"{LANDING}/nic", exist_ok=True)
    zip_sources = []
    for src in [NIC_SRC_ZIPS, f"{LANDING}/nic_zips"]:
        try:
            zip_sources += [f.path.replace("dbfs:", "") for f in dbutils.fs.ls(src) if f.name.endswith(".zip")]
        except Exception:
            pass
    staged = []
    for zpath in zip_sources:
        with zipfile.ZipFile(zpath) as z:
            for name in z.namelist():
                if name.upper().endswith(".CSV"):
                    target = f"{LANDING}/nic/{os.path.basename(name)}"
                    if not os.path.exists(target):
                        with z.open(name) as src_f, open(target, "wb") as dst_f:
                            shutil.copyfileobj(src_f, dst_f)
                    staged.append(os.path.basename(name))
    print("staged NIC csvs:", staged)


def load_nic():
    stage_nic_zips()
    present = [f.name.upper() for f in dbutils.fs.ls(f"{LANDING}/nic/")]
    for table, pattern in NIC_TABLE_PATTERNS.items():
        matches = [n for n in present if pattern in n]
        if not matches:
            print(f"WARNING: no file matching *{pattern}* — skipping {table} (empty fallback later)")
            continue
        df = (spark.read.option("header", True).option("inferSchema", False)
              .csv(f"{LANDING}/nic/*{pattern}*"))
        df = clean_columns(df).withColumn("_snapshot_date", F.lit(SNAP).cast("date"))
        write_bronze(df, table, "FFIEC NIC", replace_where=f"_snapshot_date = DATE'{SNAP}'")


# --- Empty fallbacks so 04/05 run unmodified ---
def empty_fallbacks():
    fallbacks = {
        "census_acs5": ("name string, b01003_001e string, b19013_001e string, b17001_001e string, "
                        "b17001_002e string, b25077_001e string, b23025_003e string, b23025_005e string, "
                        "state string, county string, acs_year string"),
        "nic_relationships": "id_rssd_parent string, id_rssd_offspring string, _snapshot_date date",
        "nic_transformations": "id_rssd_predecessor string, id_rssd_successor string, dt_trans string, trnsfm_cd string, _snapshot_date date",
        "noaa_ghcn_stations": "station_id string, lat string, lon string, elevation string, state string, name string",
        "noaa_ghcn_daily": GHCN_SCHEMA + ", year int",
    }
    for table, schema in fallbacks.items():
        name = tbl("bronze", table)
        if not spark.catalog.tableExists(name):
            print(f"WARNING: creating EMPTY fallback {name}")
            df = (spark.createDataFrame([], schema)
                  .withColumn("_source", F.lit("EMPTY FALLBACK"))
                  .withColumn("_ingested_at", F.current_timestamp()))
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)

# COMMAND ----------

run_steps([
    ("ref_cbsa_delineation", load_delineation),
    ("census_acs5", load_acs),
    ("census_pep", load_pep),
    ("bls_laus_county", load_laus),
    ("fdic_sod", load_sod),
    ("noaa_stations", load_noaa_stations),
    ("noaa_daily", load_noaa_daily),
    ("nflverse", ingest_nflverse),
    ("oi_spending", ingest_oi_spending),
    ("nic", load_nic),
    ("empty_fallbacks", empty_fallbacks),
])

# COMMAND ----------

for t in ["ref_cbsa_delineation", "census_acs5", "census_pep", "bls_laus_county", "fdic_sod",
          "noaa_ghcn_stations", "noaa_ghcn_daily", "nflverse_games", "oi_spending_county_daily",
          "nic_attributes_active", "nic_attributes_closed", "nic_attributes_branches",
          "nic_relationships", "nic_transformations"]:
    name = tbl("bronze", t)
    n = spark.table(name).count() if spark.catalog.tableExists(name) else "MISSING"
    print(f"{t:<28}{n}")
