# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Config & helpers
# MAGIC Shared settings for the NIC + economic enrichment pipeline.
# MAGIC Every other notebook starts with `%run ./00_config`.

# COMMAND ----------

dbutils.widgets.text("catalog", "nic_econ", "Catalog")
dbutils.widgets.text("start_year", "2015", "Start year")
dbutils.widgets.text("end_year", "2025", "End year")
dbutils.widgets.text("weather_start_year", "2020", "Weather start year")
dbutils.widgets.text("contact_email", "you@example.com", "Contact email (API User-Agent)")

CATALOG = dbutils.widgets.get("catalog")
START_YEAR = int(dbutils.widgets.get("start_year"))
END_YEAR = int(dbutils.widgets.get("end_year"))
WEATHER_START_YEAR = int(dbutils.widgets.get("weather_start_year"))  # GHCN yearly files are ~1 GB each
CONTACT_EMAIL = dbutils.widgets.get("contact_email")                 # BLS rejects requests without one

LANDING = f"/Volumes/{CATALOG}/bronze/landing"
SECRET_SCOPE = "nic_econ"  # keys: fred_api_key (required), census_api_key (optional)

# COMMAND ----------

# MAGIC %md ## Source settings

# COMMAND ----------

GAZETTEER_YEAR = 2024     # county centroids + land area
DELINEATION_YEAR = 2023   # OMB county -> CBSA/MSA delineation
ACS_LATEST_YEAR = 2024    # latest ACS 5-year release (2020-2024)

PEP_FILES = {  # county population estimates; newer vintage wins where years overlap
    2020: "https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv",
    2024: "https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/totals/co-est2024-alldata.csv",
}

FRED_NATIONAL = {
    "UNRATE": "unemployment_rate",
    "FEDFUNDS": "fed_funds_rate",
    "DGS10": "treasury_10y",
    "T10Y2Y": "yield_curve_10y_2y",
    "MORTGAGE30US": "mortgage_30y",
    "CPIAUCSL": "cpi",
    "DRALACBS": "delinquency_rate_all_loans",
    "CORALACBS": "chargeoff_rate_all_loans",
}
FRED_STATE = {  # {st} = postal abbreviation
    "{st}UR": "unemployment_rate",
    "{st}POP": "population_k",
    "MEHOINUS{st}A646N": "median_hh_income",
    "{st}PCPI": "per_capita_income",
    "{st}STHPI": "house_price_index",
    "{st}NGSP": "gdp_nominal_musd",
}

ACS_VARS = {
    "B01003_001E": "population_acs",
    "B19013_001E": "median_hh_income",
    "B17001_001E": "poverty_universe",
    "B17001_002E": "poverty_count",
    "B25077_001E": "median_home_value",
    "B23025_003E": "civilian_labor_force_acs",
    "B23025_005E": "unemployed_acs",
}

LAUS_MEASURES = {"03": "unemployment_rate", "04": "unemployed", "05": "employed", "06": "labor_force"}

# NIC column names after header cleanup (lower-case, '#' stripped).
# Verify against your extract with the profile cell in 02_bronze_nic.
NIC_COLS = {
    "rssd_id": "id_rssd",
    "name": "nm_lgl",
    "entity_type": "entity_type",
    "city": "city",
    "state_code": "state_cd",   # FIPS state
    "county_code": "county_cd",  # FIPS county (3-digit) — real 2026 NIC extract uses county_cd, not cnty_cd
    "open_date": "dt_open",
    "close_date": "dt_end",     # 99991231 = still active
}
NIC_TRANS_COLS = {
    "predecessor": "id_rssd_predecessor",
    "successor": "id_rssd_successor",
    "date": "dt_trans",
    "code": "trnsfm_cd",
}
NIC_DATE_FORMAT = "yyyyMMdd"

# Entity types counted as "institutions" in gold (None = everything in the active/closed files)
INSTITUTION_ENTITY_TYPES = ["NAT", "SMB", "NMB", "SSB", "SAL", "FSB", "CPB"]

# Team -> home county for the game-day spending table
TEAM_HOME_COUNTY = {"CHI": "17031"}  # Bears -> Cook County. Add more, e.g. "GB": "55009"

# COMMAND ----------

# MAGIC %md ## Helpers

# COMMAND ----------

import datetime, io, os, re, shutil, time, zipfile
import requests
import pandas as pd
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType

HTTP_HEADERS = {"User-Agent": f"nic-econ-pipeline/1.0 ({CONTACT_EMAIL})"}


def tbl(layer, name):
    return f"{CATALOG}.{layer}.{name}"


def secret(key, default=None):
    try:
        return dbutils.secrets.get(SECRET_SCOPE, key)
    except Exception:
        return default


def http_get(url, params=None, stream=False, retries=5):
    last = None
    for attempt in range(retries):
        r = requests.get(url, params=params, headers=HTTP_HEADERS, timeout=300, stream=stream)
        if r.status_code == 200:
            return r
        last = r
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        break
    last.raise_for_status()
    raise RuntimeError(f"GET failed: {url}")


def download_to_volume(url, rel_path):
    path = f"{LANDING}/{rel_path}"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with http_get(url, stream=True) as r:
        r.raw.decode_content = True
        with open(path, "wb") as f:
            shutil.copyfileobj(r.raw, f, length=16 * 1024 * 1024)
    return path


def clean_col(c):
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(c)).strip("_").lower()


def clean_columns(df):
    return df.toDF(*[clean_col(c) for c in df.columns])


def pandas_to_spark(pdf):
    """All-string bronze frame from pandas (keeps raw values, avoids type inference surprises)."""
    pdf = pdf.copy()
    pdf.columns = [clean_col(c) for c in pdf.columns]
    pdf = pdf.astype("string")
    schema = StructType([StructField(c, StringType()) for c in pdf.columns])
    rows = pdf.astype(object).where(pdf.notna(), None).values.tolist()
    return spark.createDataFrame(rows, schema)


def write_table(df, name, mode="overwrite", replace_where=None):
    w = df.write.mode(mode)
    if replace_where and spark.catalog.tableExists(name):
        w = w.option("replaceWhere", replace_where).option("mergeSchema", "true")
    elif mode == "overwrite":
        w = w.option("overwriteSchema", "true")
    w.saveAsTable(name)


def write_bronze(df, table, source, replace_where=None):
    df = df.withColumn("_source", F.lit(source)).withColumn("_ingested_at", F.current_timestamp())
    write_table(df, tbl("bronze", table), replace_where=replace_where)


# ANSI-safe parsing (serverless runs with ANSI mode on)
def num(col_name):
    return F.expr(f"try_cast(trim(`{col_name}`) as double)")


def parse_date(col_name, fmt):
    return F.expr(f"cast(try_to_timestamp(trim(cast(`{col_name}` as string)), '{fmt}') as date)")


def map_values(mapping, col):
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])[col]


def haversine_km(lat1, lon1, lat2, lon2):
    a = (F.sin(F.radians(lat2 - lat1) / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(F.radians(lon2 - lon1) / 2) ** 2)
    return 2 * 6371 * F.asin(F.sqrt(a))


def run_steps(steps):
    results = []
    for name, fn in steps:
        t0 = time.time()
        try:
            fn()
            results.append((name, "ok", round(time.time() - t0, 1), ""))
        except Exception as e:
            results.append((name, "FAILED", round(time.time() - t0, 1), str(e)[:500]))
    display(spark.createDataFrame(results, "step string, status string, seconds double, error string"))
    failed = [r[0] for r in results if r[1] != "ok"]
    if failed:
        raise RuntimeError(f"Steps failed: {failed}")
