"""Generate day2/3/4 incremental CSVs from the quality-batch template and upload to the staging volume path."""
import configparser, os, urllib.request

CFG = configparser.ConfigParser(); CFG.read(os.path.expanduser("~/.databrickscfg"))
HOST = CFG[os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")]["host"].strip().rstrip("/")
TOKEN = CFG[os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")]["token"].strip()
SP = os.path.dirname(os.path.abspath(__file__))

lines = open(os.path.join(SP, "quality_batch.csv")).read().splitlines()
HEADER = lines[0]
NCOLS = 74
IDX = {"id": 0, "start": 1, "chtr": 6, "name": 27, "city": 36, "state": 39}

def row(rssd, name, city, state, chtr="300", start="2026-10-01"):
    f = [""] * NCOLS
    f[IDX["id"]] = str(rssd) if rssd is not None else ""
    f[IDX["start"]] = start
    f[IDX["chtr"]] = chtr
    f[IDX["name"]] = name
    f[IDX["city"]] = city
    f[IDX["state"]] = state
    return ",".join(f)

def day_file(day, state, city, n_clean, n_nokey, n_shortcity, id_base):
    out = [HEADER]
    for i in range(1, n_clean + 1):
        out.append(row(id_base + i, f"DAY {day} EXPANSION BRANCH {i}", city, state))
    for i in range(1, n_nokey + 1):
        out.append(row(None, f"DAY {day} BAD BRANCH {i} (NO KEY)", city, state))
    for i in range(1, n_shortcity + 1):
        out.append(row(id_base + 900000 + i, f"DAY {day} BAD BRANCH {i} (SHORT CITY)", "X", state))
    return "\n".join(out) + "\n"

FILES = {
    "branches_day2.csv": day_file(2, "TX", "DALLAS", 300, 3, 2, 95200000),
    "branches_day3.csv": day_file(3, "NY", "ALBANY", 250, 2, 1, 95300000),
    "branches_day4.csv": day_file(4, "FL", "MIAMI", 200, 1, 1, 95400000),
}

def put(path, content):
    req = urllib.request.Request(HOST + "/api/2.0/fs/files" + path + "?overwrite=true",
                                 data=content.encode(), method="PUT")
    req.add_header("Authorization", f"Bearer {TOKEN}")
    req.add_header("Content-Type", "application/octet-stream")
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.status

req = urllib.request.Request(HOST + "/api/2.0/fs/directories/Volumes/training_nic/raw/landing/incremental_days", method="PUT")
req.add_header("Authorization", f"Bearer {TOKEN}")
urllib.request.urlopen(req, timeout=60)
print("staging dir ready", flush=True)

for name, content in FILES.items():
    open(os.path.join(SP, name), "w", newline="").write(content)
    st = put(f"/Volumes/training_nic/raw/landing/incremental_days/{name}", content)
    print(f"uploaded {name}: HTTP {st}, {len(content.splitlines())-1} data rows", flush=True)
