# Bundles

The course environment as Declarative Automation Bundles, deployable in pieces.

Each directory is an independent bundle with its own `databricks.yml`. Deploy only what the
labs you are running actually need, and destroy the expensive ones when you are done.

---

## Environment bundles

| Bundle | Deploys | Needed by | Notes |
|---|---|---|---|
| `00-foundation` | Catalog, 4 schemas, landing volume, all training data | **Everything** | Deploy first |
| `10-classic-compute` | Single-node classic cluster | Labs 4, 8, 9 | Spark UI is unavailable on serverless |
| `20-perf-data` | 2M-row skewed tables | Lab 8 only | The expensive one — destroy between deliveries |
| `30-landing-data` | Auto Loader source CSVs | Lab 10 | Without it the landing volume is empty and Bronze has nothing to read |
| `40-attendees` | Per-attendee schemas + grants | Labs 4–7 | Edit the schema list to match your roster |

## Solution bundles

Answer keys. Attendees build these themselves — deploy them for a working reference, a demo,
or to recover a wedged lab mid-class.

| Bundle | Deploys | Lab |
|---|---|---|
| `solutions/lab-10-pipeline` | Full medallion pipeline with AUTO CDC | 10 |
| `solutions/lab-11-job` | Job with task values and an If/else gate | 11 |
| `solutions/lab-12-reference` | The finished manifest Lab 12 builds toward | 12 |

---

## Deploying

```bash
cd bundles/00-foundation
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run build_environment
```

Then, as needed:

```bash
cd ../10-classic-compute && databricks bundle deploy -t dev   # Labs 4, 8, 9
cd ../20-perf-data      && databricks bundle deploy -t dev && databricks bundle run build_perf_tables
cd ../30-landing-data   && databricks bundle deploy -t dev && databricks bundle run stage_landing_files
cd ../40-attendees      && databricks bundle deploy -t dev && databricks bundle run grant_attendee_access
```

Tearing down after a delivery:

```bash
cd bundles/20-perf-data      && databricks bundle destroy -t dev   # the expensive one
cd ../10-classic-compute     && databricks bundle destroy -t dev
```

**Requires Databricks CLI v1.0 or later.** Bundle commands on the 0.x line shell out to
Terraform and fail with `error downloading Terraform: unable to verify checksums signature:
openpgp: key expired`. It reads like a network fault and is not one — it is an expired signing
key inside an old binary. Reproduced on v0.224.0, resolved on v1.14.1.

---

## Accounts with no metastore storage root

If `00-foundation` fails with **`Metastore storage root URL does not exist`**, the metastore has
no default managed location — common when Default Storage is enabled on the account. Supply one:

```bash
databricks bundle deploy -t dev \
  --var="managed_location=s3://<bucket>/<prefix>/training_nic"
```

Two things are worth knowing about this error. It is neither a syntax nor a permissions problem,
and `CREATE CATALOG **IF NOT EXISTS**` hits it **even when the catalog already exists**, because
storage is validated before existence is checked. The foundation notebook therefore checks
`SHOW CATALOGS` first rather than relying on `IF NOT EXISTS`; without that the job fails on every
re-run of an environment that is already perfectly healthy.

Lab 7 Task 1 hits the same wall, and carries the same note.

---

## Two ways to create a schema, both correct

`00-foundation` creates its schemas **inside a job**. `40-attendees` declares them as native
`resources.schemas` entries. That is not an inconsistency, and it is worth showing attendees.

A bundle cannot declare a Unity Catalog *catalog* — there is no `resources.catalogs`. On a clean
workspace the catalog does not exist yet, so declared schemas would have nothing to live in and
the deploy would fail on ordering. The foundation therefore does it imperatively.

By the time `40-attendees` runs the catalog exists, so the declarative form works — and it is the
better one: `bundle destroy` removes the schemas, and editing the list and redeploying reconciles
the difference rather than leaving orphans behind.

Prefer the declarative form whenever ordering allows it. Reach for a job task only when the
resource type is not declarable, as with catalogs, grants, groups, and service principals.

---

## What a bundle cannot do here

Verified against the CLI's own schema, not assumed:

| Declarable | Not declarable |
|---|---|
| `jobs`, `pipelines`, `clusters`, `schemas`, `volumes`, `dashboards`, `sql_warehouses`, `apps`, `secret_scopes` | Unity Catalog **catalogs**, **grants**, account **groups**, **service principals**, **Genie spaces** |

Everything in the right-hand column is done by a job task in these bundles. That split is the
honest boundary of what infrastructure-as-code covers on this platform today.

---

## Verified

Every bundle here was deployed and run against a live workspace.

- `00-foundation` rebuilds the environment and reproduces Lab 3's answer key exactly: 100 rows
  dropped, 217 empty cities at source against 212 nulls in the cloud copy, 5,000 padded rows,
  1,504 rows in Lab 2's 1970–1990 range, `VERSION AS OF 0` returning 4,900.
- `30-landing-data` stages 4 CSVs and Spark reads `#ID_RSSD` back intact, `#` and all.
- `solutions/lab-10-pipeline` runs all four flows to COMPLETED — Bronze via Auto Loader from
  those files, Silver, Gold, and the AUTO CDC snapshot flow producing `__START_AT`/`__END_AT`.
- `solutions/lab-11-job` runs with `promote_gold` SUCCESS and `halt_and_alert` SKIPPED/EXCLUDED.
