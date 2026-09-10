# Environment Setup — Cloud Analytics for Business Users

Run this **once** before class. It builds the `training_nic` catalog that every lab queries —
without it, Lab 1 opens Catalog Explorer to nothing.

## Run it

1. Add this repository to Databricks as a **Git folder**
   (Workspace → **Create → Git folder** → `https://github.com/roitraining/db-on-aws`).
2. Open **`00_environment_setup.py`** — the notebook in this folder.
3. Attach it to compute. A **serverless** notebook cluster is fine.
4. In **Part 0 · Configuration**, set the `ATTENDEES` list — one short, stable id per attendee
   (`a01`, `a02`, …).
5. Click **Run All.**

You are done when the last cell prints **`All setup checks passed. Environment is ready for
Labs 1-6.`** The notebook raises on any mismatch, so a green run is a real signal.

## Confirm

In the SQL Editor:

```sql
SELECT COUNT(*) FROM training_nic.migrated.institutions;   -- expect 4900
```

## Notes

- **Run once per workspace.** It builds shared data — the instructor or a workspace admin runs
  it, not attendees.
- **Instructor only.** The notebook plants the five defects Lab 3 exists to find; do not walk
  attendees through Part 5 before Lab 3 is complete.
- **If catalog creation fails** with `Metastore storage root URL does not exist … Default
  Storage`, the metastore has no default managed location. Create the catalog explicitly, then
  Run All again:
  ```sql
  CREATE CATALOG training_nic MANAGED LOCATION 's3://<bucket>/<prefix>/training_nic';
  ```
- Prefer a repeatable, CLI-based deploy? See [`SETUP.md`](../../../SETUP.md) Part 3 and
  [`bundles/README.md`](../../../bundles/README.md).
