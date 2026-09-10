# Environment Setup — Advanced Data Engineering

Run this **once** before class. It adds the two datasets the advanced labs need on top of the
shared `training_nic` foundation.

> **This course is Days 3–4.** If the class also took **Cloud Analytics** (Days 1–2), the
> `training_nic` foundation already exists and this notebook only adds the perf-scale tables Lab 8
> needs. Running Advanced standalone? Build the foundation first — see
> [`SETUP.md`](../../../SETUP.md) Parts 3 and 4.

## Run it

1. Add this repository to Databricks as a **Git folder**
   (Workspace → **Create → Git folder** → `https://github.com/roitraining/db-on-aws`).
2. Open **`00_environment_setup.py`** — the notebook in this folder.
3. Attach it to a **serverless** notebook cluster.
4. Click **Run All.**

You are done when the last cell prints **`All setup checks passed. Environment is ready for
Labs 7-12.`** The notebook fails loudly if the Cloud Analytics foundation is missing — run that
setup first (or the `00-foundation` bundle) and try again.

## Confirm

```sql
SELECT COUNT(*) FROM training_nic.migrated.institutions;     -- 4900   (foundation)
SELECT COUNT(*) FROM training_nic.perf.institutions_large;   -- 2000000 (Lab 8)
```

## Notes

- **Run once per workspace** — instructor or admin, not attendees.
- **Labs 8 and 9 need a classic cluster** — the Spark UI is not available on serverless. Create
  one: **Compute → Create compute**, single node, **`m5d.large`**, Databricks Runtime **16.4
  LTS**. (Plain `m5.large` is rejected with *"At least one EBS volume must be attached"* — use
  `m5d.large`.) A cold cluster takes ~6 minutes; start it before Lab 8.
- Prefer a repeatable, CLI-based deploy? See [`SETUP.md`](../../../SETUP.md) Part 3 and
  [`bundles/README.md`](../../../bundles/README.md).
