# Appendix — Databricks Product Naming: Current vs. Legacy

Shared appendix for **Cloud Analytics for Business Users** (Days 1–2) and **Advanced Data Engineering** (Days 3–4).

**Why this exists.** Databricks renamed a large part of the data-engineering surface at Data + AI Summit 2025. The instructor will frequently use the older names out of habit, and much of the internet — including Stack Overflow answers students will find mid-lab — still uses them. Rather than pretend the old names don't exist, we teach both and name the change explicitly.

**Teaching stance:** *lead with the current name, put the legacy name in parentheses on first use per module, then use the current name.* Students should leave able to read both.

---

## Verified renames

Checked against official Databricks documentation, 2026-08-30.

| Legacy name | Current name | Notes |
|---|---|---|
| **Delta Live Tables (DLT)** | **Lakeflow pipelines** / Spark Declarative Pipelines | Existing DLT code runs unchanged — no migration required |
| **Workflows** | **Lakeflow Jobs** | |
| **Databricks Asset Bundles (DABs)** | **Declarative Automation Bundles** | Docs read: *"formerly known as Databricks Asset Bundles."* Abbreviation **DAB** is unchanged, as is `databricks.yml` and the `validate` / `deploy` verbs |
| **Repos** | **Databricks Git folders** | |
| **Lakeview dashboards** | **AI/BI Dashboards** | The API still carries the Lakeview name |
| **Data Explorer** | **Catalog Explorer** | |

### API-level changes inside pipelines

| Legacy | Current |
|---|---|
| `import dlt` | `from pyspark import pipelines as dp` |
| `@dlt` | `@dp` |
| `@table` (for a materialized view) | `@materialized_view` |
| `@view` | `@temporary_view` |

> **Key Insight for students:** the rename is not cosmetic-only at the API layer, but it *is* backward compatible. Old code keeps running. Classic **SKUs still begin with `DLT`**, and the **pipeline event log schema still uses `dlt` naming** — so students will see the old term in billing and in monitoring queries even after the rename. Name that explicitly or it reads as an inconsistency.

---

## Change data capture — verified

The **AUTO CDC APIs replace the APPLY CHANGES APIs and have identical syntax.** APPLY CHANGES still works; Databricks recommends AUTO CDC going forward. Teach both names — the old one is all over existing pipeline code.

| Legacy | Current | Interface |
|---|---|---|
| `APPLY CHANGES INTO` | **`AUTO CDC INTO`** | SQL |
| `apply_changes()` | **`create_auto_cdc_flow()`** | Python |
| `apply_changes_from_snapshot()` | **`create_auto_cdc_from_snapshot_flow()`** | Python only |

**Two variants, and the distinction matters for this client:**

- **AUTO CDC** — requires a change data feed enabled on the source. SQL *and* Python.
- **AUTO CDC FROM SNAPSHOT** — compares snapshots to derive changes. **Python only.** This is the variant that fits a SQL Server migration where CDC was never enabled on the source, which is the likely real-world case here.

Both compute **SCD Type 1 and Type 2**. Bitemporal tracking is Beta.

> **Environment prerequisite — blocking.** The AUTO CDC APIs are **not supported by Apache Spark Declarative Pipelines**. The pipeline must run on **serverless Lakeflow pipelines, or the Pro or Advanced edition**. If the training workspace lands on Core, the CDC lab cannot run as designed. Confirm edition before building.

---

## Not yet verified — confirm before teaching as fact

These are commonly cited as renames but were **not** confirmed against docs in this pass. Do not assert them in the guides until checked.

- SQL Endpoints → **SQL Warehouses**
- Genie → **AI/BI Genie**
- Feature Store → **Unity Catalog feature engineering**
- Model Serving → **Mosaic AI Model Serving**

---

## Where stale names appear in our own materials

Scanned across the lab guide and both course outlines.

| Document | Stale term | Count | Replace with |
|---|---|---|---|
| **Advanced DE outline** | "Databricks Asset Bundles" / "Asset Bundles" | 4 | Declarative Automation Bundles |
| **Advanced DE outline** | "Repos" | 2 | Databricks Git folders |
| **Advanced DE outline** | "Workflows" | 1 | Lakeflow Jobs |
| **Business Users outline** | "Repo" | 2 | Git folder |
| **Lab guide (candidate)** | "Repos" | 2 | Git folders — inconsistent, it uses "Git folder" 4× elsewhere |

**Notable:** the Claude-written lab guide is *more* current than either outline. It already uses Catalog Explorer, Git folders, AI/BI Dashboards and Genie correctly. Its only naming fault is mixing "Repos" and "Git folders" within one document.

The Day 2 afternoon module of the DE course is titled **"CI/CD with Databricks Asset Bundles"** — that title needs changing, and it is the single most visible instance since it is a session heading.

---

## Terms that did NOT change

Worth stating so nobody "corrects" them: **Unity Catalog**, **Delta Lake**, **Auto Loader**, **Photon**, **Databricks Runtime**, **Delta Sharing**, **Lakehouse Federation**, **Medallion architecture** (Bronze/Silver/Gold), **OPTIMIZE**, **Liquid Clustering**, **Time travel**, **Volumes**.

---

## Suggested student-facing framing

> Databricks renamed much of the data-engineering surface in 2025. Delta Live Tables became Lakeflow pipelines, Workflows became Lakeflow Jobs, Repos became Git folders. Your existing code still runs — the rename did not break anything. But the older names are everywhere: in your billing SKUs, in the pipeline event log, in every blog post written before mid-2025, and in the habits of anyone who has used the platform for a while. You need to recognise both.
