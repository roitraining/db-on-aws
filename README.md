# Databricks on AWS — Training Labs

Lab instructions for two linked ROI Training courses. Both run against the same Unity Catalog
data, built by the setup notebooks in this repository.

| Course | Days | Labs | Audience |
|---|---|---|---|
| [Cloud Analytics for Business Users](courses/cloud-analytics-business-users/) | 1–2 | 1–6 | Business analysts, reporting specialists, data consumers migrating off on-premises SQL Server |
| [Advanced Data Engineering](courses/advanced-data-engineering/) | 3–4 | 7–12 | Engineers who own the platform the analysts use |

The advanced course assumes the intro course. Lab 8 explicitly builds on Lab 4, and Labs 7–12
all write into a catalog created in Lab 7.

---

## Start here

**[`SETUP.md`](SETUP.md)** — add the material to your workspace, get compute, and verify your
environment. Everything in it happens inside Databricks; no local tooling and no access token.

Taking the Advanced course without having taken the Intro course? `SETUP.md` **Part 4** is the
bridge. Read it before Lab 7.

---

## Using this repository in Databricks

The intended path is a **Databricks Git folder**, so attendees read each guide inside the
workspace next to the notebook they are running rather than alongside a separate document.

In **Workspace → Create → Git folder**, enter this repository's URL and authenticate with a
personal access token. Attendees do this themselves in Lab 7; for Labs 1–6 the instructor
usually adds it once and shares it.

---

## Instructor setup — run this first

Both setup notebooks are **instructor-only**. Do not distribute them: the intro notebook plants
the five deliberate defects that Lab 3 exists to find, and reading it first spoils the lab.

Run them in this order:

1. `courses/cloud-analytics-business-users/setup/00_environment_setup.py`
   Builds `training_nic` — the `raw`, `legacy_onprem`, `migrated` and `reference` schemas — and
   plants Lab 3's defects.
2. `courses/advanced-data-engineering/setup/00_environment_setup.py`
   Adds the `training_nic.perf` tables that Lab 8 measures against. Fails loudly if step 1 has
   not been run.

Each notebook verifies its own output and raises if anything is missing, so a green run is a
real signal that the environment is ready.

---

## Compute

| Labs | Compute | Why |
|---|---|---|
| 1, 2, 3, 5, 6, 7, 9 | Serverless SQL warehouse | Fast to start, nothing else needed |
| 4, 8, 9 | **Classic cluster** | The Spark UI is not available on serverless — this is a platform constraint, not a preference |
| 10, 11 | Serverless Lakeflow pipelines and Jobs | AUTO CDC needs serverless, Pro, or Advanced |
| 12 | Databricks CLI **v1.0 or later** | Older 0.x builds fail on an expired Terraform signing key |

A cold classic cluster takes about **six minutes** to start. Start it before the break preceding
Lab 4, or attendees spend that time watching a spinner.

---

## Repository layout

```
courses/
  cloud-analytics-business-users/
    labs/          Lab_1_Guide.md .. Lab_6_Guide.md
    setup/         00_environment_setup.py   (instructor only)
  advanced-data-engineering/
    labs/          Lab_7_Guide.md .. Lab_12_Guide.md
    setup/         00_environment_setup.py   (instructor only)
reference/
  Appendix_Product_Naming.md   Old and current Databricks product names, side by side
  LAB_TEST_RESULTS.md          What was verified live, and what is still open
```

---

## Product naming

Several things in these courses were renamed recently, and most existing material still uses the
old names. Each lab leads with the current name and flags the legacy one where an attendee will
realistically meet it. `reference/Appendix_Product_Naming.md` collects them in one place.

The ones that come up most:

| Current | Formerly |
|---|---|
| Declarative Automation Bundles (DAB) | Databricks Asset Bundles |
| Lakeflow Declarative Pipelines | Delta Live Tables (DLT) |
| `from pyspark import pipelines as dp` | `import dlt` |
| AUTO CDC APIs | APPLY CHANGES APIs |
| If/else task | Condition task |

---

## Verification

Every lab in this repository has been executed against a live Databricks workspace rather than
reviewed on paper. Measured values quoted in Expected Results — timings, row counts, file
counts, skew ratios — came from those runs.

`reference/LAB_TEST_RESULTS.md` records what was verified, the defects that found, and the items
still needing a human before delivery.
