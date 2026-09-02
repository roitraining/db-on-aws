# Environment Setup

Everything here happens **inside Databricks**. You need no local tooling, no Python install, and
no personal access token.

Work through Part 1 and Part 2 before your first lab. Part 3 is for instructors.

> **Attending the Advanced course without the Intro course?** Do Parts 1 and 2, then read
> [Part 4](#part-4--advanced-course-catch-up) before Lab 7.

---

## Part 1 · Add the course material as a Git folder

The lab guides live in a GitHub repository. Adding it as a **Git folder** puts every guide inside
your workspace, next to the notebook you are running, so you are never alt-tabbing to a document.

1. In the sidebar, click **Workspace**.

2. Navigate to your own folder — **Workspace → Users → your.email@company.com**.

    Creating the folder here rather than in **Shared** means it is yours: you can pull updates
    without affecting anyone else, and nobody else's edits appear in yours.

3. Click **Create** (top right) and choose **Git folder**.

4. Fill in the dialog:

    | Field | Value |
    |---|---|
    | Git repository URL | `https://github.com/roitraining/db-on-aws` |
    | Git provider | **GitHub** |
    | Git folder name | `db-on-aws` (fills in automatically) |

5. Click **Create Git folder**.

    > **Note:** The repository is public, so **no credential is required**. If you are prompted
    > for one, or you see `No Git credential configured, but credential required for this
    > repository`, the repository is private to you — tell your instructor rather than creating a
    > token. Nothing in these courses needs you to authenticate to GitHub.

6. Confirm the clone worked. You should see:

    ```
    db-on-aws/
      courses/
        cloud-analytics-business-users/labs/     Lab_1_Guide.md .. Lab_6_Guide.md
        advanced-data-engineering/labs/          Lab_7_Guide.md .. Lab_12_Guide.md
      bundles/
      reference/
    ```

7. Open the guide for your first lab and leave it in a browser tab.

    Click any `.md` file to read it rendered in the workspace.

> **Pulling updates mid-course:** if your instructor pushes a fix, click the Git folder name, then
> **Pull**. Your own notebooks are unaffected — they live outside the Git folder.

---

## Part 2 · Get compute

Databricks separates *what you run* from *what it runs on*. Nearly every lab uses a **serverless
SQL warehouse**, which starts in seconds. Three labs need a **classic cluster** instead.

### For SQL work — Labs 1, 2, 3, 5, 6, 7, 9

1. Click **SQL Editor** in the sidebar.
2. In the warehouse selector at the top right, choose the serverless warehouse your instructor
   named — typically `Serverless Starter Warehouse`.
3. Run a one-line check:

    ```sql
    SELECT current_catalog(), current_user();
    ```

    > **Expected Result:** One row, returning a catalog name and your email address. If this runs,
    > your SQL environment is ready.

### For notebook work — Labs 4, 8, 9, 10, 11

1. Click **Workspace**, open the notebook the lab names.
2. Use the compute selector at the top right of the notebook.

| Labs | Attach to | Why |
|---|---|---|
| 10, 11 | **Serverless** | Fast, and pipelines require it for AUTO CDC |
| **4, 8, 9** | **A classic cluster** | The Spark UI is **not available** on serverless, and these labs read it directly |

> **Common Pitfall:** A cold classic cluster takes about **six minutes** to start. Start it at the
> break *before* Lab 4 rather than at the start of it. If your compute dropdown shows no classic
> cluster at all, ask your instructor — attendees usually cannot create one.

### Confirm your data is there

```sql
SELECT COUNT(*) FROM training_nic.migrated.institutions;
```

> **Expected Result:** `4900`. If the table is missing, the environment has not been built —
> see Part 3, or tell your instructor.

---

## Part 3 · Build the environment (instructors)

Two paths. Both produce the same environment; pick on whether you want local tooling.

### Path A · Entirely inside Databricks — no CLI

Best when you are setting up from a browser, or on a locked-down machine.

1. Add the repository as a Git folder, following Part 1.
2. Open `bundles/00-foundation/src/build_environment.py`.
3. Attach it to serverless compute.
4. **Run All.**

    The notebook's widgets default to catalog `training_nic`, so it needs no arguments. It creates
    the catalog, schemas and landing volume, generates the training data, plants the five defects
    Lab 3 exists to find, then verifies every figure the labs quote. It raises on any mismatch —
    a green run is a real signal.

5. Then, only as the labs require:

    | Notebook | Gives you | Needed for |
    |---|---|---|
    | `bundles/30-landing-data/src/stage_landing_files.py` | Auto Loader source CSVs | Lab 10 |
    | `bundles/20-perf-data/src/build_perf_tables.py` | 2M-row skewed tables | Lab 8 |
    | `bundles/40-attendees/src/grant_attendee_access.py` | Attendee grants | Labs 4–7 |

6. Create the classic cluster for Labs 4, 8 and 9 by hand — **Compute → Create compute**,
   single node, `m5d.large`, Databricks Runtime 16.4 LTS.

    > **Common Pitfall:** Plain `m5.large` is rejected with *"At least one EBS volume must be
    > attached"*. Use `m5d.large`, which carries instance storage.

### Path B · Declarative Automation Bundles — repeatable

Best when you deliver this more than once. Requires **Databricks CLI v1.0 or later**.

```bash
cd bundles/00-foundation
databricks bundle deploy -t dev
databricks bundle run build_environment
```

Then deploy only what you need — `10-classic-compute`, `20-perf-data`, `30-landing-data`,
`40-attendees`. Full detail in [`bundles/README.md`](bundles/README.md), including how to tear the
expensive pieces down afterwards.

> **Common Pitfall:** On a CLI from the 0.x line every bundle command fails with `error
> downloading Terraform: unable to verify checksums signature: openpgp: key expired`. That is an
> expired signing key inside an old binary, not a network problem. Check `databricks --version`
> first.

### If catalog creation fails

```
Metastore storage root URL does not exist. Default Storage is enabled in your account.
```

The metastore has no default managed location. Supply one:

```sql
CREATE CATALOG training_nic MANAGED LOCATION 's3://<bucket>/<prefix>/training_nic';
```

or, on Path B, `databricks bundle deploy -t dev --var="managed_location=s3://..."`.

Worth settling before class: Lab 7 Task 1 has every attendee create a catalog and will hit the
same wall at the same moment.

---

## Part 4 · Advanced course catch-up

**Read this only if you are taking the Advanced course (Labs 7–12) without having taken Cloud
Analytics for Business Users (Labs 1–6).**

Labs 7–12 assume the intro course and deliberately do not re-teach it. You do not need to work
through Labs 1–6, but four things from them are assumed knowledge, and Lab 7 starts immediately.

### Do this before Lab 7

1. Complete **Part 1** and **Part 2** above.

2. Confirm you can reach the training data:

    ```sql
    SELECT `#ID_RSSD`, NM_LGL, STATE_ABBR_NM
    FROM training_nic.migrated.institutions
    LIMIT 5;
    ```

    > **Expected Result:** Five rows. If you get `PARSE_SYNTAX_ERROR`, you dropped the backticks —
    > see point 2 below, which is the single most common error in these courses.

### Four things Labs 7–12 assume

**1 · The three-level namespace.** Every object is `catalog.schema.object`. `USE CATALOG` and
`USE SCHEMA` set a default so you can use short names, but they last only for your session.

**2 · The key column carries a leading `#`.** NIC publishes it as `#ID_RSSD`, and the courses
preserve native names through raw and Bronze deliberately, cleaning at Silver. In SQL it **must**
be backtick-quoted:

```sql
SELECT `#ID_RSSD` FROM training_nic.migrated.institutions;   -- correct
SELECT   #ID_RSSD FROM training_nic.migrated.institutions;   -- PARSE_SYNTAX_ERROR
```

In PySpark it is an ordinary string: `df.select("#ID_RSSD")`.

**3 · Reading a table needs three privileges, not one.** `SELECT` on the object, `USE CATALOG` on
the catalog, `USE SCHEMA` on the schema. Granting `SELECT` alone leaves a colleague unable to read
and looks exactly like a bug. Lab 7 Task 2 has you reproduce this deliberately — expect it.

**4 · The dataset is a migration with known defects.** `legacy_onprem` stands for the
on-premises SQL Server source; `migrated` is the cloud copy and carries five deliberate defects
(dropped rows, truncated decimals, shifted dates, empty-string-to-NULL, stripped padding). Labs 9
and 11 build validation machinery on top of that. You do not need the details — only that the two
schemas are *supposed* to disagree.

### One lab worth skimming

**Lab 8 builds directly on Intro Lab 4** and says so: "This lab starts where Intro Lab 4 stopped."
Lab 4 covers `spark.table`, lazy evaluation, `filter`, `withColumn`, `select`, `groupBy`/`agg` and
`saveAsTable`. If you are not fluent in the DataFrame API, skim
`courses/cloud-analytics-business-users/labs/Lab_4_Guide.md` — about twenty minutes — before Lab 8.
Everything else in Labs 7–12 is self-contained.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `No Git credential configured` | The repository is private to you | Tell your instructor. Do not create a token — the public repo needs none |
| Git folder clones but is empty | Wrong branch | The default branch is `main` |
| `PARSE_SYNTAX_ERROR` near `#` | Unquoted native column name | Backtick it: `` `#ID_RSSD` `` |
| `TABLE_OR_VIEW_NOT_FOUND` on a short name | `USE CATALOG` was a separate session | Qualify fully, or re-run `USE` in the same session |
| `Table training_nic...` not found | Environment not built | Part 3, or ask your instructor |
| No Spark UI link on your compute | You are on serverless | Labs 4, 8, 9 need a classic cluster |
| Classic cluster missing from the dropdown | Not created, or you lack permission | Ask your instructor |
| Cluster takes minutes to start | Normal — about six for a cold start | Start it before the preceding break |
| `Metastore storage root URL does not exist` | No default managed location | Part 3, "If catalog creation fails" |

---

## Where to go next

| You are taking | Start at |
|---|---|
| Cloud Analytics for Business Users | `courses/cloud-analytics-business-users/labs/Lab_1_Guide.md` |
| Advanced Data Engineering | Part 4 above, then `courses/advanced-data-engineering/labs/Lab_7_Guide.md` |
