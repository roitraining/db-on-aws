# Lab Test Results — Live Workspace Verification

**Both courses.** Every lab executed against a real Databricks workspace, not reviewed on paper.

- **Workspace:** `https://dbc-bba24453-9619.cloud.databricks.com`
- **SQL warehouse:** `5121b5e74b38d7a0` — 2X-Small serverless, Photon, 5-minute auto-stop
- **Classic cluster:** `lab-testing-single` — single-node `m5d.large`, DBR 16.4 LTS, 10-minute auto-terminate
- **Status:** Labs 1–12 all executed. **7 defects found and fixed.** 0 outstanding failures.

---

## Result by lab

| Lab | Course | How it was run | Result |
|---|---|---|---|
| 1 | Intro | SQL warehouse | Pass |
| 2 | Intro | SQL warehouse | Pass **after fix** — padding lesson was broken |
| 3 | Intro | SQL warehouse | Pass — all five defect magnitudes confirmed |
| 4 | Intro | Classic cluster, PySpark | Pass |
| 5 | Intro | SQL warehouse | Pass — including materialized view create/query/refresh |
| 6 | Intro | SQL + API capability check | Pass — dashboards and Genie both reachable |
| 7 | Advanced | SQL warehouse + Catalog Explorer | Pass **after fix** — `LIST` step misdiagnosed |
| 8 | Advanced | Classic cluster, PySpark | Pass **after substantial rewrite** — see below |
| 9 | Advanced | SQL warehouse | Pass — file counts pinned to measured values |
| 10 | Advanced | Real serverless Lakeflow pipeline | Pass — every modern API name verified |
| 11 | Advanced | Real Lakeflow Job | Pass — task values and branch routing verified |
| 12 | Advanced | Databricks CLI, real deploy | Pass **after fix** — CLI version blocker found |

---

## Defects found and fixed

### 1. Lab 8 taught a lesson the environment could not deliver — *the significant one*

Lab 8 asked attendees to find a straggler in Summary Metrics, comparing max task duration
against the median. Measured on the 4,900-row tables, the join produced **exactly one
partition holding all 4,900 rows**. Max equals median. There is no straggler, no distribution
to read, and nothing in Summary Metrics to see. The lab's central diagnostic moment did not
happen at all.

Two further problems sat underneath it:

- **AQE is on by default**, and `spark.sql.adaptive.skewJoin.enabled` with it. Even at a
  volume where skew *would* appear, the platform removes it before an attendee can observe it.
  The lab as written taught a pre-AQE mental model of a platform that has had AQE on by default
  for years.
- **Stretch task 2 was simply wrong.** It asked attendees to broadcast the small table and
  observe the change in stage count. `state_population` is seven rows, far below
  `spark.sql.autoBroadcastJoinThreshold`, so Spark **already** broadcasts it. Measured: both
  plans show `BroadcastHashJoin` and one exchange, before and after. Nothing changes. An
  attendee following the instruction would conclude they had made a mistake.

**Fixed** by rebuilding Parts 2–3 against measured numbers:

| Measurement | Value |
|---|---|
| AQE off | **17.4s** |
| AQE on | **6.2s** (≈3× faster, no code change) |
| Partition ratio, AQE off | max 1,200,000 / median 133,333 = **9.0×** |
| Skewed key | `CA` at **60%** of rows |
| Cache effect | 3.3s → **0.5s** |

Part 1 still translates the stored procedure against the real migrated tables, because that
is a correctness exercise. Part 2 opens by explaining *why* it then switches to
two-million-row tables — you cannot make a performance claim on 4,900 rows. That switch is now
a taught point rather than a hidden inconsistency. Attendees disable AQE to see the problem,
then re-enable it to see the platform solve it, and the lab states the three cases where AQE
still will not save them.

Stretch task 2 now tells attendees to expect no difference, and explains why that is the
lesson: read the plan before optimising it.

**New file:** `Databricks_Advanced_Data_Engineering/setup/00_environment_setup.py` builds the
`training_nic.perf` tables and self-verifies the skew ratio, failing loudly if the numbers
Lab 8 quotes ever stop being true. Ran clean end to end.

### 2. Lab 7 sent attendees to debug IAM for a non-problem

Step 13 runs `LIST` on the new external-location path and the guide said any failure there is
"an IAM trust or bucket policy problem." Measured: at that point in the lab the prefix is
empty, so `LIST` returns **`No such file or directory`** — which is not a permissions failure
at all. It is proof the credential *worked*: Databricks reached S3 and found nothing.
Attendees would have been sent to debug IAM for a healthy system.

**Fixed.** Step 13 now distinguishes a missing path from an access-denied error, and the
troubleshooting table carries both rows separately.

Also corrected: the overlap row now quotes the real error class,
`INVALID_PARAMETER_VALUE.LOCATION_OVERLAP`, reproduced deliberately.

**Verified as written:** managed table `Type = MANAGED` under `__unitystorage`; external table
`Type = EXTERNAL` at the S3 path; dropping the external table left the parquet files intact;
recreating over the same path without `AS SELECT` returned all 4,900 rows. Stretch task 1
behaves exactly as the guide claims.

### 3. Lab 12 fails outright on the installed CLI

`databricks bundle validate` failed on the machine's CLI v0.224.0 with:

```
error downloading Terraform: unable to verify checksums signature: openpgp: key expired
```

It reads like a network or proxy fault. It is neither — 0.x bundle commands shell out to
Terraform and the signing key baked into the old binary has expired. No amount of debugging
the manifest fixes it. Confirmed resolved on **v1.14.1**, where the same bundle reported
`Validation OK!` and deployed successfully.

**Fixed.** Prerequisites now require CLI v1.0+ with a `--version` check and a pitfall note
naming the exact error.

Two further findings from running `bundle generate job` for real, both now taught:

- It writes to `resources/*.yml` and warns the file **is not part of the bundle** until an
  `include:` pattern picks it up.
- The generated YAML **hard-codes** `pipeline_id: 013ed010-…` rather than emitting the
  `${resources.pipelines.medallion_pipeline.id}` reference. This is precisely the anti-pattern
  step 9 warns against, produced by the tool itself — `generate` reports what the workspace
  holds, and the workspace holds a resolved ID. It deploys perfectly to dev and quietly points
  prod at the dev pipeline. Now the lab's sharpest teaching moment.

Also verified: `deployment: kind: BUNDLE` and `edit_mode: UI_LOCKED` appear on deployed
resources, which is what step 16 asks attendees to confirm.

### 4. Lab 2's padding lesson was broken

The step queried `migrated`, which is already trimmed, so `LENGTH` equalled `TRIMMED` and the
lesson showed nothing. Now compares `legacy_onprem` (60) against the trimmed value (30). Steps
renumbered 1–20.

### 5. Systemic — 26 bare `ID_RSSD` references

Across Labs 2, 3, 8 and 10, the key was written without its native `#` or backticks. Every one
would have failed with `PARSE_SYNTAX_ERROR` on the first run. Fixed throughout — SQL to
`` `#ID_RSSD` ``, PySpark to `"#ID_RSSD"`. Lab 10's Silver-layer references were correctly left
alone, since that is the layer where the rename happens.

### 6. Generated data did not support the labs that queried it

- **Date span too narrow.** Data spanned 1950–1963, so Lab 2's 1970–1990 parameters returned
  zero rows. Changed to `id * 5`, giving 1950-01-06 → 2018-06-14 and 1,504 rows in range.
- **Charter distribution wrong.** `CHTR_TYPE_CD = '250'` was 22% of rows; the dropped-rows
  defect is meant to be 2%. Corrected to exactly 100 rows.
- **Only one table version**, so Lab 3's time travel had nothing to travel to. Added a no-op
  insert. Now 4 versions, `VERSION AS OF 0` returns 4,900.

### 7. Lab 9 quoted no numbers

Expected Results said "a `numFiles` value" and "a visibly lower file count." Now pinned to
measured values: **1 → 3 → 1**, size **39 KB → 19 KB**, `OPTIMIZE` logged as version 3 with
history intact and `VERSION AS OF 0` still returning 5,000. A pitfall note warns attendees not
to read the size drop as data loss.

---

## Backlog items closed

| Item | Was | Now |
|---|---|---|
| **V1** | Lakeflow edition unknown — blocked Lab 10's AUTO CDC | **Resolved.** Serverless pipeline COMPLETED in ~45s. `create_auto_cdc_from_snapshot_flow` with `stored_as_scd_type=2` produced `__START_AT`/`__END_AT` over the `#ID_RSSD` key. All four tables populated. |
| **V7** | `OPTIMIZE` file counts unverified | **Resolved.** 1 → 3 → 1, measured. |
| **V8** | Task values, condition task, event log | **Resolved.** Job ran: task value emitted from a Python notebook, `GREATER_THAN` condition on `{{tasks.read_event_log.values.dropped_records}}` routed to the true branch, false branch `SKIPPED / EXCLUDED`. |
| **V9** | `bundle generate` / `bind` unverified | **Resolved.** Real deploy and generate, with two new findings folded into the lab. |
| **V12** | Dashboard publishing unverified | **Resolved.** Lakeview API reachable (5 dashboards), Genie spaces API returns 200, dataset query returns 276 rows. |
| **V13** | Skew for Lab 8 | **Resolved and acted on** — see defect 1. |
| **V14** | Defect magnitudes for Lab 3 | **Resolved.** 100 dropped / $2,443.00 / 705 shifted dates / 212 null vs 217 empty / 5,000 padded. |

### Still open

- **V10 · Spark UI panel labels** — the cluster runs and the Spark UI is reachable, but the
  panel *labels* need eyes on a screen. Worth ten minutes before delivery.
- **V11 · Alert editor UI** — Lab 5 steps 14–23 are a UI flow. SQL underneath is verified;
  the click path is not.
- **V15 · Lab timings** — needs a human working through at attendee pace. My execution times
  are a floor, not an estimate.
- **Lab 7 Part 4 on attendee infrastructure** — verified against the existing
  `learn2training` credential. Attendees creating their *own* credential still need a real IAM
  role and bucket from the instructor.

---

## Things worth knowing before delivery

**A cold classic cluster takes about 6 minutes to start.** Measured 5.5. The serverless
warehouse answers in about 5 seconds. Labs 4, 8 and 9 need the classic cluster because the
Spark UI is not available on serverless — that is a platform constraint, not a preference.
Start the cluster before the break preceding Lab 4, or attendees spend six minutes watching a
spinner.

**Jobs now default to serverless compute.** The Lab 11 test job ran with no cluster
specification at all and completed in about 30 seconds.

**`event_log()` is owner-only**, and that collides with Run As by design. Lab 11 already
teaches this as its best moment; the constraint is confirmed real.

**Cost could not be measured directly.** `system.billing.usage` lags several hours, so I have
not attributed spend per lab rather than estimate it. What is known: the cluster ran roughly 50
minutes across all testing on a single `m5d.large`, the warehouse is 2X-Small with 5-minute
auto-stop, and both pipeline tests plus the job test ran on serverless for under two minutes
total. The cluster has been terminated. This should be queried retrospectively.

---

## Validator status

All 12 labs pass the LabForge gates after the edits:

- **Linter:** 0 errors, 0 warnings on every edited lab
- **Grounding:** Advanced — PASS 64, WARN 5, **FAIL 0**; Intro — PASS 55, WARN 2, **FAIL 0**

All 7 warnings fall in the two accepted categories already documented in
`known_false_positives.txt`: stretch tasks and knowledge-check questions are prompts rather
than claims, and table cells and resource URLs cannot carry an adjacent citation.

---

## Environment left behind

Kept — Labs 7–12 build on these:

```
training_nic.perf.institutions_large     2,000,000 rows  (60% CA skew, ratio 9.0x)
training_nic.perf.financials_large       2,000,000 rows
training_nic.analyst_test.*                             (Lab 4/5 outputs)
eng_test.work.*                                         (Lab 7/9 outputs)
```

Removed: both CDC test schemas, both test pipelines, the test job and its notebooks, the
deployed bundle and its resources, and a stray probe table. The classic cluster is terminated.
