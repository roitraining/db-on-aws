# Serverless Query Profile vs Classic Spark UI — What Actually Needs Classic?

**Why this exists:** the course repeatedly says "the Spark UI is not available on serverless"
(true, and still the [documented limitation](https://docs.databricks.com/aws/en/compute/serverless/limitations))
— but students hear "no execution visibility on serverless," and that stronger claim is **false**.
A student demonstrated the serverless performance view in class (2026-09-29) and it covers most of
what the course had implied needs a classic cluster. This is the verified capability matrix.

**Where to find it on serverless:** notebooks → **See performance** link under any Spark cell →
click a statement → query details / **query profile** (graph view). SQL editor → **Query History**
→ query → **See query profile**. Jobs on serverless → run page → query insights.

**Empirical basis:** the demo skewed aggregation (2M rows, 60/40 skew,
`training_nic.eng_demo.skewed_transactions`) run on the trial workspace's serverless warehouse,
metrics pulled from the query history API (`include_metrics=true`), 2026-09-29.

## The matrix

| Diagnostic | Classic Spark UI | Serverless query profile | Verified evidence (serverless) |
|---|---|---|---|
| Execution DAG / plan | Jobs → DAG viz | **YES** — graph view is an operator DAG; `explain()` works everywhere | graph view; plan nodes incl. scans, aggregates |
| Stage/shuffle boundaries | Stages tab | **YES** — Exchange operators in the graph are the shuffle boundaries | Exchange nodes visible in profile |
| Shuffle volume | Shuffle Read/Write per stage | **YES** — network bytes on the Exchange / query | `network_sent_bytes: 3,010,829` on the demo query |
| Task count / partition count (parallelism) | task count per stage | **YES** — task/partition counts reported | `task_progress: {completed_task_count: 16, completed_partition_count: 16}` |
| Total task time vs wall clock | Summary | **YES** | `task_total_time_ms: 52,984` vs `total_time_ms: 15,648` (~3.4× parallelism) |
| Rows/bytes read, files, pruning | SQL tab / scans | **YES** — arguably richer than the Spark UI | `rows_read_count: 2,000,000`, `read_files_count: 8`, pruning counters |
| Spill | Spill (Memory)/(Disk) columns per stage | **YES (aggregate)** — per-query spill metric, not per-task columns | `spill_to_disk_bytes: 0` present in metrics |
| **Skew — per-task distribution (max vs median)** | Summary Metrics max/median per stage | **NO (aggregate only)** — no per-task table; you see totals, not the straggler | only aggregate task metrics in profile |
| **Straggler identification** | longest task in Stages tab | **NO** | — |
| **Executor-level forensics (OOM, ExecutorLostFailure, retries)** | Executors tab, task retry log | **NO** | — |
| Live monitoring of a long-running stage | Active stages | **Partial** — progress bar, no per-task live view | — |

## The corrected teaching line

> "Serverless gives you the **query profile**: the DAG, the plan, shuffle volume, task counts,
> rows/bytes, and spill — that answers *'does my query perform well and why?'* What still needs
> the **classic Spark UI** is the *per-task* story: skew read from the max-vs-median task
> distribution, straggler hunting, and executor-level failure forensics. That per-task
> distribution is exactly what Lab 8 / the straggler demo diagnose — which is why those run on a
> classic cluster."

Say "the Spark UI is not available on serverless" only with the next sentence attached, or
students will (correctly) show you the profile and ask why you said they couldn't do this.

## Sources

- Serverless limitations (still lists Spark UI as unavailable):
  https://docs.databricks.com/aws/en/compute/serverless/limitations
- Query profile: https://docs.databricks.com/aws/en/sql/user/queries/query-profile
- Verified in-workspace 2026-09-29 (query history API metrics, trial workspace).
