# Spark UI Showcase — Instructor Demo Script

Companion to `spark_ui_showcase.py`. Numbers drift a little run to run; the shapes don't.

**Prep tip:** restart the cluster before class — a fresh cluster means an empty Spark UI, so the only stages in the list are the ones you just made.

## Setup (once, before the demo)

1. **Compute** (left sidebar) → **`[prod] db-on-aws - lab cluster`** → start it (~6 min cold).
2. Open the notebook (`/Shared/db-on-aws/spark_ui_showcase` in the class workspace) and attach it to that cluster (compute selector, top right).
3. Open the Spark UI in a **second browser tab**: Compute → click the cluster → **Spark UI** tab across the top. You'll live in its **Stages** sub-tab; newest stages appear at the top of Completed Stages. Refresh after each cell.
4. Run the first code cell (setup) — it confirms the 30M-row table exists.

## 1 · Partition sizes

1. Run the **128 MB scan** cell, then the **16 MB scan** cell.
2. Spark UI → **Stages**: the two scan stages are at the top. Point at the **Tasks** column: **~11 tasks** vs **~79 tasks** — same data, same query.
3. Click the second stage's description link → scroll to **Summary Metrics** → the **Input Size / Records** row: each task now carries ~1/8 the data.
4. Line: "task count is your parallelism — 3 tasks on an 8-core cluster means 5 cores doing nothing."

## 2 · Shuffle

1. Run the shuffle cell (`COUNT(DISTINCT …)`).
2. **Stages**: find the pair — one stage with **Shuffle Write ≈ 2.2 GiB** (11 tasks, the map side), one with **Shuffle Read ≈ 2.2 GiB** (~40 tasks, the reduce side). Both columns are visible right in the stage list.
3. Point: the numbers **match** — that's 2.2 GB physically crossing the network between stages. Same Exchange the room saw in the query profile, now with per-task detail.

## 3 · Skew

1. Run the skew cell (~1 min).
2. **Stages** → the newest stage with Shuffle Read ≈ 0.5 GiB → click into it.
3. **Summary Metrics** — the money table. **Duration** row: Median **~40 ms**, Max **~12 s**. **Shuffle Read Size** row: Median ~0, Max **~290 MB**.
4. Scroll to the **Tasks** table, sort by **Duration** descending → there's the one straggler holding 60% of the data.
5. Line: "the job finishes when the biggest partition finishes — everyone else is done and waiting."

## 4 · Spill

1. Run the spill cell (~2 min — the full 30M-row sort into 8 partitions).
2. **Stages** → click into the 8-task sort stage (Shuffle Read ≈ 2.35 GiB).
3. **Summary Metrics** has grown **two rows that weren't there before**: **Spill (Memory)** and **Spill (Disk)** — ~0.9 GB / ~275 MB per task (≈7 GB / 2 GB total). They only appear when spill actually happened, which is a nice reveal.
4. Line: "the job *succeeded* — spill isn't failure, it's the smell. The fix is more partitions or more memory."

## 5 · OOM — run last, it fails on purpose

1. Warn the room, run the cell, and narrate over the ~10-minute death spiral.
2. While it runs: **Stages** → the active stage sits at 0/1 tasks. Switch to the **Executors** tab — watch an executor's status flip to **Dead**, then a replacement appear. Refresh periodically.
3. When it fails: **Stages → Failed Stages** → click it → read the **Failure Reason**: `ExecutorLostFailure … heartbeat timed out`. In the **Tasks** table, point at the **Attempt** column: 0, 1, 2, 3 — Spark retried on fresh executors before giving up.
4. Lines: "a real memory death rarely says OutOfMemoryError politely — it looks like a lost executor," and "notice nobody else's job died; the executor got replaced."
