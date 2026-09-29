# Instructor Demo: Reading the Spark UI

**Origin:** This was Part 5 of Lab 4 in the Cloud Analytics course. Removed from the lab
2026-09-28 — the Spark UI is classic-compute-only and belongs to the Advanced Data Engineering
course, so in the DA class it is an instructor demo, not a lab exercise.

**Demo notebook:** [`spark_ui_showcase.py`](spark_ui_showcase.py) (run on a **classic cluster** —
start it before the session; cold start is ~6 minutes).
**Presenter script:** [`spark_ui_showcase_demo_script.md`](spark_ui_showcase_demo_script.md).

---

## The Five Artifacts (walk one per showcase section)

The Spark UI belongs to classic compute, and Free Edition accounts cannot create classic
clusters — so this is a demonstration. Run the showcase notebook on a classic cluster and walk
the Spark UI on screen; the audience's job is to recognise each artifact so they know it when
they meet a real cluster.

1. **Partition sizes** — the same scan run twice with different input-split sizes. Watch the
   **task count** change (11 tasks vs 79) and, under Summary Metrics, the per-task input size.
   Task count is parallelism: too few tasks and cores sit idle, too many and scheduling overhead
   beats the work.

2. **Shuffle** — a `COUNT(DISTINCT ...)` per key that Spark cannot pre-combine, so ~2.4 GB
   physically crosses the network. Watch the map stage's **Shuffle Write** and the matching
   **Shuffle Read** on the reduce stage. This is the same Exchange found in the serverless query
   profile (Lab 4 Part 4), now with per-task detail. (A plain filter, by contrast, is narrow —
   each partition is processed independently, no shuffle columns at all.)

3. **Skew** — 60% of the rows share one key. In the stage's **Summary Metrics**, compare the
   **Max** column against the **Median**: one straggler task reads 60% of the data and runs
   hundreds of times longer than the median. The job is as slow as its biggest partition.

4. **Spill** — a full sort squeezed into 8 partitions. Each task gets more data than its share
   of execution memory, and two new columns appear on the stage: **Spill (Memory)** and
   **Spill (Disk)**. Spill is not failure — the job succeeds. It is the performance smell that
   says "this stage needed more memory or more partitions."

5. **Out of memory** — an aggregate whose *single value* cannot fit in memory has nowhere to
   spill. Watch the executor die (`ExecutorLostFailure` — a real memory death rarely says
   "OutOfMemoryError" politely), Spark retry the task four times on fresh executors, and the job
   fail. Other users' jobs on the cluster survive; the executor is replaced.

---

## Key Insight (say this at the end)

The serverless query profile answers "does my query perform well?" — run it, open the profile,
find the Exchange and the bytes read. The Spark UI adds what the profile cannot show — task
counts, partition sizes, stragglers, spill. When your organisation's workspace has classic
clusters, everything in this demo is available on any job you run there.

## Former stretch prompt (usable as an audience exercise)

For each of the five sections, write one sentence predicting what the Stages tab will show
*before* the section runs — then check yourself against what was demonstrated.
