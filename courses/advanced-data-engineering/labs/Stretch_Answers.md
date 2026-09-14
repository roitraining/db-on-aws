# Stretch Task Answers — Instructor Reference

**Course:** Databricks on AWS: Advanced Data Engineering

Answers to the stretch tasks in Labs 7–12. For instructor use — attendees get the reasoning, not this file.

---

## Lab 7

1. `DROP TABLE` on an external table removes the catalog entry only — the S3 files and the Delta log survive. Recreate without re-reading the source:

    ```sql
    CREATE TABLE <name> USING DELTA LOCATION 's3://<bucket>/<path>';
    ```
    The Delta log at that path *is* the table; the catalog entry is just a pointer.
2. With `READ FILES` but no `SELECT`, the peer can read the raw files by path (`spark.read.format("delta").load(...)`, volume browsing) but cannot query the table by name. Lesson: path-based permissions and catalog permissions are two independent systems — locking down one does not lock down the other.
3. ```sql
    SHOW GRANTS ON CATALOG <catalog>;
    SHOW GRANTS ON SCHEMA <catalog>.<schema>;
    SHOW GRANTS ON TABLE <catalog>.<schema>.<table>;
    ```
    An over-permissioned principal is one whose grant sits at a higher scope than their need — e.g., `SELECT` on the whole catalog when they use one table. Spot it by comparing the scope of the grant against the scope of actual use.

## Lab 8

1. On this data the **lower** value wins: fewer tasks means less scheduling overhead, and each task still has ample work. The higher value hurts because thousands of near-empty tasks cost more to schedule than to run. Neither number is "right" — the data volume decides, which is why AQE exists.
2. Answered in the guide: both plans already show `BroadcastHashJoin` — the seven-row table is under `spark.sql.autoBroadcastJoinThreshold`. Read the plan before optimizing.
3. Look for: explicit column selection instead of `*`, filters pushed before the join, no redundant hints, shuffle partitions left to AQE, and a comment only where the code cannot say it. The justification exercise matters more than the exact answer — every deviation should have one.

## Lab 9

1. The dry run lists data files older than the retention window that no current version references. Irreversible because time travel works by reading exactly those files — `OPTIMIZE` only writes new files and keeps old versions reachable; `VACUUM` deletes the history itself.
2. Pattern — parameterize the two versions and union the deltas:

    ```sql
    SELECT 'row_count' AS check,
           (SELECT COUNT(*) FROM t VERSION AS OF :v1) -
           (SELECT COUNT(*) FROM t VERSION AS OF :v2) AS delta
    UNION ALL
    -- key delta via EXCEPT counts, aggregate delta via SUM comparison
    ```
3. `CREATE TABLE ... CLUSTER BY (<key>)`, load, then compare `DESCRIBE DETAIL` (file count/size) and file-level min/max stats. The clustered copy co-locates rows sharing the key, so selective queries touch fewer files; the unclustered original spreads them everywhere.

## Lab 10

1. The stream starts from scratch and reprocesses everything — and whether that duplicates data depends entirely on the sink. The exactly-once guarantee lives in the **checkpoint plus the transactional sink together**, not in the source or the engine alone. Delete the checkpoint and you deleted the stream's memory.
2. With `FAIL UPDATE`, one bad row stops the whole pipeline update. Right answer depends on the data contract: fail is defensible for regulated financial data (better no data than wrong data); for high-volume feeds, drop-and-quarantine keeps the pipeline alive while preserving the evidence. Either answer is acceptable if argued from consequences.
3. File notification mode needs cloud plumbing the listing mode does not: an SNS topic and SQS queue (or letting Databricks create them, which needs those IAM privileges). It pays for itself when directory listing becomes the bottleneck — directories with many thousands of files or high-frequency arrivals; at this lab's volume it is pure overhead.

## Lab 11

1. The DAG gains a parameterized fan-out: one Silver task template, N task runs. At fifty states that is fifty task runs per trigger — orchestration overhead per state, and a cluster/warehouse charge pattern that scales with the list. If the per-state work is one query, a single partitioned query is cheaper than fifty tasks.
2. It removes the assumption that data arrives on the schedule's clock. The Job now runs when files actually land — no missed late files, no empty runs on quiet days.
3. Send it to whoever owns the budget the warehouse bills to (platform owner / team lead). It informs whether the Job's schedule, warehouse size, or existence is justified — cost attribution turns "the bill went up" into "this Job costs this much per run."

## Lab 12

1. Add the Job under `targets.prod.resources` so it deploys only there. It works, but every target-specific resource is a fork you now maintain by hand — the further targets diverge, the closer you are to two pipelines with a shared filename.
2. The service principal credential lives in GitLab CI/CD variables (masked/protected), never in the repo. A malformed release tag fails the pipeline's tag rule (or the validate stage) — nothing deploys, which is the correct outcome.
3. The re-deploy overwrote the manual change. Bundle-managed resources are owned by the bundle: the workspace UI is a read-only view of them in practice, and any change worth keeping goes into the bundle source, through the same pipeline as everything else.
