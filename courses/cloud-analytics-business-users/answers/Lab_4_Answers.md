# Lab 4 Answers: Spark DataFrames and the ETL Path

Attempt the questions before opening this file.

---

## Knowledge Check Answers

**1. Why did `spark.table(...)` finish instantly while `display(df.limit(20))` took several seconds?**

`spark.table()` is a transformation — it builds a plan and reads nothing. `display()` is an action: it forces the plan to execute, and that is when the data is actually read. Lazy evaluation means work happens at the action, not where the chain was defined.

**2. Name two transformations that do not cause a shuffle and one that always does.**

Narrow (no shuffle): `filter`, `select`, `withColumn` — each partition is processed independently. Always shuffles: `groupBy` aggregation (and joins in the general case) — rows with the same key must be brought together, which means moving data between machines.

**3. Your `groupBy` produced a large shuffle read. What does that figure represent physically?**

The bytes this stage's tasks fetched across the cluster — data written out by the previous stage's tasks (shuffle write) and pulled over the network or from disk so that all rows sharing a key land in the same partition.

**4. The key column is `#ID_RSSD`. How do you reference it in SQL, and how in the DataFrame API?**

SQL requires backticks: `` SELECT `#ID_RSSD` … ``. The DataFrame API takes it as an ordinary string — `F.col("#ID_RSSD")`, `df.select("`#ID_RSSD`")` — no parser in the way, which is one practical reason to reach for PySpark when column names are awkward.

**5. You wrote the summary with `saveAsTable` rather than saving a file. What did that buy you?**

A managed Delta table registered in Unity Catalog: it is governed (you can `GRANT` on it), discoverable in Catalog Explorer, queryable by name from SQL, and carries ACID guarantees and version history. A file on a path has none of that.

**6. For read, clean, aggregate and verify — which would you do in PySpark and which in SQL, and why?**

A defensible split: read, clean, and aggregate in PySpark, because each step is a named variable you can inspect, test, and re-run — the shape of a repeatable pipeline. Verify in SQL, because verification is ad-hoc, read-only querying, which is what SQL is best at. Other splits are arguable; what matters is choosing per stage for a reason rather than by preference.

**7. When is caching a mistake?**

When the DataFrame is used once — you pay memory for no re-use. Also when the data is too large for cluster memory (eviction and disk spill can make things slower) and when cached results hide upstream changes during development. Cache only what you will reuse several times in the same session.

---

## Stretch Task Answers

**1. `spark.sql.shuffle.partitions = 8` — what changed, and did it get faster?**

The post-shuffle stage drops from the default 200 tasks to 8. On this small dataset the wall-clock time is about the same or slightly faster — 200 tiny tasks were mostly scheduling overhead. The lesson: shuffle partition count should be sized to the data. 200 partitions for a few thousand rows is waste; 8 partitions for a terabyte would be a disaster in the other direction.

**2. `.cache()` before the aggregation, run twice — why is the second run faster, and when is caching a bad idea?**

The first run reads from the source table, executes the chain, and populates the cache; the second run serves the pre-aggregation DataFrame from memory and skips the read and transforms. Bad idea: single-use data, data larger than memory, or when it masks upstream changes — see Knowledge Check 7.

**3. The whole pipeline as one SQL statement.**

```sql
CREATE OR REPLACE TABLE training_nic.analyst.institution_summary AS
SELECT CHTR_TYPE_CD,
       YEAR(CAST(D_DT_START AS DATE))  AS start_year,
       COUNT(*)                        AS institution_count,
       COUNT(DISTINCT CITY)            AS distinct_cities
FROM training_nic.migrated.institutions
WHERE STATE_ABBR_NM = 'CA'
GROUP BY CHTR_TYPE_CD, YEAR(CAST(D_DT_START AS DATE));
```

Hand the **SQL version** to a colleague — one statement, no session state, runs anywhere. Maintain the **PySpark version** as a scheduled job — the chain is stepwise, each stage is testable, and intermediate results can be inspected when a run goes wrong. Either answer is defensible; the trade-off is hand-off simplicity versus maintainability.
