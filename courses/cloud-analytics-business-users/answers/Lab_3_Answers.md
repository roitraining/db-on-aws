# Lab 3 Answers: UAT — Comparing Cloud Data to On-Premises

Attempt the lab before opening this file — the point of Lab 3 is the investigation, and reading the defect list first takes that away from you. If you are stuck, read only the section you need.

---

## The Five Planted Defects (full answer key)

| # | Defect | What you observe | Magnitude |
|---|--------|------------------|-----------|
| 1 | Rows dropped | Check 1 gap; every missing key has `CHTR_TYPE_CD = '250'` | 381 rows (62,080 → 61,699) |
| 2 | Decimals truncated | `SUM(TOT_ASSETS)` differs while row counts match — values were cast to whole dollars | ~$31,962 lost across 61,699 rows in `financials` |
| 3 | Dates shifted | `D_DT_START` differs on a subset; `datediff` grouped shows every affected row is exactly **+1 day** | 8,770 rows |
| 4 | Empty string became NULL | Cloud `CITY` has 2,687 NULLs where the source has 2,709 empty strings — `''` was mapped to NULL on migration (the difference is empty-city rows that were also charter 250) | 2,687 rows |
| 5 | Padding stripped | Source `NM_LGL` is space-padded to 120 — genuinely present in the real FFIEC export — and the cloud copy is trimmed | every source row |

Defects 4 and 5 are **comparison artifacts turned real**: they change representation, not meaning, but they make the naive row-level comparison explode until you normalize with `TRIM` and `NULLIF`. Defects 1–3 are genuine data problems: 1 blocks cutover (rows are gone), 2 and 3 need an engineering decision on whether the transformation was intended.

The build stages these defects as **separate commits**, so `DESCRIBE HISTORY` on `migrated.institutions` is the migration's audit log: version 0 is the faithful 62,080-row copy (what Task 6 time-travels to), the `DELETE` commit carries defect 1's predicate in `operationParameters`, and the `UPDATE` commits are defects 5, 4, and 3 in order.

---

## Knowledge Check Answers

**1. Name the four checks in order and state the question each one answers.**

1. **Row count parity** — did everything arrive?
2. **Key parity** — *which* rows are missing or extra?
3. **Aggregate parity** — are the values right in bulk?
4. **Row-level comparison** — which specific values differ?

**2. Check 1 passes. What have you proven, and what have you not?**

You have proven the totals match — nothing more. The same count can hide dropped rows canceled out by duplicates, and it says nothing about whether any value in the rows that did arrive is correct. That is why three more checks exist.

**3. Your row-level comparison reports that 98% of rows differ. What is the most likely explanation, and what do you do before reporting it?**

A comparison artifact, not a migration defect — almost always formatting, and in this dataset the trailing padding on `NM_LGL`. Normalize both sides with `TRIM` and `NULLIF(…, '')`, re-run, and compare the naive and normalized counts. Report only what survives normalization.

**4. Why use `IS DISTINCT FROM` rather than `<>` when comparing two columns that may contain nulls?**

`<>` returns NULL when either side is NULL, and a NULL predicate drops the row from the result — mismatches involving NULLs silently vanish. `IS DISTINCT FROM` treats two NULLs as equal and a NULL versus a value as different, which is exactly the semantics a comparison needs.

**5. A numeric column's sum differs between systems while the row count matches exactly. What kind of error does that suggest?**

Per-row value corruption rather than missing data — typically truncation or rounding applied during conversion. Here it is defect 2: `TOT_ASSETS` was cast to whole dollars, shaving ~$31,962 in cents off across the table. Small on any single row, visible only in the aggregate.

**6. Missing rows all share the same value in one column. Why is that more useful than the count of missing rows?**

A shared value points at a systematic cause — here, a filter that excluded `CHTR_TYPE_CD = '250'` — which tells the engineer exactly what to fix and predicts that re-running the migration without the filter recovers all 381 rows. A bare count tells them only how big the hole is.

**7. Why is the row-level comparison last rather than first?**

It is the most expensive check (every column of every row) and the noisiest — run first, it drowns you in formatting artifacts. Checks 1–3 scope the problem cheaply, and the normalization lesson removes the noise before you pay for the detailed comparison.

**8. What is the practical limit on how far back a time travel query can reach, and why?**

The deleted-file retention period — `delta.deletedFileRetentionDuration`, **7 days (168 hours)** by default (table history metadata is kept longer, 30 days). Past that, the data files behind old versions have been removed, and Databricks Runtime 18+ blocks the query with `DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION` rather than return incomplete results.

---

## Stretch Task Answers

**1. All four checks as one result set.**

```sql
SELECT 'check 1: row count' AS check,
       CASE WHEN (SELECT COUNT(*) FROM training_nic.migrated.institutions) =
                 (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions)
            THEN 'PASS' ELSE 'FAIL' END AS result
UNION ALL
SELECT 'check 2: key parity',
       CASE WHEN (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions s
                  LEFT ANTI JOIN training_nic.migrated.institutions c
                    ON s.`#ID_RSSD` = c.`#ID_RSSD`) = 0
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'check 3: aggregate parity',
       CASE WHEN (SELECT SUM(TOT_ASSETS) FROM training_nic.migrated.financials) =
                 (SELECT SUM(TOT_ASSETS) FROM training_nic.legacy_onprem.financials)
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'check 4: row-level (normalized)',
       CASE WHEN (SELECT COUNT(*) FROM training_nic.migrated.institutions c
                  JOIN training_nic.legacy_onprem.institutions s
                    ON c.`#ID_RSSD` = s.`#ID_RSSD`
                  WHERE NULLIF(TRIM(c.NM_LGL), '') IS DISTINCT FROM NULLIF(TRIM(s.NM_LGL), '')) = 0
            THEN 'PASS' ELSE 'FAIL' END;
```

On this dataset every check fails except check 4's name column after normalization — which is the correct result, and the skeleton of a reusable validation framework.

**2. Every text column at once.**

Chain the normalized comparison across columns with `OR`:

```sql
SELECT COUNT(*) AS any_text_mismatch
FROM training_nic.migrated.institutions c
JOIN training_nic.legacy_onprem.institutions s ON c.`#ID_RSSD` = s.`#ID_RSSD`
WHERE NULLIF(TRIM(c.NM_LGL), '')        IS DISTINCT FROM NULLIF(TRIM(s.NM_LGL), '')
   OR NULLIF(TRIM(c.CITY), '')          IS DISTINCT FROM NULLIF(TRIM(s.CITY), '')
   OR NULLIF(TRIM(c.STATE_ABBR_NM), '') IS DISTINCT FROM NULLIF(TRIM(s.STATE_ABBR_NM), '')
   OR NULLIF(TRIM(c.CHTR_TYPE_CD), '')  IS DISTINCT FROM NULLIF(TRIM(s.CHTR_TYPE_CD), '');
```

It is expensive because every text column of every row on both sides must be read, normalized, and compared — nothing can be pruned. It runs last because until the formatting artifacts are understood and normalized away, its output is dominated by noise.

**3. Prove a defect exists independent of your normalization choices.**

Pick a defect that involves no text at all. The date shift:

```sql
SELECT datediff(c.D_DT_START, s.D_DT_START) AS day_difference, COUNT(*) AS rows
FROM training_nic.migrated.institutions c
JOIN training_nic.legacy_onprem.institutions s ON c.`#ID_RSSD` = s.`#ID_RSSD`
WHERE c.D_DT_START IS DISTINCT FROM s.D_DT_START
GROUP BY 1;
```

No `TRIM`, no `NULLIF` — and it still reports 8,770 rows, every one exactly +1 day. The same logic works for the missing rows (a count by `CHTR_TYPE_CD` on both sides) — a defect that survives with normalization removed cannot be an artifact of it.
