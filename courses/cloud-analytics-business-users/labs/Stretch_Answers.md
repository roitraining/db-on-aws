# Stretch Task Answers — Instructor Reference

**Course:** Databricks on AWS: Cloud Analytics for Business Users

Answers to the stretch tasks in Labs 1–6. For instructor use — attendees get the reasoning, not this file.

---

## Lab 1

1. **On-premises source schema:** `legacy_onprem`. It carries the SQL Server export artifacts (padded names, empty-string cities).
2. **Reference/lookup schema:** `reference` — curated tables with clean column names (`state_population`).
3. **History:** two versions (0 and 1), both overwrite operations, seconds apart. The table was written by a scripted job and rebuilt at least once — it did not grow row by row.

## Lab 2

1. Run `USE CATALOG training_nic; USE SCHEMA migrated;` then only the `reference` side needs qualification. Hand a colleague the **fully qualified** version — it runs correctly regardless of their session state.
2. Add `HAVING COUNT(*) >= :min_count` after `GROUP BY`, and set the widget type to **Number**.

    ```sql
    GROUP BY i.STATE_ABBR_NM
    HAVING COUNT(*) >= :min_count
    ORDER BY institution_count DESC;
    ```
3. In `migrated`: **0 rows** (the migration trimmed the names). In `legacy_onprem`: **5,000 rows** — every name is padded. The asymmetry *is* the whitespace defect Lab 3 uncovers.

## Lab 3

1. One `SELECT` per check, `UNION ALL`ed, each returning a check name and a `CASE` pass/fail. Pattern:

    ```sql
    SELECT 'row_count' AS check,
           CASE WHEN (SELECT COUNT(*) FROM training_nic.migrated.institutions)
                   = (SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions)
                THEN 'PASS' ELSE 'FAIL' END AS status
    UNION ALL
    -- repeat for key delta, aggregate delta, sample compare
    ```
2. Compare all text columns via `EXCEPT` on the full normalized row (or a hash of it). Expensive because every column of every row is shuffled and compared; it runs last because the targeted checks first tell you *which* column and *how many* rows — the full diff alone only tells you "something differs."
3. Pick one key and show both raw values side by side with no normalization applied, e.g. the dropped-rows defect needs none at all:

    ```sql
    SELECT COUNT(*) FROM training_nic.legacy_onprem.institutions
    WHERE CHTR_TYPE_CD = '250';   -- 100 rows on-prem, 0 in migrated
    ```

## Lab 4

1. The aggregation stage drops from 200 tasks to 8. On data this small it is the same speed or slightly faster — the default 200 spends more time scheduling tasks than doing work. The point is that partition count is a knob, not a constant.
2. The second run is faster because the DataFrame is served from cluster memory instead of re-read from S3. Caching is a bad idea for single-pass jobs (you pay to populate a cache you never reuse) and for data larger than cluster memory.
3. The SQL version is the one to hand to a colleague and the one to schedule — one statement, no state. PySpark earns its keep when the logic outgrows one statement: branching, loops, UDFs, or multiple outputs.

## Lab 5

1. The alert transitions to `TRIGGERED` and notifies its recipients. Without it, a stakeholder sees nothing — the dashboard just quietly shows numbers that stopped being true.
2. The failure returns because `SELECT` alone is not enough — the missing grant is `USE SCHEMA`. Reading needs all three: `USE CATALOG`, `USE SCHEMA`, `SELECT`.
3. ```sql
    SHOW GRANTS ON CATALOG training_nic;
    SHOW GRANTS ON SCHEMA training_nic.analyst;
    SHOW GRANTS ON VIEW training_nic.analyst.<view>;
    ```
    A permission gap is a principal holding a lower-level grant (`SELECT`) with no matching `USE` grant above it — visible only by reading the three outputs together.

## Lab 6

1. With individual permissions the dashboard runs with the **viewer's** credentials. The partner sees the same numbers only if they hold the same grants; without them they get errors or empty tiles. Shared (embedded-credential) publishing is what made the numbers identical before.
2. Add a **Counter** visualization on `COUNT(*)` over the same dataset, and bind it to the existing filter widget so it responds together with the charts.
3. Genie fills ambiguity from the dataset's descriptions. Whatever it assumed (a date range, a definition of "biggest"), the fix is the same: write the definition into the dataset/column descriptions — "institution count means rows in migrated.institutions" — so the assumption becomes explicit instead of guessed.
