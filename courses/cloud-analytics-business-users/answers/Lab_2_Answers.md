# Lab 2 Answers: SQL Translation, Cross-Schema Joins, and Parameters

Attempt the questions before opening this file.

---

## Knowledge Check Answers

**1. Write the Databricks SQL equivalent of `SELECT TOP 50 * FROM dbo.Institutions`.**

```sql
SELECT * FROM training_nic.migrated.institutions LIMIT 50;
```

(Or just `FROM institutions LIMIT 50` after `USE CATALOG` / `USE SCHEMA`.)

**2. `DATEDIFF(day, '2026-01-01', '2026-01-10')` returns 9 in T-SQL. What is the Databricks equivalent, and what happens if you keep the T-SQL argument order?**

```sql
SELECT datediff('2026-01-10', '2026-01-01');   -- 9, end date first
```

Keeping the T-SQL order returns **-9**. No error is raised — the sign silently flips, which is why this is the most dangerous translation in the lab.

**3. Why does `CAST('abc' AS INT)` raise here but return null in SQL Server, and which is safer during a migration?**

Databricks SQL runs in ANSI mode, so an invalid cast raises a runtime error; SQL Server's implicit conversion rules coerce or truncate and keep going. (`CAST('abc' AS INT)` in SQL Server actually errors too, but many implicit conversions silently succeed.) During migration validation, the error is safer: it names the rows that no longer convert cleanly instead of letting them pass through as wrong numbers. Use `TRY_CAST` only when `NULL` is the outcome you actually want.

**4. You joined two tables in different schemas without a linked server. What made that possible, and which privileges did you need?**

Both schemas live under the same catalog in the Unity Catalog metastore, so a cross-schema join is just a fully qualified name — no linked server, synonym, or cross-database permission. You needed `SELECT` on each table, `USE CATALOG` on `training_nic`, and `USE SCHEMA` on both `migrated` and `reference`.

**5. A colleague who does not write SQL needs to run your query for any date range. What did you add, and what does it not protect them from?**

Named parameters `:start_date` and `:end_date`, typed as **Date** so they get a calendar picker. It does not protect them from asking a nonsensical question — a start date after the end date returns zero rows with no error.

**6. Why is committing a query to a Git folder better than emailing the `.sql` file?**

The repository gives one authoritative copy with history: a colleague pulls the latest version, sees who changed what and why, and can propose changes for review. Email produces divergent copies with no history — `final_v3_USE_THIS.sql`.

---

## Stretch Task Answers

**1. Rewrite the Task 3 join using `USE SCHEMA`.**

```sql
USE CATALOG training_nic;
USE SCHEMA migrated;

SELECT i.STATE_ABBR_NM,
       COUNT(*)          AS institution_count,
       MAX(c.population) AS state_population
FROM institutions AS i
JOIN reference.state_population AS c
  ON i.STATE_ABBR_NM = c.state_abbr
GROUP BY i.STATE_ABBR_NM
ORDER BY institution_count DESC;
```

Hand a colleague the **fully qualified** version: it runs correctly regardless of their session state, while this one silently depends on the `USE` statements having been run first.

**2. Add a minimum-count parameter with `HAVING`.**

```sql
SELECT i.STATE_ABBR_NM, COUNT(*) AS institution_count
FROM training_nic.migrated.institutions AS i
WHERE i.D_DT_START BETWEEN :start_date AND :end_date
GROUP BY i.STATE_ABBR_NM
HAVING COUNT(*) >= :min_count
ORDER BY institution_count DESC;
```

Set `:min_count` to a numeric parameter type. `HAVING` filters after aggregation; `WHERE` cannot see the count.

**3. How many rows in `institutions` have a `NM_LGL` whose length changes when trimmed?**

```sql
SELECT COUNT(*) FROM institutions
WHERE LENGTH(NM_LGL) <> LENGTH(TRIM(NM_LGL));
```

Against `training_nic.migrated`: **0 rows** — the cloud copy was trimmed during migration. Run the same query against `training_nic.legacy_onprem` and **all 5,000 rows** change length, because the source exported fixed-width `CHAR` columns padded to their declared width. That asymmetry is exactly why Lab 3's naive row-level comparison reports almost every name as different.
