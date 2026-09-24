# Lab 1 Answers: Orientation and Your First Query

Attempt the questions before opening this file. If you are stuck mid-lab, find the step you are on and read just that answer.

---

## Knowledge Check Answers

**1. SQL Server uses `dbo.TableName`. What are the three parts of the equivalent name in Unity Catalog, and what does the extra level represent?**

`catalog.schema.table` — for example `training_nic.migrated.institutions`. The new level is the **catalog**: a governance boundary above schema. SQL Server's `database.schema.table` maps roughly onto `catalog.schema.table`, with the catalog carrying the access-control and isolation role the database played.

**2. You have been granted `SELECT` on a table but still cannot query it. What is the most likely missing privilege?**

`USE SCHEMA` on the parent schema (or `USE CATALOG` on the catalog). Reading a table takes three privileges — `SELECT` on the table, `USE CATALOG`, and `USE SCHEMA`. The traversal grants are the ones people forget, and `USE SCHEMA` is the most common gap.

**3. A colleague's query has been "running" for four minutes with no result. What should you check before looking at their SQL?**

The compute selector at the top right of the SQL editor. A stopped or still-starting SQL warehouse leaves queries queued indefinitely — the query is not slow, it has not started.

**4. Why does `SELECT TOP 100 * FROM institutions` fail, and what is the correct form?**

`TOP` is T-SQL syntax and does not exist in Databricks SQL. The limit goes at the end of the statement:

```sql
SELECT * FROM institutions LIMIT 100;
```

**5. Your cloud row count does not match the on-premises figure. Name two things that could cause this that are not a data problem.**

Any two of: you are counting a different table than you think (wrong catalog or schema — `legacy_onprem` vs `migrated`); the two counts were taken at different times against a source that is still receiving writes; or a filter difference between the two queries (an implicit `WHERE`, a view instead of the base table). Rule these out before reporting a migration defect.

---

## Stretch Task Answers

**Which schema holds the on-premises data?** `legacy_onprem` — it is the snapshot extract taken from SQL Server at cutover. `migrated` is the cloud copy being validated against it.

**Which schema holds reference data?** `reference` — it carries `state_population`, the US Census lookup table used for joins in Lab 2.

**How many versions does the institutions table have, and what does that suggest?** Around nine — an initial `CREATE TABLE AS SELECT`, a `DELETE`, several `UPDATE`s, and metadata commits for table and column comments. That tells you the table was built by a scripted, multi-step process rather than a single manual load, and that Delta records every one of those writes in a queryable history. Lab 3 reads this history directly: the operations you can see here are, in fact, the migration steps you will be validating.
