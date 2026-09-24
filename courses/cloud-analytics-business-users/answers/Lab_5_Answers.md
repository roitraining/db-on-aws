# Lab 5 Answers: Publishing Datasets for Self-Service

Attempt the questions before opening this file.

---

## Knowledge Check Answers

**1. Name the three privileges required for a colleague to read your view, and state which one is most often forgotten.**

`SELECT` on the view, `USE CATALOG` on the catalog, `USE SCHEMA` on the schema. The one people forget is `USE SCHEMA` — `SELECT` alone produces an access-denied error that looks like a bug and is not.

**2. A colleague has `SELECT` and still gets access denied. What do you check, and in what order?**

Walk the namespace top-down: `SHOW GRANTS ON CATALOG training_nic` (is `USE CATALOG` there?), then `SHOW GRANTS ON SCHEMA training_nic.analyst` (is `USE SCHEMA` there?), then `SHOW GRANTS ON VIEW …` (is `SELECT` really there, on the right principal spelling?). The gap is whichever level does not list the principal.

**3. When is a materialized view worth its refresh cost, and when is a plain view the better answer?**

Worth it when reads are frequent and latency-sensitive and some staleness between refreshes is acceptable — a dashboard refreshed hourly and read hundreds of times. A plain view wins when reads are occasional or the data must always be live: it costs nothing at rest and is never stale.

**4. What can a plain view do that a materialized view cannot?**

Always reflect the current state of the base table — there is no refresh to lag behind. It also costs nothing between queries, and the underlying data can be queried with time travel; materialized views do not support time travel.

**5. Why can you not point an alert at a query you saved earlier?**

Each alert owns its query definition — the query is authored inside the alert editor and stored with the alert. Saved queries in the SQL editor are a separate object type that alerts cannot reference.

**6. What three statuses can an alert report, and what does each mean?**

`OK` — the last scheduled run evaluated the condition and it was not met. `TRIGGERED` — the condition was met and notifications were sent. `ERROR` — the alert's query itself failed to run; fix the query, because until it runs the condition is not being watched at all.

**7. Publishing a view replaced emailing a spreadsheet. Name two things that improved beyond convenience.**

Any two of: access is governed and auditable (grants can be inspected and revoked; an emailed file is gone forever); consumers always see current data through one live definition instead of divergent stale copies; and there is a single version of the logic, so a fix propagates to every reader instantly.

---

## Stretch Task Answers

**1. Fire the alert deliberately.**

Raise the threshold above the current row count and let the schedule run: status flips to `TRIGGERED` and the email arrives. Without the alert, the first person to notice the broken table would have been a stakeholder looking at wrong or missing numbers on the dashboard — and you would have heard about it from them.

**2. Revoke `USE SCHEMA`, leave `SELECT`.**

```sql
REVOKE USE SCHEMA ON SCHEMA training_nic.analyst FROM `account users`;
```

The three-level audit now shows `SELECT` at the view and `USE CATALOG` at the catalog, but nothing at the schema level — any reader would get access denied again even though `SELECT` is intact — demonstrating that `SELECT` is necessary but not sufficient. The missing grant is `USE SCHEMA`, the traversal privilege that lets them walk through the namespace to reach the object.

**3. Audit all three levels with `SHOW GRANTS`.**

```sql
SHOW GRANTS ON CATALOG training_nic;
SHOW GRANTS ON SCHEMA  training_nic.analyst;
SHOW GRANTS ON VIEW    training_nic.analyst.institution_summary_published;
```

To find a permission gap, check that the principal appears at **every** level with the right privilege: `USE CATALOG` in the first output, `USE SCHEMA` in the second, `SELECT` in the third. Any level where the principal is absent is the gap — access requires all three simultaneously.
