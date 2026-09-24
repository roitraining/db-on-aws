# Lab 6 Answers: Dashboards and Genie

Attempt the questions before opening this file.

---

## Knowledge Check Answers

**1. What is the practical difference for a viewer between shared credentials and individual permissions?**

Shared (embedded) credentials: every viewer sees the data through the **publisher's** access, so everyone sees identical figures. Individual permissions: each viewer sees only what their **own** grants allow — two people can open the same dashboard and see different numbers, or none at all.

**2. Who can a published dashboard be shared with, and why is that reach worth thinking about before you publish?**

Anyone registered to your Databricks **account** — including people with no access to the workspace the dashboard lives in. With shared credentials that is your data access being extended to all of them, so before publishing, confirm the data is something you are entitled to show that broadly.

**3. Your filter changes one chart but not the other. What is misconfigured?**

The filter's scope — it is applied to a single widget. Change its scope so it applies to both charts (they must draw from the same dataset for one filter to drive both).

**4. You renamed `CHTR_TYPE_CD` on the dashboard. Why was preserving that name upstream still the right decision?**

Upstream, the native name is the traceable link back to the source system — validation queries, joins, and the migration audit all depend on the column being recognizably the same one NIC shipped. Renaming early breaks that provenance silently. Presentation is the layer whose job is translation; that is where business language belongs.

**5. Genie answered a question confidently and wrongly. Describe how you would establish that, and what you would conclude.**

Expand the SQL Genie generated and compare it — or its result — against a query you wrote yourself for the same question. If they disagree, conclude that Genie answered the question it understood, which was not the question you asked; it is not malfunctioning. The generated SQL is the evidence of what it actually computed.

**6. Give one question you would let Genie answer unsupervised and one you would always verify. What distinguishes them?**

Unsupervised: a question with an unambiguous answer in the data — "How many institutions are in California?" Always verify: a question requiring a judgement or definition the data does not carry — "Which states are underserved?" The distinguisher is whether answering requires a definition; if it does, Genie will confidently supply its own.

**7. Your dashboard is read hundreds of times a day over a large table. What would you change about how the data is published?**

Back it with a materialized view instead of a plain view, refreshed on a schedule matched to how often the data actually changes — precomputed results make every read cheap, and the staleness window is a deliberate choice rather than an accident.

---

## Stretch Task Answers

**1. Republish with individual permissions — you still see data. Why?**

You hold every grant on your own catalog, schema, and views, so your own permissions reproduce the shared-credential view exactly. The point generalizes the other way: a viewer holding none of your grants would see errors or empty tiles on both pages — individual permissions make the dashboard honest about each viewer's access, shared credentials make it consistent. In a shared workspace, revoking a viewer's `USE SCHEMA` and having them reload shows the failure mode live.

**2. Add a KPI tile responding to the same filter.**

Add a counter visualization widget on the same dataset, aggregating `SUM(institution_count)` (or a `COUNT`), and confirm the filter's scope includes it. Because it draws from the same dataset as the charts, the `start_year` filter drives all three widgets together.

**3. Ask Genie an ambiguous question you know the answer to.**

The specific assumption varies, but the pattern is constant: Genie resolves the ambiguity with a definition of its own (a date range, a metric, an interpretation of "biggest") and states the answer with full confidence. The fix is to improve the Genie space's context — add column descriptions, synonyms, and example questions in the space's instructions so the resolution it picks matches the one your organization means. Then re-ask and compare the generated SQL.
