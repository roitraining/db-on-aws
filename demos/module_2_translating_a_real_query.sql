-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Module 2 Demo: Translating a Real Query, then Parameterising It
-- MAGIC
-- MAGIC Part A walks the T-SQL → Databricks SQL translations (slide: *Translating a Real Query*).
-- MAGIC Part B adds parameters (slide: *Parameterise and Commit*). Serverless compute.
-- MAGIC
-- MAGIC The T-SQL original we are translating:
-- MAGIC ```sql
-- MAGIC SELECT TOP 10 ID_RSSD, NM_LGL, ISNULL(CITY, '(unknown)') AS city,
-- MAGIC        LEN(NM_LGL) AS name_len,
-- MAGIC        DATEDIFF(day, D_DT_START, GETDATE()) AS days_open
-- MAGIC FROM   institutions;
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A1 — TOP becomes LIMIT, and the key needs backticks
-- MAGIC `SELECT TOP 10 ...` is a parse error here — loud, obvious, ten-second fix. And `#ID_RSSD`
-- MAGIC starts with `#`, so SQL needs backticks around it. (Uncomment the TOP line to show the room the error.)

-- COMMAND ----------

-- SELECT TOP 10 `#ID_RSSD`, NM_LGL FROM training_nic.migrated.institutions;   -- parse error: TOP

SELECT `#ID_RSSD`, NM_LGL
FROM   training_nic.migrated.institutions
LIMIT  10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A2 — LEN becomes LENGTH
-- MAGIC And look at the values it returns: **every name is 120 characters**. Say nothing more —
-- MAGIC this is the seed for tomorrow's comparison work.

-- COMMAND ----------

SELECT NM_LGL, LENGTH(NM_LGL) AS name_len
FROM   training_nic.migrated.institutions
LIMIT  5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A3 — ISNULL(x, y) becomes COALESCE
-- MAGIC Two-argument `ISNULL` is a T-SQL-ism and fails loudly here. `COALESCE` is the portable
-- MAGIC spelling — and pairing it with `NULLIF` also catches *empty strings*, which this source
-- MAGIC data actually contains:

-- COMMAND ----------

SELECT `#ID_RSSD`,
       CITY                                    AS raw_city,
       COALESCE(NULLIF(CITY, ''), '(unknown)') AS city
FROM   training_nic.legacy_onprem.institutions
WHERE  CITY = ''
LIMIT  5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A4 — the one that fails SILENTLY: DATEDIFF argument order
-- MAGIC T-SQL is `DATEDIFF(day, start, end)`. The two-argument Databricks form is
-- MAGIC `datediff(END, start)` — end date **first**. Write it in T-SQL habit order and it
-- MAGIC compiles, runs, and returns **negative numbers**. That is the only symptom.

-- COMMAND ----------

-- T-SQL habit order: runs fine, silently wrong
SELECT NM_LGL,
       datediff(D_DT_START, current_date()) AS days_open_WRONG
FROM   training_nic.migrated.institutions
LIMIT  5;

-- COMMAND ----------

-- end date first: the sign corrects
SELECT NM_LGL,
       datediff(current_date(), D_DT_START) AS days_open
FROM   training_nic.migrated.institutions
LIMIT  5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Land the summary: **ten of the eleven differences fail loudly and cost seconds.
-- MAGIC This one fails silently** — which is why the translation table matters.
-- MAGIC
-- MAGIC ---
-- MAGIC ### B — Parameterise it
-- MAGIC Running the next cell makes **`start_date` and `end_date` widgets appear** at the top of
-- MAGIC the notebook. Set them (try `2000-01-01` and `2010-12-31`) and re-run — new answer, no
-- MAGIC SQL edited. In the SQL editor the same `:name` syntax gives colleagues a **calendar
-- MAGIC picker** when the parameter type is set to Date — that plus a Git folder commit is
-- MAGIC exactly what Lab 2 has everyone build.

-- COMMAND ----------

SELECT STATE_ABBR_NM        AS state,
       COUNT(*)             AS institutions_opened
FROM   training_nic.migrated.institutions
WHERE  D_DT_START BETWEEN :start_date AND :end_date
GROUP  BY STATE_ABBR_NM
ORDER  BY institutions_opened DESC
LIMIT  10;
