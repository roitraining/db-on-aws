-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Module 3 Demo: The Four Checks, in Order
-- MAGIC
-- MAGIC Nine minutes. Serverless compute. **Do NOT reveal how many defects are planted** —
-- MAGIC the lab is a discovery exercise. This demo shows the *method*: each check narrows
-- MAGIC the next, cheapest first.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check 1 — row count parity: did everything arrive?

-- COMMAND ----------

SELECT 'source (on-prem)' AS side, COUNT(*) AS rows FROM training_nic.legacy_onprem.institutions
UNION ALL
SELECT 'cloud (migrated)', COUNT(*) FROM training_nic.migrated.institutions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 62,080 against 61,699 — **381 rows short**. The count says *how many*; it cannot say *which*.
-- MAGIC That is check 2's job.
-- MAGIC
-- MAGIC ### Check 2 — key parity: WHICH rows are missing?
-- MAGIC Show the missing rows and **let the room spot what they share** — don't tell them.

-- COMMAND ----------

SELECT s.`#ID_RSSD`, s.NM_LGL, s.STATE_ABBR_NM, s.CHTR_TYPE_CD
FROM   training_nic.legacy_onprem.institutions s
LEFT ANTI JOIN training_nic.migrated.institutions c
  ON   s.`#ID_RSSD` = c.`#ID_RSSD`
LIMIT  20;

-- COMMAND ----------

-- once someone says it: confirm the pattern
SELECT CHTR_TYPE_CD, COUNT(*) AS missing_rows
FROM   training_nic.legacy_onprem.institutions s
LEFT ANTI JOIN training_nic.migrated.institutions c
  ON   s.`#ID_RSSD` = c.`#ID_RSSD`
GROUP  BY CHTR_TYPE_CD;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC All 381 share one charter type. *"381 rows are missing"* prompts a hunt;
-- MAGIC *"every missing row is charter type 250"* is a question engineering can answer today.
-- MAGIC Characterisation is the analyst's value.
-- MAGIC
-- MAGIC ### Check 3 — aggregate parity: do the totals agree?
-- MAGIC Pair every SUM with MIN, MAX and a null count — a matching SUM alone can hide two
-- MAGIC errors that cancel.

-- COMMAND ----------

SELECT 'source' AS side,
       SUM(TOT_ASSETS)                      AS total_assets,
       MIN(TOT_ASSETS)                      AS min_assets,
       MAX(TOT_ASSETS)                      AS max_assets,
       COUNT_IF(TOT_ASSETS IS NULL)         AS null_assets
FROM   training_nic.legacy_onprem.financials
UNION ALL
SELECT 'cloud',
       SUM(TOT_ASSETS), MIN(TOT_ASSETS), MAX(TOT_ASSETS), COUNT_IF(TOT_ASSETS IS NULL)
FROM   training_nic.migrated.financials;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The sums differ by a **small** amount (~$32K on billions). Say it plainly: large
-- MAGIC differences get investigated; is this one rounding, or a pattern? (It is a pattern —
-- MAGIC but let the lab find it.)
-- MAGIC
-- MAGIC ### Check 4 — row-level comparison: a flood of mismatches
-- MAGIC Run checks 1–3 first and check 4 becomes a *targeted* question. Run it first and you
-- MAGIC get this:

-- COMMAND ----------

SELECT COUNT(*) AS naive_name_mismatches
FROM   training_nic.migrated.institutions c
JOIN   training_nic.legacy_onprem.institutions s
  ON   c.`#ID_RSSD` = s.`#ID_RSSD`
WHERE  c.NM_LGL <> s.NM_LGL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Nearly **every row** mismatches. Pause. *"Are these real?"* Pull one pair and look:

-- COMMAND ----------

SELECT c.NM_LGL              AS cloud_name,
       s.NM_LGL              AS source_name,
       LENGTH(c.NM_LGL)      AS cloud_len,
       LENGTH(s.NM_LGL)      AS source_len
FROM   training_nic.migrated.institutions c
JOIN   training_nic.legacy_onprem.institutions s
  ON   c.`#ID_RSSD` = s.`#ID_RSSD`
WHERE  c.NM_LGL <> s.NM_LGL
LIMIT  3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Identical values, different lengths — **padding** from a fixed-width export.
-- MAGIC Normalise both sides before concluding anything:

-- COMMAND ----------

SELECT COUNT(*) AS real_name_mismatches
FROM   training_nic.migrated.institutions c
JOIN   training_nic.legacy_onprem.institutions s
  ON   c.`#ID_RSSD` = s.`#ID_RSSD`
WHERE  NULLIF(TRIM(c.NM_LGL), '') IS DISTINCT FROM NULLIF(TRIM(s.NM_LGL), '');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Tens of thousands of raw mismatches; **zero** after normalisation. Report *both*
-- MAGIC numbers — "61,692 raw, 0 after normalisation" shows you understood the difference
-- MAGIC rather than hiding it. Then hand the room to the lab: *"the migration was not clean.
-- MAGIC Run the four checks in order, normalise before you conclude, and document what you
-- MAGIC find — and what you did not check."*
