-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Module 6 · Demo 2: Genie, Right and Wrong
-- MAGIC
-- MAGIC **Twelve minutes.** Never cut this one — it is a scepticism-*validating* demo, not a sales
-- MAGIC pitch, and treating it that way builds more trust than advocacy would.
-- MAGIC
-- MAGIC **Setup (before class):** a Genie space curated over the training dataset
-- MAGIC (`training_nic.migrated.institutions` + the published summary view). **Rehearse both
-- MAGIC questions** — the demo depends on the second failing in a *specific* way; an unrehearsed
-- MAGIC failure is either uninteresting or unrecoverable.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The script
-- MAGIC
-- MAGIC 1. **Ask the well-formed question:** *"How many institutions are there in each state?"*
-- MAGIC    Good answer arrives.
-- MAGIC 2. **Expand the generated SQL. Read it aloud.** *"That is the query I would have written."*
-- MAGIC    (Expected shape: the query in the cell below.)
-- MAGIC 3. **Ask the underspecified one:** *"Which are the biggest institutions?"* — nothing in the
-- MAGIC    data defines *biggest*.
-- MAGIC 4. **Read the answer.** It is confident and fluent.
-- MAGIC 5. **Expand the SQL.** It picked a column — perhaps assets, perhaps branch count. **Nothing
-- MAGIC    in the answer flagged the assumption.**
-- MAGIC 6. **Verify** — run your own query for the interpretation you actually meant (cell below).
-- MAGIC    Compare the two results.
-- MAGIC 7. **Draw the line:** *"Genie answered exactly what it was asked. The question was ambiguous,
-- MAGIC    and it resolved the ambiguity silently. That is the failure mode — not that it breaks, but
-- MAGIC    that it decides for you."*

-- COMMAND ----------

-- Step 2's expected shape — what a well-formed question should generate:
SELECT STATE_ABBR_NM, COUNT(*) AS institution_count
FROM training_nic.migrated.institutions
GROUP BY STATE_ABBR_NM
ORDER BY institution_count DESC;

-- COMMAND ----------

-- Step 6's verification — "biggest" under the interpretation YOU meant (by total assets):
SELECT i.NM_LGL, f.TOT_ASSETS
FROM training_nic.migrated.institutions AS i
JOIN training_nic.migrated.financials  AS f
  ON i.`#ID_RSSD` = f.`#ID_RSSD`
ORDER BY f.TOT_ASSETS DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What to highlight
-- MAGIC - Steps 4–5 are the lesson: **confidence and correctness are unrelated here.**
-- MAGIC - The verification habit: for any question that will inform a decision, **read the SQL**.
-- MAGIC - Tie it back: better-curated, better-named datasets produce better answers — their
-- MAGIC   Module 5 work is upstream of this.
-- MAGIC
-- MAGIC **Transition:** *"That is the toolkit. Let us close on where this leaves you."* → wrap-up.
