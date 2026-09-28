# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3 Demo: A First Taste of Python
# MAGIC
# MAGIC Orientation, not a tutorial — tomorrow morning is the real thing. This is every Python
# MAGIC concept Day 2 assumes, in one runnable page. Serverless compute; run along in your own
# MAGIC account, or come back to it tonight.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 — variables and f-strings
# MAGIC A variable is a name for a value. An **f-string** builds text with values dropped in —
# MAGIC the Python habit you will use constantly for table names.

# COMMAND ----------

catalog = "training_nic"
table = f"{catalog}.migrated.institutions"

print(table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 — lists and loops
# MAGIC A list holds several values; a `for` loop does something with each one. Indentation is
# MAGIC not decoration — the indented lines ARE the loop body.

# COMMAND ----------

states = ["CA", "NY", "TX"]

for state in states:
    print(f"checking {state}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 — functions
# MAGIC `def` names a recipe so you can reuse it. Arguments in, `return` out.

# COMMAND ----------

def describe(name, count):
    return f"{name}: {count:,} rows"

print(describe("institutions", 61699))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 — you already read PySpark this morning
# MAGIC The lazy-evaluation demo was Python. Same shape: load a table, chain steps, count.
# MAGIC SQL people read this fine — `filter` is WHERE, the dot chains like a pipeline.

# COMMAND ----------

from pyspark.sql import functions as F

ca_count = spark.table(table).filter(F.col("STATE_ABBR_NM") == "CA").count()
print(f"CA institutions: {ca_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 — the payoff: one loop, many tables
# MAGIC This afternoon you ran validation checks against ONE table by hand. This is the same
# MAGIC row-count check against a *list* of tables — three lines. Twenty tables would be the
# MAGIC same three lines. **That is why Python exists in your toolkit**, and it is exactly
# MAGIC what tomorrow morning builds toward.

# COMMAND ----------

for t in ["institutions", "financials"]:
    n = spark.table(f"training_nic.migrated.{t}").count()
    print(describe(t, n))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6 — and SQL never left
# MAGIC A `%sql` cell in the same notebook, against the same data. You mix them freely —
# MAGIC each cell uses whichever language fits it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS institutions FROM training_nic.migrated.institutions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Free learning material
# MAGIC
# MAGIC | Resource | What it is |
# MAGIC |---|---|
# MAGIC | [The official Python tutorial](https://docs.python.org/3/tutorial/) | The canonical reference — chapters 3–5 cover everything on this page |
# MAGIC | [Kaggle Learn: Python](https://www.kaggle.com/learn/python) | Free, hands-on, in-browser exercises — the best "do it, don't read it" option |
# MAGIC | [PySpark Getting Started](https://spark.apache.org/docs/latest/api/python/getting_started/index.html) | Official Apache Spark intro to the DataFrame API |
# MAGIC | [PySpark basics on Databricks](https://docs.databricks.com/aws/en/pyspark/basics) | The same API, documented in the environment you are actually using |
# MAGIC | [Databricks Academy](https://www.databricks.com/learn/training/home) | Free self-paced Databricks courses (account sign-up required) |
# MAGIC
# MAGIC If you do one thing before tomorrow: the Kaggle course's first two lessons.
