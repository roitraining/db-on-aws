-- Lab 2 completed: SQL Translation, Cross-Schema Joins, and Parameters
-- Run block by block in the SQL editor.

USE CATALOG training_nic;
USE SCHEMA migrated;

-- TOP -> LIMIT
SELECT * FROM institutions LIMIT 100;

-- ISNULL -> COALESCE
SELECT `#ID_RSSD`, COALESCE(CITY, 'UNKNOWN') AS CITY_CLEAN
FROM institutions LIMIT 20;

-- LEN -> LENGTH (cloud side: lengths match the visible characters)
SELECT `#ID_RSSD`, NM_LGL, LENGTH(NM_LGL) AS name_length
FROM institutions LIMIT 20;

-- same query against the on-premises copy: longer lengths -- CHAR padding
SELECT `#ID_RSSD`, NM_LGL, LENGTH(NM_LGL) AS name_length
FROM training_nic.legacy_onprem.institutions LIMIT 20;

-- datediff: END date first, no unit argument, always days
SELECT datediff('2009-07-31', '2009-07-30') AS forward,
       datediff('2009-07-30', '2009-07-31') AS backward;

-- strict casting: raises instead of coercing
-- SELECT CAST('not-a-number' AS INT);          -- errors by design
SELECT TRY_CAST('not-a-number' AS INT) AS safe_cast;

-- cross-schema join: native NIC name on the left, curated name on the right
SELECT i.STATE_ABBR_NM,
       COUNT(*)              AS institution_count,
       MAX(c.population)     AS state_population
FROM training_nic.migrated.institutions  AS i
JOIN training_nic.reference.state_population AS c
  ON i.STATE_ABBR_NM = c.state_abbr
GROUP BY i.STATE_ABBR_NM
ORDER BY institution_count DESC;

-- final parameterized report: set both widgets to type Date
SELECT i.STATE_ABBR_NM,
       COUNT(*) AS institution_count
FROM training_nic.migrated.institutions AS i
WHERE i.D_DT_START BETWEEN :start_date AND :end_date
GROUP BY i.STATE_ABBR_NM
ORDER BY institution_count DESC;
