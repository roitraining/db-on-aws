-- Lab 6 completed: the four dashboard dataset queries (Data tab > Create from SQL).

-- dataset 1: the published view with charter codes decoded to names (page 1 charter chart)
SELECT COALESCE(ct.charter_type, CONCAT('Code ', s.CHTR_TYPE_CD)) AS charter_type,
       s.start_month, s.institution_count, s.distinct_cities
FROM training_nic.analyst.institution_summary_published s
LEFT JOIN training_nic.reference.charter_types ct
  ON s.CHTR_TYPE_CD = ct.chtr_type_cd;

-- dataset 2: state-growth spine (state bar + growth line share this, enabling cross-filtering)
-- one dataset for both the state chart and the growth line: per state, per month,
-- new institutions plus a carry-forward running total. The "spine" pattern (cross
-- join states x months) keeps the cumulative correct when states are filtered or
-- summed -- months with no openings still carry the total forward.
WITH per AS (
  SELECT STATE_ABBR_NM, date_trunc('month', D_DT_START) AS start_month, COUNT(*) AS n
  FROM training_nic.migrated.institutions
  WHERE STATE_ABBR_NM IS NOT NULL
AND TRIM(STATE_ABBR_NM) NOT IN ('', '0')   -- code '0' marks foreign entities
  GROUP BY 1, 2),
spine AS (
  SELECT s.STATE_ABBR_NM, m.start_month
  FROM (SELECT DISTINCT STATE_ABBR_NM FROM per) s
  CROSS JOIN (SELECT explode(sequence((SELECT MIN(start_month) FROM per),
   (SELECT MAX(start_month) FROM per),
   INTERVAL 1 MONTH)) AS start_month) m)
SELECT sp.STATE_ABBR_NM,
   sp.start_month,
   COALESCE(p.n, 0) AS new_institutions,
   SUM(COALESCE(p.n, 0)) OVER (PARTITION BY sp.STATE_ABBR_NM
ORDER BY sp.start_month) AS total_institutions
FROM spine sp
LEFT JOIN per p
  ON p.STATE_ABBR_NM = sp.STATE_ABBR_NM AND p.start_month = sp.start_month;

-- dataset 3: one row per validation attempt (Migration Health bar chart)
SELECT run_ts AS run_attempt,
       SUM(CASE WHEN passed THEN 1 ELSE 0 END)     AS checks_passed,
       SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS checks_failed
FROM training_nic.analyst.validation_runs
GROUP BY run_ts;

-- dataset 4: latest attempt's failures (Migration Health counter tile)
SELECT COUNT(*) AS failed_checks
FROM training_nic.analyst.validation_runs
WHERE NOT passed
  AND run_ts = (SELECT MAX(run_ts) FROM training_nic.analyst.validation_runs);


-- dataset 6: live alert status (Migration Health table -- same conditions the alerts run)
SELECT 'summary row count below 650' AS alert,
       CASE WHEN (SELECT COUNT(*) FROM training_nic.analyst.institution_summary) < 1900
            THEN 'TRIGGERED' ELSE 'OK' END AS status
UNION ALL
SELECT 'validation failures in latest run',
       CASE WHEN (SELECT COUNT(*) FROM training_nic.analyst.validation_runs
                  WHERE NOT passed
                    AND run_ts = (SELECT MAX(run_ts) FROM training_nic.analyst.validation_runs)) > 0
            THEN 'TRIGGERED' ELSE 'OK' END;
