-- Lab 6 completed: the four dashboard dataset queries (Data tab > Create from SQL).

-- dataset 1: the published view (page 1 bar + line charts, start_month filter)
SELECT CHTR_TYPE_CD, start_month, institution_count, distinct_cities
FROM training_nic.analyst.institution_summary_published;

-- dataset 2: the Lab 2 report, reborn as a dashboard dataset (page 1 state bar chart)
SELECT i.STATE_ABBR_NM,
       COUNT(*)          AS institution_count,
       MAX(c.population) AS state_population
FROM training_nic.migrated.institutions i
JOIN training_nic.reference.state_population c
  ON i.STATE_ABBR_NM = c.state_abbr
GROUP BY i.STATE_ABBR_NM;

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

-- dataset 5: cumulative growth (page 1 line chart -- per-month counts are noise)
SELECT start_month,
       SUM(institution_count) AS new_institutions,
       SUM(SUM(institution_count)) OVER (ORDER BY start_month) AS total_institutions
FROM training_nic.analyst.institution_summary_published
GROUP BY start_month;

-- dataset 6: live alert status (Migration Health table -- same conditions the alerts run)
SELECT 'summary row count below 1,900' AS alert,
       CASE WHEN (SELECT COUNT(*) FROM training_nic.analyst.institution_summary) < 1900
            THEN 'TRIGGERED' ELSE 'OK' END AS status
UNION ALL
SELECT 'validation failures in latest run',
       CASE WHEN (SELECT COUNT(*) FROM training_nic.analyst.validation_runs
                  WHERE NOT passed
                    AND run_ts = (SELECT MAX(run_ts) FROM training_nic.analyst.validation_runs)) > 0
            THEN 'TRIGGERED' ELSE 'OK' END;
