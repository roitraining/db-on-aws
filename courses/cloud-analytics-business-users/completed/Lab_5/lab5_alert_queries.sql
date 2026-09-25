-- Lab 5 completed: the two alert queries.
-- Each is authored INSIDE its alert editor (alerts cannot reuse saved queries).

-- Alert 1: lab5_row_count_alert
-- Condition: First row of row_count < 650   (table holds exactly 730 rows)
SELECT COUNT(*) AS row_count
FROM training_nic.analyst.institution_summary;

-- Alert 2: lab5_validation_failures_alert
-- Condition: First row of failed_checks > 0  (goes TRIGGERED while the migration is broken)
SELECT COUNT(*) AS failed_checks
FROM training_nic.analyst.validation_runs
WHERE NOT passed
  AND run_ts = (SELECT MAX(run_ts) FROM training_nic.analyst.validation_runs);
