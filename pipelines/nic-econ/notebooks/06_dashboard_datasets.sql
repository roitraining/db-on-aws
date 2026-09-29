-- AI/BI dashboard datasets. Create each as a dataset in the dashboard's "Data" tab.
-- Replace nic_econ with your catalog if you changed it.

-- ============================================================
-- ds_states  (landing page + trends)
-- ============================================================
SELECT year, state_abbr, state_name, population,
       institutions_active, branches_active, institutions_per_100k, branches_per_100k,
       institutions_opened, institutions_closed,
       unemployment_rate, median_hh_income_popwtd, poverty_rate,
       deposits_thousands, deposits_per_capita, banking_orgs, deposit_hhi
FROM nic_econ.gold.state_metrics;

-- ============================================================
-- ds_counties  (drill-down; filtered by the same State filter)
-- ============================================================
SELECT year, state_abbr, county_fips, county_name, msa_title, is_metro, population,
       institutions_active, branches_active, institutions_per_100k, branches_per_100k,
       unemployment_rate, median_hh_income, median_home_value, poverty_rate,
       deposits_per_capita, banking_orgs, deposit_hhi
FROM nic_econ.gold.county_metrics;

-- ============================================================
-- ds_metros
-- ============================================================
SELECT year, msa_code, msa_title, is_metro, county_count, population,
       institutions_active, branches_active, institutions_per_100k, branches_per_100k,
       unemployment_rate, median_hh_income_popwtd, deposits_per_capita, deposit_hhi
FROM nic_econ.gold.msa_metrics;

-- ============================================================
-- ds_macro
-- ============================================================
SELECT * FROM nic_econ.gold.national_macro_monthly ORDER BY month;

-- ============================================================
-- ds_weather_spending   parameter :state (string, default 'ALL')
-- spend is a fraction vs Jan 2020, shown here in percentage points
-- ============================================================
SELECT temp_bucket, rain_day,
       SUM(days) AS days,
       ROUND(SUM(spend_sum) / SUM(days) * 100, 2) AS avg_spend_vs_jan2020_pct
FROM nic_econ.gold.spending_by_weather
WHERE :state = 'ALL' OR state_abbr = :state
GROUP BY temp_bucket, rain_day
ORDER BY temp_bucket, rain_day;

-- ============================================================
-- ds_game_summary   parameter :team (string, default 'CHI')
-- ============================================================
SELECT result,
       COUNT(*) AS games,
       ROUND(AVG(next_day_change) * 100, 2) AS avg_next_day_change_pts,
       ROUND(AVG(week_change) * 100, 2) AS avg_week_change_pts
FROM nic_econ.gold.game_result_spending
WHERE team = :team
GROUP BY result
ORDER BY result;

-- ============================================================
-- ds_game_detail   parameter :team (string, default 'CHI')
-- ============================================================
SELECT date, season, week, opponent, is_home, result, margin,
       ROUND(spend_day_before * 100, 2) AS spend_day_before_pct,
       ROUND(spend_day_after * 100, 2) AS spend_day_after_pct,
       ROUND(week_change * 100, 2) AS week_change_pts
FROM nic_econ.gold.game_result_spending
WHERE team = :team
ORDER BY date DESC;
