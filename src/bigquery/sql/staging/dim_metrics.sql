CREATE OR REPLACE VIEW `my-bigquery-lab-492816.gus_staging.dim_metrics` AS

SELECT * FROM UNNEST([
  STRUCT(64428 AS metric_id, 'monthly_gross_wages' AS metric_name, 'Average monthly gross wages and salaries' AS description),
  STRUCT(60270 AS metric_id, 'unemployment_rate_registered' AS metric_name, 'Registered unemployment rate (end of year)' AS description),
  STRUCT(216968 AS metric_id, 'household_disposable_income_per_capita' AS metric_name, 'Average monthly household disposable income per capita' AS description),
  STRUCT(7737 AS metric_id, 'household_expenditures_per_capita' AS metric_name, 'Average monthly household expenditures per capita' AS description),
  STRUCT(458421 AS metric_id, 'gdp_per_capita' AS metric_name, 'Gross Domestic Product (GDP) per capita' AS description),
  STRUCT(458271 AS metric_id, 'total_gdp' AS metric_name, 'Gross Domestic Product (GDP) - current prices (Total)' AS description),
  STRUCT(60520 AS metric_id, 'investment_outlays_national_economy_per_capita' AS metric_name, 'Investment outlays in national economy per capita' AS description),
  STRUCT(60530 AS metric_id, 'entities_national_economy_per_10k' AS metric_name, 'Entities of national economy per 10k population' AS description),
  STRUCT(747060 AS metric_id, 'dwellings_completed_per_1k' AS metric_name, 'Dwellings completed per 1000 population' AS description),
  STRUCT(633692 AS metric_id, 'residential_price_per_m2' AS metric_name, 'Price of 1 m2 of usable floor area of a residential building' AS description),
  STRUCT(60508 AS metric_id, 'voivodship_budget_revenue_per_capita' AS metric_name, 'Total revenue of voivodship budgets per capita' AS description),
  STRUCT(60518 AS metric_id, 'voivodship_budget_expenditure_per_capita' AS metric_name, 'Current expenditure of voivodship budgets per capita' AS description),
  STRUCT(72305 AS metric_id, 'population_total' AS metric_name, 'Total population (actual place of residence)' AS description),
  STRUCT(748601 AS metric_id, 'dwellings_completed_total' AS metric_name, 'Dwellings completed (Total count)' AS description),
  STRUCT(633101 AS metric_id, 'dwellings_sold_total' AS metric_name, 'Sold residential premises (Total count)' AS description),
  STRUCT(633617 AS metric_id, 'dwellings_sold_market_transactions' AS metric_name, 'Sold residential premises (Market transactions count)' AS description)
])