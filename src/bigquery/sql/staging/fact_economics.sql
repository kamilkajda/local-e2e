CREATE OR REPLACE VIEW `my-bigquery-lab-492816.gus_staging.fact_economics` AS
SELECT
variable_id as metric_id,
r.id as region_id,
v.year as year,
v.val as value

FROM `my-bigquery-lab-492816.gus_raw.*`,
UNNEST(results) as r,
UNNEST(r.values) as v