CREATE OR REPLACE VIEW `my-bigquery-lab-492816.gus_staging.dim_region` AS

WITH unnested_national as (
  SELECT
    r.name AS region_source,
    r.id AS region_id

  FROM `my-bigquery-lab-492816.gus_raw.gdp_per_capita_lvl0_p0`,
  UNNEST(results) AS r
),
unnested_voivodeship as (
  SELECT
    r.name AS region_source,
    r.id AS region_id

  FROM `my-bigquery-lab-492816.gus_raw.gdp_per_capita_lvl2_p0`,
  UNNEST(results) AS r
),
combined AS (
  SELECT * FROM unnested_national
  UNION ALL
  SELECT * FROM unnested_voivodeship
)

Select DISTINCT
region_id,
region_source,
CASE WHEN region_source = 'POLSKA' THEN 'NATIONAL' ELSE region_source END AS region,
CASE WHEN region_source = 'POLSKA' THEN 0 ELSE 1 END AS sort_order,
CASE WHEN region_source = 'POLSKA' THEN 'Country' ELSE 'Voivodeship' END AS level_type
from combined