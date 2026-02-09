-- Query to create acs_geo_features for places


-- Query to create parcel table from parcel universe
CREATE OR REPLACE TABLE `teu_site_similarity.stage_parcel_universe` 
CLUSTER BY fips AS
SELECT
  LEFT(fips,2) AS state_code,	
  fips,
  parcel_shape_hash,
  parcel_shape_id,
  parcel_polygon AS parcel_geometry,
  parcel_geocode,
  parcel_geocode_type,
  parcel_latitude,
  parcel_longitude,
  ROUND(parcel_shape_area,2) AS parcel_area_in_sq_mtr
FROM `clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.vw_parcel_universe`;

-- Query to create parcel features
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_site_similarity.place_parcel_features` AS
SELECT
  a.geoidfq,
  COUNT(DISTINCT b.parcel_shape_id) AS unq_parcel_count,
  AVG(b.parcel_area_in_sq_mtr) AS avg_parcel_area_sq_mtr,
  -- Calculate 100 quantiles and pick the middle one (50th percentile)
  APPROX_QUANTILES(b.parcel_area_in_sq_mtr, 100)[OFFSET(50)] AS median_parcel_area_sq_mtr,
  APPROX_QUANTILES(b.parcel_area_in_sq_mtr, 100)[OFFSET(10)] AS p10_parcel_area_sq_mtr,
  APPROX_QUANTILES(b.parcel_area_in_sq_mtr, 100)[OFFSET(90)] AS p90_parcel_area_sq_mtr,

FROM `clgx-gis-app-dev-06e3.teu_site_similarity.place_acs_geo_features` a
JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.stage_parcel_universe` b
  ON a.state_fips = b.state_code
  AND ST_CONTAINS(a.geometry, ST_GEOGPOINT(b.parcel_longitude, b.parcel_latitude))
WHERE b.parcel_area_in_sq_mtr IS NOT NULL
GROUP BY a.geoidfq;

-- Query to create address table
CREATE OR REPLACE TABLE `teu_site_similarity.stage_address_universe` 
CLUSTER BY fips AS
SELECT
  FIPS_STATE_CODE AS state_code,	
  FIPS_CODE AS fips,
  ADDRESS_ID AS address_id,
  LOCATION_LATITUDE AS address_latitude,
  LOCATION_LONGITUDE AS address_longitude,
  ADDRESS_QUALITY_SCORE AS address_qscore
FROM `clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.vw_address_connect`;

-- Query to create address features
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_site_similarity.place_address_features` AS
SELECT
  a.geoidfq,
  COUNT(DISTINCT b.address_id) AS unq_addr_count,
FROM `clgx-gis-app-dev-06e3.teu_site_similarity.place_acs_geo_features` a
JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.stage_address_universe` b
  ON a.state_fips = b.state_code
  AND ST_CONTAINS(a.geometry, ST_GEOGPOINT(b.address_longitude, b.address_latitude))
GROUP BY a.geoidfq;

-- Query to access property_v3
CREATE OR REPLACE TABLE `teu_site_similarity.stage_property_v3` 
CLUSTER BY fips AS
SELECT 
  clip,
  LEFT(fips_code,2) AS state_code,
  fips_code AS fips,
  -- number of buildings -- what purpose will this serve?
  assessed_total_value,
  market_total_value,
  total_tax_amount,
  parcel_level_latitude AS property_latitude,
  parcel_level_longitude AS property_longitude
FROM `clgx-idap-bigquery-prd-a990.view_ent_property_products.property_v3`;

-- Query to create property features
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_site_similarity.place_property_features` AS
SELECT
  a.geoidfq,
  COUNT(DISTINCT b.clip) AS unq_clips,
  ROUND(AVG(b.assessed_total_value),2) AS avg_assessed_val,
  APPROX_QUANTILES(b.assessed_total_value, 100)[OFFSET(50)] AS median_assessed_value,
  ROUND(AVG(b.market_total_value),2) AS avg_market_val,
  APPROX_QUANTILES(b.market_total_value, 100)[OFFSET(50)] AS median_market_value,
  ROUND(AVG(b.total_tax_amount),2) AS avg_tax_amount,
  APPROX_QUANTILES(b.total_tax_amount, 100)[OFFSET(50)] AS median_tax_amount,

FROM `clgx-gis-app-dev-06e3.teu_site_similarity.place_acs_geo_features` a
JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.stage_property_v3` b
  ON a.state_fips = b.state_code
  AND ST_CONTAINS(a.geometry, ST_GEOGPOINT(b.property_longitude, b.property_latitude))
GROUP BY a.geoidfq;


-- Query to create growth intelligence staging table
CREATE OR REPLACE TABLE `teu_site_similarity.stage_growth_intel_v2`
CLUSTER BY fips AS 
  SELECT
  LEFT(b.fips,2) AS state_code,
  b.fips,
  a.puid,
  a.growth_stage,
  b.clipLatitude,
  b.clipLongitude
FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_enriched.vw_edr_panoramiq_growth_indicators_v2` a
LEFT JOIN `clgx-idap-bigquery-prd-a990.edr_ent_property_parcel_polygons.vw_property_parcelpolygon` b
ON CAST(a.puid AS STRING) = b.clip
WHERE ST_GEOMETRYTYPE(b.geometry) IN ('ST_Polygon','ST_MultiPolygon');

-- Query to create growth features for places
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_site_similarity.place_growth_v2_features` AS
WITH unique_growth_puids AS (
  -- First, get exactly one row per puid
  SELECT 
    puid,
    state_code,
    -- Using MAX handles cases where a puid might have conflicting stages
    MAX(LOWER(growth_stage)) as growth_stage,
    -- We take the average or first coordinate for the spatial join
    ANY_VALUE(clipLongitude) as clipLongitude,
    ANY_VALUE(clipLatitude) as clipLatitude
  FROM `clgx-gis-app-dev-06e3.teu_site_similarity.stage_growth_intel_v2`
  GROUP BY puid, state_code
)
SELECT
  a.geoidfq,
  COUNT(b.puid) AS unq_growth_clips,
  -- Now SUM logic will only see each puid once
  COUNTIF(b.growth_stage = 'early growth') AS early_growth_puids,
  COUNTIF(b.growth_stage = 'ongoing growth') AS ongoing_growth_puids,
  COUNTIF(b.growth_stage = 'recently completed') AS recently_completed_puids
FROM `clgx-gis-app-dev-06e3.teu_site_similarity.place_acs_geo_features` a
JOIN unique_growth_puids b
  ON a.state_fips = b.state_code
  AND ST_CONTAINS(a.geometry, ST_GEOGPOINT(CAST(b.clipLongitude AS FLOAT64), CAST(b.clipLatitude AS FLOAT64)))
GROUP BY a.geoidfq;