-- Feature set before calculating any percentiles
-- Check table name so that the percentile one is not replaced
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_features_v1` AS
SELECT
  a.geoidfq, 
  a.state_fips, 
  a.state_name, 
  a.stusps, 
  a.namelsad,
  a.pop_2024, 
  a.households_2024,
  a.median_income_2024, 
  a.median_home_value_2024,
  b.unq_parcel_count, 
  b.median_parcel_area_sq_mtr,
  ROUND(SAFE_DIVIDE(b.unq_parcel_count, (ST_AREA(a.geometry) / 1000000)), 2) AS parcel_density,
  d.unq_clips, 
  c.unq_addr_count, 
  c.condo_address_counts,
  d.median_assessed_value, 
  d.median_tax_amount,
  e.unq_growth_clips,
  ROUND(SAFE_DIVIDE(e.unq_growth_clips, d.unq_clips) * 100, 2) AS growth_clip_share,
  f.business_count,
  a.geometry
FROM `clgx-gis-app-dev-06e3.teu_site_similarity.place_acs_5yr_2024_geo_features` a
LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_parcel_features` b USING (geoidfq)
LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_address_features` c USING (geoidfq)
LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_property_features` d USING (geoidfq)
LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_growth_v2_features` e USING (geoidfq)
LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_firmographics_features` f USING (geoidfq)
-- Other population thresholds should be applied at run-time
WHERE a.median_income_2024 > 0 AND a.median_home_value_2024 > 0