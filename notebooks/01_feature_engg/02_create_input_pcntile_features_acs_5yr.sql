-- Final feature set: National vs. State Percentiles (Scale 0-100)
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_features_for_pctile_scoring_v1` AS
WITH base_features AS (
  SELECT
    a.geoidfq, a.state_fips, a.state_name, a.stusps, a.namelsad,
    a.pop_2024, a.households_2024,
    a.median_income_2024, a.median_home_value_2024,
    b.unq_parcel_count, b.median_parcel_area_sq_mtr,
    ROUND(SAFE_DIVIDE(b.unq_parcel_count, (ST_AREA(a.geometry) / 1000000)), 2) AS parcel_density,
    d.unq_clips, c.unq_addr_count, c.condo_address_counts,
    d.median_assessed_value, d.median_tax_amount,
    e.unq_growth_clips,
    ROUND(SAFE_DIVIDE(e.unq_growth_clips, d.unq_clips) * 100, 2) AS growth_clip_share,
    f.business_count
  FROM `clgx-gis-app-dev-06e3.teu_site_similarity.place_acs_5yr_2024_geo_features` a
  LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_parcel_features` b USING (geoidfq)
  LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_address_features` c USING (geoidfq)
  LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_property_features` d USING (geoidfq)
  LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_growth_v2_features` e USING (geoidfq)
  LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.acs_5yr_place_firmographics_features` f USING (geoidfq)
  WHERE a.pop_2024 >= 500 AND a.median_income_2024 > 0 AND a.median_home_value_2024 > 0
)
SELECT
  *,
  -- NATIONAL PERCENTILES (Scale 0-100)
  ROUND(PERCENT_RANK() OVER (ORDER BY pop_2024) * 100, 2) AS pop_2024_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY households_2024) * 100, 2) AS households_2024_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY median_income_2024) * 100, 2) AS median_income_2024_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY median_home_value_2024) * 100, 2) AS median_home_value_2024_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY unq_parcel_count) * 100, 2) AS unq_parcel_count_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY median_parcel_area_sq_mtr) * 100, 2) AS median_parcel_area_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY parcel_density) * 100, 2) AS parcel_density_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY unq_clips) * 100, 2) AS unq_clips_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY unq_addr_count) * 100, 2) AS unq_addr_count_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY condo_address_counts) * 100, 2) AS condo_address_counts_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY median_assessed_value) * 100, 2) AS median_assessed_value_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY median_tax_amount) * 100, 2) AS median_tax_amount_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY unq_growth_clips) * 100, 2) AS unq_growth_clips_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY growth_clip_share) * 100, 2) AS growth_clip_share_pct,
  ROUND(PERCENT_RANK() OVER (ORDER BY business_count) * 100, 2) AS business_count_pct,

  -- STATE-LEVEL PERCENTILES (Scale 0-100)
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY pop_2024) * 100, 2) AS pop_2024_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY households_2024) * 100, 2) AS households_2024_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY median_income_2024) * 100, 2) AS median_income_2024_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY median_home_value_2024) * 100, 2) AS median_home_value_2024_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY unq_parcel_count) * 100, 2) AS unq_parcel_count_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY median_parcel_area_sq_mtr) * 100, 2) AS median_parcel_area_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY parcel_density) * 100, 2) AS parcel_density_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY unq_clips) * 100, 2) AS unq_clips_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY unq_addr_count) * 100, 2) AS unq_addr_count_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY condo_address_counts) * 100, 2) AS condo_address_counts_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY median_assessed_value) * 100, 2) AS median_assessed_value_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY median_tax_amount) * 100, 2) AS median_tax_amount_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY unq_growth_clips) * 100, 2) AS unq_growth_clips_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY growth_clip_share) * 100, 2) AS growth_clip_share_state_pct,
  ROUND(PERCENT_RANK() OVER (PARTITION BY state_fips ORDER BY business_count) * 100, 2) AS business_count_state_pct
FROM base_features;