-- Creating feature set for percentile scoring of places
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_site_similarity.place_all_features_for_pctile_scoring` AS
SELECT
  a.*,
  b.unq_parcel_count,
  b.avg_parcel_area_sq_mtr,
  b.median_parcel_area_sq_mtr,
  b.p10_parcel_area_sq_mtr,
  b.p90_parcel_area_sq_mtr,
  c.unq_addr_count,
  d.unq_clips	avg_assessed_val,
  d.median_assessed_value,
  d.avg_market_val,
  d.median_market_value,
  d.avg_tax_amount,
  d.median_tax_amount,
  e.unq_growth_clips,
  e.early_growth_puids AS early_growth_clips,
  e.ongoing_growth_puids AS ongoing_growth_clips,
  e.recently_completed_puids AS recently_completed_clips
FROM `clgx-gis-app-dev-06e3.teu_site_similarity.place_acs_geo_features` a
LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.place_parcel_features`b USING (geoidfq)
LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.place_address_features`c USING (geoidfq)
LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.place_property_features`d USING (geoidfq)
LEFT JOIN `clgx-gis-app-dev-06e3.teu_site_similarity.place_growth_v2_features`e USING (geoidfq)

-- 