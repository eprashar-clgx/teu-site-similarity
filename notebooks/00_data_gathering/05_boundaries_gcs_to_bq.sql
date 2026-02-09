-- Creating table for place boundaries
LOAD DATA OVERWRITE `teu_site_similarity.boundaries_places_2024`
FROM FILES (
  format='PARQUET',
  uris = ['gs://geospatial-projects/teu_site_similarity/boundaries/place_boundaries_2024.parquet']
);

-- Creating table for state boundaries
CREATE TABLE
  teu_site_similarity.boundaries_states_2020 AS
SELECT
  * EXCEPT (st_gnis,geometry_geojson,geometry), 
  ST_GEOGFROMTEXT(geometry) AS geometry -- Convert the string to GEOGRAPHY
FROM
  `clgx-idap-bigquery-prd-a990.edr_ent_property_geospatial_admin_boundaries.geospatial_admin_boundaries_state`;
