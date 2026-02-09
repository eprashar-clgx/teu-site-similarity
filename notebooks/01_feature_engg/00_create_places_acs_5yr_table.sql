-- Create a single table for ACS years: 2024
-- TODO: remove the 1yr, 5yr and declare as a variable
DECLARE year INT64 DEFAULT 2024;

EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE TABLE `teu_site_similarity.acs_5yr_%d_sumlevel_place_features` AS
  SELECT
    b01003.GEO_ID AS geo_id,
    %d AS year,

    -- Population
    b01003.B01003_E001 AS pop_%d,
    b01003.B01003_M001 AS pop_moe_%d,

    -- Households + family households
    b11001.B11001_E001 AS households_%d,
    b11001.B11001_M001 AS households_moe_%d,
    b11001.B11001_E002 AS family_households_%d,
    b11001.B11001_M002 AS family_households_moe_%d,

    -- Median income
    b19013.B19013_E001 AS median_income_%d,
    b19013.B19013_M001 AS median_income_moe_%d,

    -- Housing units
    b25001.B25001_E001 AS housing_units_%d,
    b25001.B25001_M001 AS housing_units_moe_%d,

    -- Owner occupied units
    b25003.B25003_E002 AS owner_occupied_%d,
    b25003.B25003_M002 AS owner_occupied_moe_%d,

    -- Median home value
    b25077.B25077_E001 AS median_home_value_%d,
    b25077.B25077_M001 AS median_home_value_moe_%d

  FROM `teu_site_similarity.raw_acs_5yr_%d_sumlevel_place_b01003` b01003
  LEFT JOIN `teu_site_similarity.raw_acs_5yr_%d_sumlevel_place_b11001` b11001
    ON b01003.GEO_ID = b11001.GEO_ID
  LEFT JOIN `teu_site_similarity.raw_acs_5yr_%d_sumlevel_place_b19013` b19013
    ON b01003.GEO_ID = b19013.GEO_ID
  LEFT JOIN `teu_site_similarity.raw_acs_5yr_%d_sumlevel_place_b25001` b25001
    ON b01003.GEO_ID = b25001.GEO_ID
  LEFT JOIN `teu_site_similarity.raw_acs_5yr_%d_sumlevel_place_b25003` b25003
    ON b01003.GEO_ID = b25003.GEO_ID
  LEFT JOIN `teu_site_similarity.raw_acs_5yr_%d_sumlevel_place_b25077` b25077
    ON b01003.GEO_ID = b25077.GEO_ID
""",
year, year, -- For the table name and 'year' column
year,year,year,year,year,year,year,year,year,year,year,year,year,year, -- for year in column names
year, year, year, year, year, year -- For the 6 JOINed tables
);