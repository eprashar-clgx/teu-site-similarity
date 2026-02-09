-- Loop over all tables from ACS_TABLE_DOCS and create BQ tables
DECLARE year INT64 DEFAULT 2024; -- 2024 5 year data is out! 
DECLARE geo STRING DEFAULT 'sumlevel_place';
DECLARE sumlevel STRING DEFAULT 'place';
DECLARE bq_projectname STRING DEFAULT 'clgx-gis-app-dev-06e3';
DECLARE bq_datasetname STRING DEFAULT 'teu_site_similarity';

--DECLARE table_id STRING;
DECLARE gcs_prefix STRING DEFAULT 'gs://geospatial-projects/teu_site_similarity/acs/5yr';
DECLARE gcs_uri STRING;
DECLARE target_table STRING;

For record IN (
    SELECT table_id FROM UNNEST([
        'B01003', -- Total population
        'B07003', -- Geographical Mobility
        'B11001', -- Household Composition
        'B19001', -- Household Income Distribution
        'B19013', -- Median Household Income
        'B25001', -- Housing Units
        'B25002', -- Occupancy Status
        'B25003', -- Tenure (Owner vs Renter)
        'B25024', -- Units in Structure
        'B25077', -- Median Home Value
        'C17002' -- Ratio of Income to Poverty Level
    ]) AS table_id
)
    DO
    SET gcs_uri = FORMAT('%s/%s/%s_place_state_all_%d.csv',
    gcs_prefix,
    geo,
    record.table_id,
    year
    );

    SET target_table = FORMAT(
        '`%s.%s.raw_acs_5yr_%d_%s_%s`',
        bq_projectname,
        bq_datasetname,
        year,
        geo,
        LOWER(record.table_id)
        );
    
    EXECUTE IMMEDIATE FORMAT("""
        LOAD DATA OVERWRITE %s
        FROM FILES (
            format='CSV',
            uris =['%s'],
            skip_leading_rows =1
            )
        """, target_table, gcs_uri);
END FOR;