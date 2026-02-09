-- Loop over all tables from ACS_TABLE_DOCS and create BQ tables
DECLARE year INT64 DEFAULT 2023;
DECLARE sumlevel STRING DEFAULT 'block_group';
DECLARE bq_projectname STRING DEFAULT 'clgx-gis-app-dev-06e3';
DECLARE bq_datasetname STRING DEFAULT 'teu_site_similarity';

--DECLARE table_id STRING;
DECLARE gcs_uri STRING;
DECLARE target_table STRING;

For record IN (
    SELECT table_id FROM UNNEST([
        'B01003', -- Total population
        'B07003', -- Geographical Mobility
        'B11001', -- Household Composition
        'B19001', -- Household Income Distribution
        'B19013', -- Median Household Income
        'B25002', -- Occupancy Status
        'B25003', -- Tenure (Owner vs Renter)
        'B25024', -- Units in Structure
        'B25077', -- Median Home Value
        'C17002' -- Ratio of Income to Poverty Level
    ]) AS table_id
)
    DO
    SET gcs_uri = FORMAT('gs://geospatial-projects/teu_site_similarity/acs/%s/%s_%s_state_all_%d.csv',
    sumlevel,
    record.table_id,
    sumlevel,
    year
    );

    SET target_table = FORMAT(
        '`%s.%s.raw_acs_%d_%s_%s`',
        bq_projectname,
        bq_datasetname,
        year,
        sumlevel,
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