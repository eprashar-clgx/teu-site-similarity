-- Loop over all V1 RELEVANT tables from ACS_TABLE_DOCS and create BQ tables
DECLARE bq_projectname STRING DEFAULT 'clgx-gis-app-dev-06e3';
DECLARE bq_datasetname STRING DEFAULT 'teu_site_similarity';
DECLARE years ARRAY<INT64> DEFAULT [2022, 2023, 2024];
DECLARE geos ARRAY<STRING> DEFAULT ['sumlevel_place'];

DECLARE tables ARRAY<STRING> DEFAULT [
    'B01003', 'B11001', 'B19013', 'B25001', 'B25003', 'B25077'
];
DECLARE gcs_prefix STRING DEFAULT 'gs://geospatial-projects/teu_site_similarity/acs/1yr';

DECLARE gcs_uri STRING;
DECLARE target_table STRING;

-- Use ALIASES (val) in the subqueries to make them accessible
FOR yr IN (SELECT val FROM UNNEST(years) AS val) DO
    FOR geo IN (SELECT val FROM UNNEST(geos) AS val) DO
        FOR table_id IN (SELECT val FROM UNNEST(tables) AS val) DO
            
            -- Access the values using .val
            SET gcs_uri = FORMAT('%s/%s/%s_place_state_all_%d.csv', gcs_prefix, geo.val, table_id.val, yr.val);
            
            SET target_table = FORMAT('`%s.%s.raw_acs_1yr_%d_%s_%s`', 
                bq_projectname, 
                bq_datasetname,
                yr.val,
                geo.val,
                LOWER(table_id.val));

            EXECUTE IMMEDIATE FORMAT("""
                LOAD DATA OVERWRITE %s
                FROM FILES (
                    FORMAT = 'CSV',
                    URIS = ['%s'],
                    skip_leading_rows = 1
                )
                """ , target_table, gcs_uri);
        END FOR;
    END FOR;
END FOR;