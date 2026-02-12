from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely import wkt
from google.cloud import bigquery
import pandas_gbq
import os
import uuid

# Function to find project root

def find_project_root(start:Path | None=None) -> Path:
    start = start or Path.cwd()
    for p in [start, *start.parents]:
        # print(p)
        if (p / 'pyproject.toml').exists() or (p / '.git').exists() or (p / 'data').exists():
            return p
    raise FileNotFoundError('Could not find root directory')

# Define constants to load data
# TODO: store these in a separate config.py file and load from there
PROJECT = 'clgx-gis-app-dev-06e3'
DATASET = 'teu_site_similarity'
TABLE = 'acs_5yr_place_features_v1'
DATA_DIR = find_project_root() / "data" / "intermediate" / "features"

# Function to import data from a BQ table
# Function needs authentication to gcloud before function call
def load_data_from_bq(
    project:str,
    dataset:str,
    table:str,
    save:bool=True,
    data_dir:Path=DATA_DIR) -> gpd.GeoDataFrame:
    '''
    Given a project, dataset and a table, load and return data in a geopandas dataframe
    '''
    client = bigquery.Client(project=project)
    query = f"SELECT * FROM `{project}.{dataset}.{table}`"
    
    # Load to pandas
    df = client.query(query).to_dataframe()
    
    # Load geometry object
    if 'geometry' in df.columns:
        df['geometry'] = df['geometry'].apply(wkt.loads)
    else:
        raise ValueError("Geometry column not found in input dataframe")
    
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    print(f'The crs of the dataframe is {gdf.crs}')
    print(f'Loaded dataframe has shape: {gdf.shape}')
    if save:
        if not data_dir:
            raise ValueError("Geometry not found in input dataframe.")
        # Ensure directory exists
        os.makedirs(data_dir, exist_ok=True)
        file_path = os.path.join(data_dir, f'{table}.parquet')
        gdf.to_parquet(file_path)
        print(f'Data saved to: {file_path}!')
    return gdf