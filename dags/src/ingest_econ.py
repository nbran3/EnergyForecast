import requests
import pandas as pd
import pandas_gbq as gbq
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("1API_KEY")


series_list = ['UNRATE', 'CPIAUCSL', 'FEDFUNDS', 'INDPRO', 'MCUMFN']

project_id = 'energy-461918'
dataset_id = 'staging' 

def download_econ_data(series):
    base_econ_url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series}&api_key={api_key}&file_type=json"
    )
    response = requests.get(base_econ_url)
    response.raise_for_status()
    data = response.json()

    clean_data = []
    for obs in data['observations']:
        cols = {k: v for k, v in obs.items() if k not in ['realtime_start', 'realtime_end']}
        cols['series_id'] = series
        clean_data.append(cols)

    df = pd.DataFrame(clean_data)
    return df

def upload_to_bq():
    for i in series_list:
        
        try:
            df = download_econ_data(i)
            table_id = f'{dataset_id}.{i}'  
            print(f'Uploading {i} to BigQuery table {table_id}...')
            gbq.to_gbq(df, table_id, project_id=project_id, chunksize=10000, if_exists='replace')
        
        except Exception as e:
            print(f"Failed to upload {i} to BigQuery: {e}")

upload_to_bq()