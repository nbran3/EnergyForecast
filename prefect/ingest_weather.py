import requests
import pandas as pd
import pandas_gbq as gbq
from dotenv import load_dotenv
import os

load_dotenv()

project_id = os.getenv("project_id")
dataset_id = os.getenv("dataset")

datasets = ['tavg', 'cdd', 'hdd']

def download_weather_data(url, filename):
    
    response = requests.get(url)
    response.raise_for_status()

    with open(filename, 'wb') as file:
        file.write(response.content)

    print(f"File downloaded as {filename}")

def run_weather_pipeline():
    for i in datasets:
        try:
            download_weather_data(f"https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/national/time-series/110/{i}/1/0/1973-2025/data.csv", f"{i}.csv")
            if os.path.exists(f"{i}.csv"):
                df = pd.read_csv(f"{i}.csv", skiprows=3)
                table_id = f'{dataset_id}.{i}'  
                print(f'Uploading {i} to BigQuery table {table_id}...')
                gbq.to_gbq(df, table_id, project_id=project_id, chunksize=10000, if_exists='replace')
                print(f"Uploaded {i} to BigQuery Successfully")
                os.remove(f"{i}.csv")
        except Exception as e:
            print(f"Failed to upload {i} to BigQuery: {e}")
            
run_weather_pipeline()          