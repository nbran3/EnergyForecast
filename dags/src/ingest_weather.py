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



def upload_to_gcs():
    for i in datasets:
        try:
            download_weather_data(f"https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/national/time-series/110/{i}/1/0/1973-2025/data.json", f"{i}.json")
            if os.path.exists(f"{i}.json"):
                df = pd.read_json(f"{i}.json")
                df = df.drop('description', axis=1)
                df = df.drop(['title','missing','units'])
                df['data'] = df['data'].apply(lambda x: x['value'])
                table_id = f'{dataset_id}.{i}'  
                print(f'Uploading {i} to BigQuery table {table_id}...')
                gbq.to_gbq(df, table_id, project_id=project_id, chunksize=10000, if_exists='replace')
                print(f"Uploaded {i} to BigQuery Successfully")
                os.remove(f"{i}.json")
        except Exception as e:
            print(f"Failed to upload {i} to BigQuery: {e}")
            
            
upload_to_gcs()
