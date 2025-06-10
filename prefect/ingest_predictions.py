import pandas as pd
import pandas_gbq as gbq
from dotenv import load_dotenv
import os

load_dotenv()
project_id = os.getenv("project_id")
dataset_id = os.getenv("finalbq_table")

file = ['arima_results', 'prophet_results', 'xgboost_results', 'sarima_results']


def run_preds_pipeline():
    try:
        for i in file:
            if os.path.exists(f"{i}.csv"):
                df = pd.read_csv(f"{i}.csv")
                table_id = f'{dataset_id}.{i}'
                print(f'Uploading {i} to BigQuery table {table_id}...')
                gbq.to_gbq(df, table_id, project_id=project_id, chunksize=10000, if_exists='replace')
                print(f"Uploaded Predictions to BigQuery Successfully")
                os.remove(f"{i}.csv")
    
    except Exception as e:
        print(f"Failed to upload Predictions to BigQuery: {e}")

run_preds_pipeline()
    