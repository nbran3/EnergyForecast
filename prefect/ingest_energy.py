import requests
import os
import pandas as pd
import pandas_gbq as gbq
from dotenv import load_dotenv
import openpyxl

load_dotenv()

project_id = os.getenv("project_id")
dataset_id = os.getenv("dataset")

CSV_URL = "https://www.eia.gov/totalenergy/data/monthly/query/mer_data_excel.asp?table=T02.01"

def download_file(url, filename):
    response = requests.get(url)
    response.raise_for_status() 

    with open(filename, 'wb') as file:
        file.write(response.content)

    print(f"File downloaded as {filename}")


def run_energy_pipeline():
    try:
        download_file(CSV_URL, "energy.xlsx")
        df = pd.read_excel(r"energy.xlsx", header=None)
        new_column_headers = df.iloc[10]
        df = df[12:]
        df.columns = new_column_headers
        df = df.reset_index(drop=True)
        table_id = f'{dataset_id}.energy'  
        print(f'Uploading Enegry to BigQuery table {table_id}...')
        gbq.to_gbq(df, table_id, project_id=project_id, chunksize=10000, if_exists='replace')
        print(f"Uploaded Energy to BigQuery Successfully")
        os.remove("energy.xlsx")
    except Exception as e:
            print(f"Failed to upload Enegry to BigQuery: {e}")


