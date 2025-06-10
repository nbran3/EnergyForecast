import pandas as pd
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

load_dotenv()


project_id = os.getenv("project_id")
table = os.getenv("finalbq_table")
dataset = os.getenv("finalbq_dataset")


query = f"""
SELECT *
FROM `{project_id}.{table}.{dataset}`
"""

def fetch_data(query: str) -> pd.DataFrame:
    client = bigquery.Client()
    return client.query(query).to_dataframe()

def prepare_prophet_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[["Date", "total primary energy consumption"]].copy()
    df.rename(columns={"Date": "ds", "total primary energy consumption": "y"}, inplace=True)
    df["ds"] = pd.to_datetime(df["ds"])
    return df

def train_prophet_model(df: pd.DataFrame):
    model = Prophet()
    model.fit(df)
    return model

def forecast_future(model, periods: int, freq: str = 'M'):
    future = model.make_future_dataframe(periods=periods, freq=freq)
    forecast = model.predict(future)
    return forecast

def run_prophet_pipeline():
    raw_df = fetch_data(query)
    prophet_df = prepare_prophet_data(raw_df)
    
    model = train_prophet_model(prophet_df)
    forecast = forecast_future(model, periods=12)  
    
    merged = prophet_df.merge(forecast[["ds", "yhat"]], on="ds", how="left")
    merged.rename(columns={"y": "Actual", "yhat": "Predicted"}, inplace=True)
    merged.to_csv("prophet_results.csv", index=False)


    return merged

results = run_prophet_pipeline()
print("Results saved to prophet_results.csv")


def evaluate_forecast(df: pd.DataFrame):
    df = df.dropna(subset=["Actual", "Predicted"])

    mae = mean_absolute_error(df["Actual"], df["Predicted"])
    rmse = np.sqrt(mean_squared_error(df["Actual"], df["Predicted"]))
    r2 = r2_score(df["Actual"], df["Predicted"])
    mape = np.mean(np.abs((df["Actual"] - df["Predicted"]) / df["Actual"])) * 100

    print("Evaluation Metrics:")
    print(f"MAE:  {mae:.4f}")
    print(f"RMSE: {rmse:.4f}")
    print(f"R²:   {r2:.4f}")
    print(f"MAPE: {mape:.2f}%")

    return {"MAE": mae, "RMSE": rmse, "R2": r2, "MAPE": mape}

evaluate_forecast(results)