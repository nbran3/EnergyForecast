import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
from google.cloud import bigquery
from dotenv import load_dotenv
import os

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

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    df = df.drop(columns=[
        'nuclear electric power production',
        'total renewable energy production',
        'total fossil fuels production',
        'nuclear electric power consumption',
        'total fossil fuels consumption',
        'total renewable energy consumption'
    ], errors='ignore')
    return df

def train_model(df: pd.DataFrame, target_column: str, arima_order: tuple=(5,1,0)):
    X = df.drop(columns=[target_column])
    y = df[target_column]
    
    n = len(df)
    split_index = int(n * 0.8)
    
    y_train = y.iloc[:split_index]

    model = SARIMAX(y_train, order=arima_order, seasonal_order=(5,1,0,12))
    model_fit = model.fit()
    return model_fit
    

def predict(model, n_steps: int) -> pd.Series:
    return model.forecast(steps=n_steps)

def run_sarima_pipeline() -> pd.DataFrame:
    raw_df = fetch_data(query)
    target_col = "total primary energy consumption"
    
    df = preprocess_data(raw_df.copy())
    model = train_model(df, target_column=target_col, arima_order=(5,1,0)) 
    
    n = len(df)
    split_index = int(n * 0.8)
    n_steps_for_forecast = n - split_index
    
    preds = predict(model, n_steps=n_steps_for_forecast)
    actual_series = df[target_col].iloc[split_index:]
    
    mae = mean_absolute_error(actual_series, preds)
    mse = mean_squared_error(actual_series, preds)
    rmse = np.sqrt(mse)
    mape = np.mean(np.abs((actual_series - preds) / actual_series)) * 100
    r2 = r2_score(actual_series, preds)

    print(f"Evaluation Metrics:")
    print(f"R² Score: {r2:.4f}")
    print(f"MAE: {mae:.4f}")
    print(f"MSE: {mse:.4f}")
    print(f"RMSE: {rmse:.4f}")
    print(f"MAPE: {mape:.2f}%")

    results = pd.DataFrame({
        "Date": raw_df["Date"],
        "Actual": df["total primary energy consumption"],
        "Predicted": preds
    })
    results.to_csv("sarima_results.csv", index=False)

results = run_sarima_pipeline()
print("Results saved to sarima_results.csv")
