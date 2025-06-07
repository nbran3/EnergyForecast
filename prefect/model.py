import pandas as pd
import xgboost as xgb
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

project_id = os.getenv("project_id")
dataset = os.getenv("finalbq_table")

query = f"""
SELECT *
FROM `{project_id}.{dataset}`
"""

def fetch_data(query: str) -> pd.DataFrame:
    client = bigquery.Client()
    return client.query(query).to_dataframe()

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=[
        'Date',
        'nuclear electric power production',
        'total renewable energy production',
        'total fossil fuels production',
        'nuclear electric power consumption',
        'total fossil fuels consumption',
        'total renewable energy consumption'
    ])
    return df

def train_model(df: pd.DataFrame, target_column: str):
    X = df.drop(columns=[target_column])
    y = df[target_column]
    
    n = len(df)
    split_index = int(n * 0.8)
    
    X_train = X.iloc[:split_index]
    y_train = y.iloc[:split_index]
    X_test = X.iloc[split_index:]
    y_test = y.iloc[split_index:]

    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)

    params = {
        'objective': 'reg:squarederror',
        'max_depth': 6,
        'eta': 0.1,
        'eval_metric': 'rmse'
    }

    evals = [(dtrain, 'train'), (dtest, 'eval')]
    model = xgb.train(params, dtrain, num_boost_round=100, evals=evals, early_stopping_rounds=10)

    return model

def predict(model, df: pd.DataFrame):
    dmatrix = xgb.DMatrix(df)
    return model.predict(dmatrix)

def run_full_pipeline() -> pd.DataFrame:
    raw_df = fetch_data(query)
    df = preprocess_data(raw_df)
    model = train_model(df, target_column="total primary energy consumption")
    
    X = df.drop(columns=["total primary energy consumption"])
    preds = predict(model, X)

    results = pd.DataFrame({
        "Date": raw_df["Date"],
        "Actual": df["total primary energy consumption"],
        "Predicted": preds
    })

    return results

if __name__ == "__main__":
    results = run_full_pipeline()
    results.to_csv("results.csv", index=False)
    print("Results saved to results.csv")


