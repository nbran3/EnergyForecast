from prefect import flow, task
import os
from ingest_econ import run_econ_pipeline as econ_pipeline
from ingest_energy import run_energy_pipeline as energy_pipeline
from ingest_weather import run_weather_pipeline as weather_pipeline
from xgboost_model import run_xgboost_pipeline
from sarima_model import run_sarima_pipeline
from arima_model import run_arima_pipeline
from prophet_model import run_prophet_pipeline
from ingest_predictions import run_preds_pipeline
import subprocess


@task
def run_econ():
    econ_pipeline()

@task
def run_energy():
    energy_pipeline()

@task
def run_weather():
    weather_pipeline()

@task
def run_dbt(dbt_folder_path: str):
    cwd = os.getcwd()
    try:
        os.chdir(dbt_folder_path)
        result = subprocess.run(["dbt", "run"], capture_output=True, text=True, check=True)
        print(result.stdout)
    finally:
        os.chdir(cwd)

@task
def run_xgboost():
    run_xgboost_pipeline()

@task
def run_arima():
    run_arima_pipeline()

@task
def run_sarima():
    run_sarima_pipeline()


@task
def run_prophet():
    run_prophet_pipeline()

@task
def run_preds():
    run_preds_pipeline()


@flow
def training_pipeline():
    xgb_future = run_xgboost.submit()
    arima_future = run_arima.submit()
    prophet_future = run_prophet.submit()
    sarima_future = run_sarima.submit()

    xgb_result = xgb_future.result()
    arima_result = arima_future.result()
    prophet_result = prophet_future.result()
    sarima_result = sarima_future.result()


@flow
def master_flow():
    run_econ()
    run_energy()
    run_weather()
    run_dbt(r"C:\Users\nbwan\Python\EnergyPipeline\dbtFolder")
    training_pipeline()
    run_preds()


if __name__ == "__main__":
    master_flow()
