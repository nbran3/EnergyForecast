from prefect import flow, task
import subprocess
import os
import sys
from ingest_econ import run_econ_pipeline as econ_pipeline
from ingest_energy import run_energy_pipeline as energy_pipeline
from ingest_weather import run_weather_pipeline as weather_pipeline
from model import run_full_pipeline
from ingest_predictions import run_preds_pipeline as preds_pipeline



@task
def run_econ_pipeline():
    econ_pipeline()

@task
def run_energy_pipeline():
    energy_pipeline()

@task
def run_weather_pipeline():
    weather_pipeline()

@task
def run_dbt(dbt_folder_path: str):
    cwd = os.getcwd()
    try:
        os.chdir(dbt_folder_path)
        print(f"Attempting to run 'dbt run' in {dbt_folder_path}...")
        result = subprocess.run(
            ["dbt", "run"], 
            capture_output=True, 
            text=True, 
            check=True 
        )
        print("dbt run stdout:")
        print(result.stdout)
        if result.stderr:
            print("dbt run stderr (warnings/info):")
            print(result.stderr)
    except FileNotFoundError:
        print(f"Error: The 'dbt' command was not found. Ensure dbt is installed and in your PATH.")
        raise
    except subprocess.CalledProcessError as e:
        print(f"dbt run failed with exit code {e.returncode}:")
        print(f"Stdout:\n{e.stdout}")
        print(f"Stderr:\n{e.stderr}")
        raise RuntimeError(f"dbt run failed. Check dbt logs for details. Stderr:\n{e.stderr}") from e
    except Exception as e:
        print(f"An unexpected error occurred in run_dbt: {e}")
        raise
    finally:
        os.chdir(cwd)


@task
def run_model_pipeline():
    results = run_full_pipeline()
    results.to_csv("results.csv", index=False)
    return results

@task
def run_preds_pipeline():
    preds_pipeline()

@flow
def master_flow():
    run_econ_pipeline_result = run_econ_pipeline()
    run_energy_pipeline_result = run_energy_pipeline()
    run_weather_pipeline_result = run_weather_pipeline()
    run_dbt(r"C:\Users\nbwan\Python\EnergyPipeline\dbtFolder")
    run_model_pipeline()
    run_preds_pipeline()



if __name__ == "__main__":
    master_flow()
