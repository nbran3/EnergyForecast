from prefect import flow, task
from ingest_econ import run_econ_pipeline as econ_pipeline
from ingest_energy import run_energy_pipeline as energy_pipeline
from ingest_weather import run_weather_pipeline as weather_pipeline


@task
def run_econ_pipeline():
    econ_pipeline()

@task
def run_energy_pipeline():
    energy_pipeline()

@task
def run_weather_pipeline():
    weather_pipeline()

@flow
def master_flow():
    run_econ_pipeline()
    run_energy_pipeline()
    run_weather_pipeline()


if __name__ == "__main__":
    master_flow()
