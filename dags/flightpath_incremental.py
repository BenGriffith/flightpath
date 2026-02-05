from datetime import datetime

from airflow.decorators import dag, task
from include.scripts.fetch_bts_data import cli


@task
def fetch_task(year: int, month: int) -> list[str]:
    argv = ["--type", "incremental", "--year", str(year), "--month", str(month)]
    return cli(argv)


@dag(
    dag_id="flightpath_incremental",
    start_date=datetime(2026, 2, 3),
    schedule="@monthly",
    catchup=False,
)
def flightpath_incremental():
    fetch_task(year=2000, month=1)


flightpath_incremental()
