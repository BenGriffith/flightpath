from datetime import date, datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from include.scripts.fetch_bts_data import cli
from include.scripts.unzip_bts_data import move_to_bronze
from include.utils.constants import BTS_LAG

FLIGHTPATH_ALL_DATA = Variable.get("flightpath_all_data", deserialize_json=True)


@task
def fetch_task() -> list[str] | None:
    if FLIGHTPATH_ALL_DATA:
        argv = ["--type", "all"]
        bts_objects = cli(argv)
    else:
        run_date = date.today() - relativedelta(years=BTS_LAG)
        run_month = str(run_date.month)
        run_year = str(run_date.year)

        argv = ["--type", "incremental", "--year", run_year, "--month", run_month]
        bts_objects = cli(argv)
    return bts_objects


@task
def unzip_task(bts_objects: list[str] | None) -> None:
    move_to_bronze(bts_objects)


@dag(
    dag_id="flightpath",
    start_date=datetime(2026, 1, 15),
    schedule="@monthly",
    catchup=False,
)
def flightpath():
    bts_objects = fetch_task()

    unzip_task(bts_objects)


flightpath()
