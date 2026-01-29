import argparse
import datetime
import logging
import time
from urllib.parse import urljoin

import requests
from minio import Minio
from utils.constants import (
    BTS_BASE_URL,
    BTS_FILENAME,
    BTS_START_YEAR,
    BUCKET_BRONZE,
    MAX_MONTH,
    MINIO_ENDPOINT,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
)

logger = logging.getLogger(__name__)  # Airflow logger


def _write_to_minio(
    minio_client: Minio, bts_year: int, bts_month: int, raise_on_emtpy: bool = False
):
    prefix = f"BTS/{bts_year}"
    object = f"{BTS_FILENAME}_{bts_year}_{bts_month}.zip"
    full_url = urljoin(BTS_BASE_URL, object)

    with requests.get(full_url, stream=True) as response:
        response.raise_for_status()
        file_size = int(response.headers.get("Content-Length", 0))

        if file_size == 0:
            file_size_message = (
                f"File size is 0 for {bts_year}-{bts_month} from {full_url}"
            )
            if raise_on_emtpy:
                raise ValueError(file_size_message)
            else:
                logger.warning(file_size_message)
                return

        minio_client.put_object(
            bucket_name=BUCKET_BRONZE,
            object_name=f"{prefix}/{object}",
            data=response.raw,
            length=file_size,
            part_size=10 * 1024 * 1024,
        )
        logger.info(f"File {object} uploaded to {BUCKET_BRONZE}/{prefix}/ successfully")


def main(api_delay: int, bts_end_year: int, bts_month: int | None = None):
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )

    if bts_month:
        _write_to_minio(minio_client, bts_end_year, bts_month, True)
        return

    start_year = BTS_START_YEAR
    while start_year <= bts_end_year:
        for _month in range(1, MAX_MONTH + 1):
            _write_to_minio(minio_client, bts_end_year, _month)
        start_year += 1
        time.sleep(api_delay)
    return


def cli():
    parser = argparse.ArgumentParser(
        description="Fetch Bureau of Transportation Statistics data"
    )

    parser.add_argument(
        "--type", type=str, required=True, choices=["incremental", "all"]
    )
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--api-delay", type=int, default=2)
    args = parser.parse_args()
    today = datetime.date.today()

    match args.type:
        case "all":
            main(today.year)

        case "incremental":
            if args.year is None or args.month is None:
                parser.error("argument --type incremental requires --year and --month")

            if args.year < BTS_START_YEAR:
                parser.error(f"argument --year must be greater than {BTS_START_YEAR}")

            if args.year > today.year:
                parser.error(f"argument --year must be less than {today.year}")

            if args.year == today.year and args.month >= today.month:
                parser.error(f"argument --month must be less than {today.month}")

            if args.month > MAX_MONTH:
                parser.error(f"argument --month must be less than {MAX_MONTH}")

            main(args.api_delay, args.year, args.month)


if __name__ == "__main__":
    cli()
