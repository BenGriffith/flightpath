import argparse
import datetime
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


def _write_to_minio(minio_client, year, month):
    prefix = f"BTS/{year}"
    object = f"{BTS_FILENAME}_{year}_{month}.zip"
    full_url = urljoin(BTS_BASE_URL, object)

    with requests.get(full_url, stream=True) as response:
        response.raise_for_status()
        breakpoint()
        file_size = int(response.headers.get("Content-Length", 0))

        if file_size > 0:
            minio_client.put_object(
                bucket_name=BUCKET_BRONZE,
                object_name=f"{prefix}/{object}",
                data=response.raw,
                length=file_size,
                part_size=10 * 1024 * 1024,
            )

    time.sleep(3)


def main(end_year, month=None):
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )

    if month:
        _write_to_minio(minio_client, end_year, month)
        return "Incremental Processing Complete"

    start_year = BTS_START_YEAR
    while start_year <= end_year:
        for _month in range(1, MAX_MONTH + 1):
            _write_to_minio(minio_client, end_year, _month)
        start_year += 1
    return "Processing Complete"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Bureau of Transportation Statistics data"
    )

    parser.add_argument(
        "--type", type=str, required=True, choices=["incremental", "all"]
    )
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    args = parser.parse_args()

    if args.type == "all":
        end_year = datetime.date.today().year
        main(end_year)

    if args.type == "incremental":
        if args.year is None or args.month is None:
            parser.error("argument --type incremental requires --year and --month")

        if args.year < BTS_START_YEAR:
            parser.error(f"argument --year must be greater than {BTS_START_YEAR}")

        year = datetime.date.today().year
        month = datetime.date.today().month
        if args.year > year:
            parser.error(f"argument --year must be less than {year}")

        if args.year == year and args.month >= month:
            parser.error(f"argument --month must be less than {month}")

        if args.month > MAX_MONTH:
            parser.error(f"argument --month must be less than {MAX_MONTH}")

        main(args.year, args.month)
