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
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
)


def main(start_year, end_year):
    minio_client = Minio(
        endpoint="localhost:9000",
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )

    while start_year <= end_year:
        for month in range(1, MAX_MONTH + 1):
            if start_year == end_year and month == datetime.date.today().month:
                print("breaking out of loop")

            object = f"BTS/{start_year}/{BTS_FILENAME}_{start_year}_{month}.zip"
            full_url = urljoin(BTS_BASE_URL, object)

            with requests.get(full_url, stream=True) as response:
                response.raise_for_status()

                file_size = int(response.headers.get("Content-length", 0))

                if file_size > 0:
                    minio_client.put_object(
                        bucket_name=BUCKET_BRONZE,
                        object_name=object,
                        data=response.raw,
                        length=file_size,
                        part_size=10 * 1024 * 1024,
                        content_type=response.headers.get("Content-Type"),
                    )

            time.sleep(5)

        start_year += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Bureau of Transportation Statistics data"
    )

    parser.add_argument("--start-year", type=int, required=True, help="Year from")
    parser.add_argument(
        "--end-year",
        type=int,
        required=False,
        default=datetime.date.today().year,
        help="Year to",
    )

    args = parser.parse_args()

    if args.start_year < BTS_START_YEAR:
        raise ValueError(f"Year must be greater than {BTS_START_YEAR}")

    if args.start_year > args.end_year:
        raise ValueError(f"Year must be less than or equal to {args.end_year}")

    main(start_year=args.start_year, end_year=args.end_year)
