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

# argparse to accept arguments
# check for valid year
# create url
# submit request
# receive response
# write to filesystem / replace with object storage

# add checks and/or exceptions
# default scenario for optional args


# could add an end year argument that defaults to current year


def main(start_year, end_year, month):
    minio_client = Minio(
        endpoint="localhost:9000",
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )

    while start_year <= end_year:
        for i in range(month, MAX_MONTH + 1):
            if start_year == end_year and i == datetime.date.today().month:
                print("breaking out of loop")
                break

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

    parser.add_argument("--year", type=int, required=True, help="Year to start from")
    parser.add_argument("--month", type=int, required=True, help="Month to start from")

    args = parser.parse_args()

    if args.year < BTS_START_YEAR:
        raise ValueError(f"Year must be greater than {BTS_START_YEAR}")

    if args.year > datetime.date.today().year:
        raise ValueError(
            f"Year must be less than or equal to {datetime.date.today().year}"
        )

    if args.month > MAX_MONTH:
        raise ValueError(f"Month must be less than {MAX_MONTH}")

    if (
        args.year == datetime.date.today().year
        and args.month > datetime.date.today().month
    ):
        raise ValueError("Invalid year and month combination")

    main(start_year=args.year, end_year=args.year, month=args.month)
