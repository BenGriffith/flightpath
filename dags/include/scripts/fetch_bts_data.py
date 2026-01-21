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


def _minio_client():
    client = Minio(
        endpoint="localhost:9000",
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )
    return client


minio_client = _minio_client()

parser = argparse.ArgumentParser(
    description="Fetch Bureau of Transportation Statistics data"
)
parser.add_argument("--year", type=int, default=datetime.date.today().year)
parser.add_argument("--month", type=int, default=datetime.date.today().month)

args = parser.parse_args()

if args.year < BTS_START_YEAR:
    raise ValueError(f"Year must be greater than {BTS_START_YEAR}")

if args.month > MAX_MONTH:
    raise ValueError(f"Month must be less than {MAX_MONTH}")

year = args.year

while year <= args.year:
    for i in range(args.month, MAX_MONTH):
        object = f"{BTS_FILENAME}_{year}_{args.month}.zip"
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

    year += 1
