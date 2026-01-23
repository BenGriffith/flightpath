import argparse
import datetime
import time
from urllib.parse import urljoin

import requests
from dateutil.relativedelta import relativedelta
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


def _object_url(year, month):
    object = f"BTS/{year}/{BTS_FILENAME}_{year}_{month}.zip"
    full_url = urljoin(BTS_BASE_URL, object)
    return object, full_url


def _write_to_minio(minio_client, object, url):
    with requests.get(url, stream=True) as response:
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

    time.sleep(3)


def main(end_year, month=None):
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )

    if month:
        object, url = _object_url(end_year, month)
        _write_to_minio(minio_client, object, url)
        return "Incremental Processing Complete"

    start_year = BTS_START_YEAR
    while start_year <= end_year:
        for temp_month in range(1, MAX_MONTH + 1):
            object, url = _object_url(end_year, temp_month)
            _write_to_minio(minio_client, object, url)

        start_year += 1
    return "Processing Complete"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Bureau of Transportation Statistics data"
    )

    parser.add_argument("type", type=str, choices=["incremental", "all"])
    args = parser.parse_args()

    if args.type == "all":
        end_year = datetime.date.today().year
        main(end_year)

    if args.type == "incremental":
        today = datetime.date.today()
        previous_month = today - relativedelta(months=1)
        main(end_year=previous_month.year, month=previous_month.month)
