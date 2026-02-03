import argparse
import datetime
import sys
import time

import requests
from minio import Minio
from scripts.unzip_bts_data import move_to_bronze
from utils.constants import (
    API_DELAY,
    BTS_BASE_URL,
    BTS_FILENAME,
    BTS_START_YEAR,
    BUCKET_LANDING,
    MAX_MONTH,
    MIN_MONTH,
    MINIO_ENDPOINT,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
)
from utils.logger import get_logger

logger = get_logger(__name__)


def _write_to_minio(
    minio_client: Minio, bts_year: int, bts_month: int, raise_on_empty: bool = False
) -> str:
    prefix = f"BTS/{bts_year}"
    bts_object = f"{BTS_FILENAME}_{bts_year}_{bts_month}.zip"
    full_url = f"{BTS_BASE_URL}{bts_object}"

    with requests.get(full_url, stream=True) as response:
        response.raise_for_status()
        file_size = int(response.headers.get("Content-Length", 0))

        if file_size == 0:
            file_size_message = (
                f"File size is {file_size} for {bts_year}-{bts_month} from {full_url}"
            )
            if raise_on_empty:
                raise ValueError(file_size_message)
            else:
                logger.warning(file_size_message)
                return None

        result = minio_client.put_object(
            bucket_name=BUCKET_LANDING,
            object_name=f"{prefix}/{bts_object}",
            data=response.raw,
            length=file_size,
            part_size=10 * 1024 * 1024,
        )

        logger.info(
            f"File {result.object_name} uploaded to {BUCKET_LANDING} successfully"
        )

        return f"/{prefix}/{bts_object}"


def main(api_delay: int, bts_year: int, bts_month: int | None = None) -> list[str]:
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )

    logger.info("Starting retrieval Bureau of Transporation Statistics data...")
    bts_objects = []
    if bts_month is not None:
        logger.info(f"Processing {bts_year}-{bts_month}")
        object = _write_to_minio(minio_client, bts_year, bts_month, True)
        bts_objects.append(object)
        logger.info("Ending data retrieval...")
        return bts_objects

    start_year = BTS_START_YEAR
    while start_year <= bts_year:
        for month in range(1, MAX_MONTH + 1):
            logger.info(f"Processing {start_year}-{month}")
            object = _write_to_minio(minio_client, start_year, month)
            bts_objects.append(object)
            time.sleep(api_delay)
        start_year += 1
    logger.info("Ending data retrieval...")
    return bts_objects


def cli() -> list[str]:
    parser = argparse.ArgumentParser(
        description="Fetch Bureau of Transportation Statistics data"
    )

    parser.add_argument(
        "--type", type=str, required=True, choices=["incremental", "all"]
    )
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--api-delay", type=int, default=API_DELAY)
    args = parser.parse_args()
    today = datetime.date.today()

    match args.type:
        case "all":
            main(args.api_delay, today.year)

        case "incremental":
            if args.year is None or args.month is None:
                parser.error("argument --type incremental requires --year and --month")

            if args.year < BTS_START_YEAR:
                parser.error(f"argument --year must be greater than {BTS_START_YEAR}")

            if args.year > today.year:
                parser.error(f"argument --year must be less than {today.year}")

            if args.year == today.year and args.month >= today.month:
                parser.error(f"argument --month must be less than {today.month}")

            if not MIN_MONTH <= args.month <= MAX_MONTH:
                parser.error(
                    f"argument --month must be greater than {MIN_MONTH} and less than {MAX_MONTH}"
                )

            bts_objects = main(args.api_delay, args.year, args.month)
            return bts_objects


if __name__ == "__main__":
    try:
        objects_created = cli()
        move_to_bronze(objects_created)
    except ValueError as error:
        logger.exception(error)
        sys.exit(1)
    except Exception as error:
        logger.exception(error)
        sys.exit(1)
