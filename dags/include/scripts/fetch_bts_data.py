import argparse
import os
import time
import datetime
from urllib.parse import urljoin
from http import HTTPStatus

import requests


from constants import BTS_START_YEAR, BTS_BASE_URL, BTS_FILENAME, MAX_MONTH

#full_url = urljoin()

# argparse to accept arguments
# check for valid year
# create url
# submit request
# receive response
# write to filesystem / replace with object storage

# add checks and/or exceptions
# default scenario for optional args



parser = argparse.ArgumentParser(description="Fetch Bureau of Transportation Statistics data")
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
        filename = f"{BTS_FILENAME}_{year}_{args.month}.zip"
        full_url = urljoin(BTS_BASE_URL, filename)
        breakpoint()

        directory = os.getcwd()
        file_path = os.path.join(directory, filename)
        with requests.get(full_url, stream=True) as response:
            if response.status_code == HTTPStatus.OK:
                with open(file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)

        time.sleep(5)
    year += 1


    