import os

BTS_START_YEAR = 1987
BTS_BASE_URL = "https://transtats.bts.gov/PREZIP/"
BTS_FILENAME = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present"
MIN_MONTH = 1
MAX_MONTH = 12
API_DELAY = 5
CONTENT_TYPE = "application/x-zip-compressed"

BUCKET_LANDING = "landing"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ROOT_USER = os.environ["MINIO_ROOT_USER"]
MINIO_ROOT_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
