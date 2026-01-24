import os

BTS_START_YEAR = 1987
BTS_BASE_URL = "https://transtats.bts.gov/PREZIP/"
BTS_FILENAME = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present"
MAX_MONTH = 12

BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "SILVER"
BUCKET_GOLD = "GOLD"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
