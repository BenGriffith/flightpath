import io
import zipfile

from include.utils.constants import (
    BUCKET_BRONZE,
    BUCKET_LANDING,
    MINIO_ENDPOINT,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
)
from include.utils.logger import get_logger
from minio import Minio

logger = get_logger(__name__)


def process_zip_file(
    client: Minio, source_bucket: str, target_bucket: str, object: str
) -> None:
    with client.get_object(source_bucket, object) as response:
        zip_data = response.read()

        # Unzip in-memory and write each file to target bucket
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            for info in zf.infolist():
                # Read unzipped file content
                file_content = zf.read(info.filename)

                result = client.put_object(
                    bucket_name=target_bucket,
                    object_name=info.filename,
                    data=io.BytesIO(file_content),
                    length=len(file_content),
                )

                logger.info(
                    f"File {object} unzipped and moved to {BUCKET_BRONZE}{result.object_name}"
                )


def move_to_bronze(bts_objects: list[str] | None) -> None:
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )

    logger.info("Starting unzipping process...")
    for bts_object in bts_objects:
        process_zip_file(minio_client, BUCKET_LANDING, BUCKET_BRONZE, bts_object)
    logger.info("Ending unzipping process...")
