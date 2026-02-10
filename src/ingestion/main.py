"""
Punto de entrada para la capa de ingesta desde la API de HackerNews.
"""

import os
import sys

import boto3

from ingestion.hn_client import HNClient
from ingestion.hn_fetcher import HNFetcher
from ingestion.hn_ingestor import HNIngestor
from ingestion.story_tracker import StoryTracker
from utils.layer_storage_writer import LayerStorageWriter
from utils.logger import get_log_file_path
from utils.logger import ingestion_logger as logger


def run():
    """
    Punto de entrada principal: inicializa componentes y ejecuta ingesta.
    """
    logger.info("=== Empezando proceso de ingesta desde HackerNews ===")

    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    api_client = HNClient(max_retries=3, timeout=10)
    fetcher = HNFetcher(api_client)
    tracker = StoryTracker(
        bucket_name=bucket_name, s3_client=s3_client, tracking_days=7
    )
    writer = LayerStorageWriter(bucket_name=bucket_name, s3_client=s3_client)
    ingestor = HNIngestor(
        api_client=api_client,
        fetcher=fetcher,
        tracker=tracker,
        writer=writer,
        max_stories=20,
    )

    try:
        stats = ingestor.ingest()

        logger.info(
            f"Ingesta completada: {stats['total_stories']} historias "
            f"({stats['new_stories']} nuevas, {stats['updated_stories']} actualizadas), "
            f"{stats['total_comments']} comentarios, "
            f"{stats['active_tracked_stories']} historias activas en tracking"
        )

    except Exception as e:
        logger.error(f"Error durante la ingesta: {e}", exc_info=True)
        raise

    finally:
        try:
            ingestion_log_file = get_log_file_path("ingestion.log")
            storage_log_file = get_log_file_path("storage.log")
            writer.upload_log_file(
                pipeline="ingestion", log_file_path=ingestion_log_file
            )
            writer.upload_log_file(pipeline="storage", log_file_path=storage_log_file)
        except Exception as e:
            logger.error(f"No se pudo subir el log a S3: {e}")


if __name__ == "__main__":
    try:
        run()
    except Exception:
        sys.exit(1)
