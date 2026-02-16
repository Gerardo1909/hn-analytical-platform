"""
Punto de entrada para la capa de procesamiento (raw -> processed).
"""

import os
import sys
from datetime import datetime

import boto3

from processing.hn_processor import HNProcessor, QualityCheckError
from quality.runner import QualityRunner
from utils.layer_storage_loader import LayerStorageLoader
from utils.layer_storage_writer import LayerStorageWriter
from utils.logger import get_log_file_path
from utils.logger import processing_logger as logger


def run(ingestion_date: str | None = None):
    """
    Punto de entrada principal: inicializa componentes y ejecuta procesamiento.

    Args:
        ingestion_date: Fecha de ingesta a procesar (YYYY-MM-DD).
                        Si no se provee, usa la fecha actual.
    """
    ingestion_date = ingestion_date or datetime.utcnow().strftime("%Y-%m-%d")

    logger.info(
        f"=== Empezando procesamiento para fecha de ingesta: {ingestion_date} ==="
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    bucket_name = os.getenv("AWS_BUCKET_NAME")

    loader = LayerStorageLoader(bucket_name=bucket_name, s3_client=s3_client)
    writer = LayerStorageWriter(bucket_name=bucket_name, s3_client=s3_client)
    quality_runner = QualityRunner()
    processor = HNProcessor(loader=loader, writer=writer, quality_runner=quality_runner)

    try:
        stats = processor.process(ingestion_date)

        logger.info(
            f"Procesamiento completado: "
            f"{stats['stories_processed']} historias "
            f"({stats['stories_duplicates_removed']} duplicados removidos), "
            f"{stats['comments_processed']} comentarios "
            f"({stats['comments_duplicates_removed']} duplicados removidos, "
            f"{stats['comments_orphaned']} huérfanos descartados)"
        )

    except QualityCheckError as e:
        logger.error(f"Fallo de calidad bloqueante: {e}")
        raise

    except Exception as e:
        logger.error(f"Error durante el procesamiento: {e}", exc_info=True)
        raise

    finally:
        try:
            processing_log_file = get_log_file_path("processing.log")
            storage_log_file = get_log_file_path("storage.log")
            quality_log_file = get_log_file_path("quality.log")
            writer.upload_log_file(
                pipeline="processing", log_file_path=processing_log_file
            )
            writer.upload_log_file(pipeline="storage", log_file_path=storage_log_file)
            writer.upload_log_file(pipeline="quality", log_file_path=quality_log_file)
        except Exception as e:
            logger.error(f"No se pudo subir el log a S3: {e}")


if __name__ == "__main__":
    try:
        # Permite pasar la fecha como argumento: python -m processing.main 2026-02-01
        date_arg = sys.argv[1] if len(sys.argv) > 1 else None
        run(ingestion_date=date_arg)
    except Exception:
        sys.exit(1)
