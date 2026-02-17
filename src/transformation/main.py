"""
Punto de entrada para la capa de transformación (processed -> output).
"""

import os
import sys
from datetime import datetime

import boto3

from quality.runner import QualityCheckError, QualityRunner
from transformation.hn_transformer import HNTransformer
from utils.layer_storage_loader import LayerStorageLoader
from utils.layer_storage_writer import LayerStorageWriter
from utils.logger import get_log_file_path
from utils.logger import transformation_logger as logger


def run(ingestion_date: str):
    """
    Punto de entrada principal: inicializa componentes y ejecuta transformación.

    Args:
        ingestion_date: Fecha de ingesta a transformar (YYYY-MM-DD).
    """
    ingestion_date = ingestion_date or datetime.utcnow().strftime("%Y-%m-%d")

    logger.info(
        f"=== Empezando transformación para fecha de ingesta: {ingestion_date} ==="
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
    transformer = HNTransformer(
        loader=loader, writer=writer, quality_runner=quality_runner
    )

    try:
        stats = transformer.transform(ingestion_date)

        logger.info(
            f"Transformación completada: "
            f"{stats['stories_enriched']} stories enriquecidas "
            f"(input: {stats['stories_input']}, "
            f"histórico: {stats['historical_observations_loaded']}), "
            f"{stats['comments_enriched']} comments enriquecidos "
            f"(input: {stats['comments_input']})"
        )

    except QualityCheckError as e:
        logger.error(f"Fallo de calidad bloqueante: {e}")
        raise

    except Exception as e:
        logger.error(f"Error durante la transformación: {e}", exc_info=True)
        raise

    finally:
        try:
            transformation_log = get_log_file_path("transformation.log")
            quality_log = get_log_file_path("quality.log")
            storage_log = get_log_file_path("storage.log")
            writer.upload_log_file(
                pipeline="transformation", log_file_path=transformation_log
            )
            writer.upload_log_file(pipeline="storage", log_file_path=storage_log)
            writer.upload_log_file(pipeline="quality", log_file_path=quality_log)
        except Exception as e:
            logger.error(f"No se pudo subir el log a S3: {e}")


if __name__ == "__main__":
    try:
        date_arg = sys.argv[1] if len(sys.argv) > 1 else None
        if not date_arg:
            raise ValueError("Fecha de ingesta no proporcionada")
        run(ingestion_date=date_arg)
    except Exception:
        sys.exit(1)
