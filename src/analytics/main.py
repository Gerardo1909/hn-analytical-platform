"""
Punto de entrada para la capa de reportes analíticos (output -> reports).
Ejecuta queries de negocio sobre output/ usando DuckDB y persiste CSV en reports/.
"""

import os
import sys
from datetime import datetime

import boto3

from analytics.hn_analytics import HNAnalytics
from utils.layer_storage_writer import LayerStorageWriter
from utils.logger import analytics_logger as logger
from utils.logger import get_log_file_path

# Registro de queries: (nombre_reporte, método, kwargs adicionales)
REPORT_QUERIES = [
    ("top_stories_by_score_velocity", "top_stories_by_score_velocity", {}),
    ("engagement_speed", "engagement_speed", {}),
    ("long_tail_stories", "long_tail_stories", {}),
    ("sentiment_by_story", "sentiment_by_story", {}),
    ("topic_trends", "topic_trends", {}),
]


def _run_and_save(
    analytics: HNAnalytics,
    writer: LayerStorageWriter,
    ingestion_date: str,
) -> dict:
    """
    Ejecuta todas las queries analíticas y guarda los resultados como CSV en reports/.

    Returns:
        Estadísticas de ejecución.
    """
    stats = {"reports_generated": 0, "reports_empty": 0, "reports_failed": 0}

    for report_name, method_name, extra_kwargs in REPORT_QUERIES:
        try:
            # sentiment_by_story requiere ambas fechas
            if method_name == "sentiment_by_story":
                df = getattr(analytics, method_name)(ingestion_date, ingestion_date)
            else:
                df = getattr(analytics, method_name)(ingestion_date, **extra_kwargs)

            if df.empty:
                logger.warning(f"Reporte '{report_name}' sin resultados, omitido")
                stats["reports_empty"] += 1
                continue

            records = df.to_dict(orient="records")
            key = writer.save(
                layer="reports",
                entity=report_name,
                data=records,
                format="csv",
                partition_date=ingestion_date,
            )
            logger.info(f"Reporte '{report_name}': {len(records)} filas -> {key}")
            stats["reports_generated"] += 1

        except Exception as e:
            logger.error(f"Error generando reporte '{report_name}': {e}", exc_info=True)
            stats["reports_failed"] += 1

    return stats


def run(ingestion_date: str):
    """
    Punto de entrada principal: inicializa componentes y ejecuta reportes analíticos.

    Args:
        ingestion_date: Fecha de ingesta a analizar (YYYY-MM-DD).
    """
    ingestion_date = ingestion_date or datetime.utcnow().strftime("%Y-%m-%d")

    logger.info(
        f"=== Empezando generación de reportes para fecha de ingesta: {ingestion_date} ==="
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    bucket_name = os.getenv("AWS_BUCKET_NAME")

    writer = LayerStorageWriter(bucket_name=bucket_name, s3_client=s3_client)
    analytics = HNAnalytics(
        bucket_name=bucket_name,
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    try:
        stats = _run_and_save(analytics, writer, ingestion_date)

        logger.info(
            f"Reportes completados: "
            f"{stats['reports_generated']} generados, "
            f"{stats['reports_empty']} vacíos, "
            f"{stats['reports_failed']} fallidos"
        )

    except Exception as e:
        logger.error(f"Error durante la generación de reportes: {e}", exc_info=True)
        raise

    finally:
        try:
            analytics_log = get_log_file_path("analytics.log")
            storage_log = get_log_file_path("storage.log")
            writer.upload_log_file(pipeline="analytics", log_file_path=analytics_log)
            writer.upload_log_file(pipeline="storage", log_file_path=storage_log)
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
