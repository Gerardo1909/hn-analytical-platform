"""
Módulo para controlar logging del sistema
"""

import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
os.makedirs(LOG_DIR, exist_ok=True)


def get_logger(name: str, filename: str):
    """
    Obtiene un logger configurado con handlers de consola y archivo.

    Args:
        name: Nombre del logger
        filename: Nombre del archivo de log

    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler de consola (stdout)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler de archivo
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, filename),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def get_log_file_path(filename: str) -> str:
    """
    Retorna la ruta completa del archivo de log.

    Args:
        filename: Nombre del archivo de log

    Returns:
        Ruta completa al archivo
    """
    return os.path.join(LOG_DIR, filename)


# Loggers
ingestion_logger = get_logger(name="ingestion_logger", filename="ingestion.log")
storage_writer_logger = get_logger(name="storage_writer", filename="storage.log")
processing_logger = get_logger(name="processing_logger", filename="processing.log")
quality_logger = get_logger(name="quality_logger", filename="quality.log")
transformation_logger = get_logger(
    name="transformation_logger", filename="transformation.log"
)
analytics_logger = get_logger(name="analytics_logger", filename="analytics.log")
