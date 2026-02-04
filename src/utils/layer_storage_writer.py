"""
Módulo para escribir datos en las diferentes capas del data lake.
Maneja particionamiento y formatos (JSON, Parquet).
"""

import io
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from utils.logger import storage_writer_logger as logger


class LayerStorageWriter:
    """
    Escritor genérico para todas las capas del data lake.
    Maneja particionamiento por fecha y múltiples formatos.
    """

    VALID_LAYERS = {"raw", "processed", "output"}
    VALID_FORMATS = {"json", "parquet"}

    def __init__(self, bucket_name: str, s3_client):
        """
        Args:
            bucket_name: Nombre del bucket S3
            s3_client: Cliente boto3 S3
        """
        self.bucket_name = bucket_name
        self.s3 = s3_client

    def save(
        self,
        layer: Literal["raw", "processed", "output"],
        entity: str,
        data: List[Dict[str, Any]],
        format: Literal["json", "parquet"] = "json",
        partition_date: Optional[str] = None,
        additional_metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Guarda datos en la capa especificada con el formato indicado.

        Args:
            layer: Capa del data lake (raw, processed, output)
            entity: Entidad de los datos (stories, comments, etc)
            data: Lista de diccionarios con los datos
            format: Formato de salida (json o parquet)
            partition_date: Fecha de partición (default: hoy)
            additional_metadata: Metadata adicional para incluir en el objeto

        Returns:
            Key del objeto guardado en S3

        Raises:
            ValueError: Si layer o format son inválidos
            ValueError: Si data está vacío
        """
        # Validaciones
        self._validate_inputs(layer, format, data)

        # Preparar metadatos
        partition_date = partition_date or datetime.utcnow().strftime("%Y-%m-%d")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Construir key
        key = self._build_key(layer, entity, partition_date, timestamp, format)

        # Preparar y guardar datos según formato
        if format == "json":
            self._save_as_json(key, data, additional_metadata)
        else:  # parquet
            self._save_as_parquet(key, data, additional_metadata)

        logger.info(
            f"Guardados {len(data)} registros de {entity} en {layer}/{entity} "
            f"(formato: {format}, key: {key})"
        )

        return key

    def _validate_inputs(
        self, layer: str, format: str, data: List[Dict[str, Any]]
    ) -> None:
        """Valida los inputs del método save."""
        if layer not in self.VALID_LAYERS:
            raise ValueError(
                f"Layer inválido: {layer}. Debe ser uno de {self.VALID_LAYERS}"
            )

        if format not in self.VALID_FORMATS:
            raise ValueError(
                f"Formato inválido: {format}. Debe ser uno de {self.VALID_FORMATS}"
            )

        if not data:
            raise ValueError("No hay datos para guardar (lista vacía)")

    def _build_key(
        self, layer: str, entity: str, partition_date: str, timestamp: str, format: str
    ) -> str:
        """Construye la key S3 con estructura de particionamiento."""
        return (
            f"{layer}/{entity}/ingestion_date={partition_date}/"
            f"{entity}_{timestamp}.{format}"
        )

    def _save_as_json(
        self,
        key: str,
        data: List[Dict[str, Any]],
        additional_metadata: Optional[Dict[str, Any]],
    ) -> None:
        """
        Guarda datos en formato JSON Lines (un JSON por línea).
        """
        # Convertir a JSONL
        jsonl_content = "\n".join(
            json.dumps(record, ensure_ascii=False) for record in data
        )

        # Metadata S3
        metadata = {
            "record_count": str(len(data)),
            "format": "jsonl",
            "ingestion_timestamp": datetime.utcnow().isoformat(),
        }

        if additional_metadata:
            metadata.update({k: str(v) for k, v in additional_metadata.items()})

        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=jsonl_content.encode("utf-8"),
            ContentType="application/x-ndjson",
            Metadata=metadata,
        )

    def _save_as_parquet(
        self,
        key: str,
        data: List[Dict[str, Any]],
        additional_metadata: Optional[Dict[str, Any]],
    ) -> None:
        """
        Guarda datos en formato Parquet.
        """
        # Convertir a DataFrame
        df = pd.DataFrame(data)

        # Serializar a Parquet en memoria
        parquet_buffer = io.BytesIO()
        df.to_parquet(
            parquet_buffer,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        parquet_buffer.seek(0)

        # Metadata S3
        metadata = {
            "record_count": str(len(df)),
            "column_count": str(len(df.columns)),
            "format": "parquet",
            "compression": "snappy",
            "ingestion_timestamp": datetime.utcnow().isoformat(),
        }

        if additional_metadata:
            metadata.update({k: str(v) for k, v in additional_metadata.items()})

        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
            Metadata=metadata,
        )

    def upload_log_file(
        self, pipeline: str, log_file_path: str, execution_date: Optional[str] = None
    ) -> Optional[str]:
        """
        Sube un archivo de log a S3 particionado por fecha.

        Args:
            pipeline: Nombre del pipeline (ej: "ingestion", "processing")
            log_file_path: Ruta local al archivo de log
            execution_date: Fecha de ejecución (default: hoy)

        Returns:
            Key del archivo subido o None si no existe
        """
        if not os.path.exists(log_file_path):
            logger.warning(f"Archivo de log no encontrado: {log_file_path}")
            return None

        execution_date = execution_date or datetime.utcnow().strftime("%Y-%m-%d")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(log_file_path)

        # Estructura: logs/{pipeline}/execution_date={date}/{timestamp}_{filename}
        key = f"logs/{pipeline}/execution_date={execution_date}/{timestamp}_{filename}"

        with open(log_file_path, "rb") as f:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=f,
                ContentType="text/plain",
                Metadata={
                    "pipeline": pipeline,
                    "execution_date": execution_date,
                    "original_filename": filename,
                },
            )

        logger.info(f"Log subido a S3: {key}")
        return key
