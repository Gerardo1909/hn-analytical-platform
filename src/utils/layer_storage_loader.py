"""
Módulo para leer datos desde las diferentes capas del data lake.
Maneja listado de objetos y lectura de formatos (JSONL, Parquet).
"""

import io
import json
from typing import Any, Dict, List, Literal

import pandas as pd

from utils.logger import storage_writer_logger as logger


class LayerStorageLoader:
    """
    Lector genérico para todas las capas del data lake.
    Complemento de LayerStorageWriter: lista y lee objetos desde S3.
    """

    VALID_LAYERS = {"raw", "processed", "output"}

    def __init__(self, bucket_name: str, s3_client):
        """
        Args:
            bucket_name: Nombre del bucket S3
            s3_client: Cliente boto3 S3
        """
        self.bucket_name = bucket_name
        self.s3 = s3_client

    def list_objects(self, prefix: str) -> List[str]:
        """
        Lista todas las keys de objetos bajo un prefijo en S3.

        Args:
            prefix: Prefijo S3 para filtrar objetos

        Returns:
            Lista de keys encontradas (excluyendo el prefijo vacío)
        """
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key != prefix and not key.endswith("/"):
                    keys.append(key)

        logger.debug(f"Encontrados {len(keys)} objetos bajo prefijo '{prefix}'")
        return keys

    def load_jsonl(self, key: str) -> List[Dict[str, Any]]:
        """
        Lee un archivo JSONL desde S3 y retorna lista de diccionarios.

        Args:
            key: Key del objeto en S3

        Returns:
            Lista de diccionarios, uno por línea del archivo

        Raises:
            Exception: Si no se puede leer o parsear el archivo
        """
        response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")

        records = []
        for line in content.strip().split("\n"):
            if line.strip():
                records.append(json.loads(line))

        logger.debug(f"Leídos {len(records)} registros desde '{key}'")
        return records

    def load_parquet(self, key: str) -> pd.DataFrame:
        """
        Lee un archivo Parquet desde S3 y retorna un DataFrame.

        Args:
            key: Key del objeto en S3

        Returns:
            DataFrame con los datos del archivo
        """
        response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
        parquet_bytes = response["Body"].read()
        df = pd.read_parquet(io.BytesIO(parquet_bytes), engine="pyarrow")

        logger.debug(f"Leídas {len(df)} filas desde '{key}'")
        return df

    def load_partition(
        self,
        layer: Literal["raw", "processed", "output"],
        entity: str,
        partition_date: str,
        format: Literal["json", "parquet"] = "json",
    ) -> pd.DataFrame:
        """
        Carga todos los archivos de una partición y retorna un DataFrame consolidado.

        Args:
            layer: Capa del data lake (raw, processed, output)
            entity: Entidad de los datos (stories, comments)
            partition_date: Fecha de la partición (YYYY-MM-DD)
            format: Formato de los archivos a leer

        Returns:
            DataFrame consolidado con todos los registros de la partición.
            DataFrame vacío si no hay archivos.

        Raises:
            ValueError: Si layer es inválido
        """
        if layer not in self.VALID_LAYERS:
            raise ValueError(
                f"Layer inválido: {layer}. Debe ser uno de {self.VALID_LAYERS}"
            )

        prefix = f"{layer}/{entity}/ingestion_date={partition_date}/"
        keys = self.list_objects(prefix)

        if not keys:
            logger.warning(
                f"No se encontraron archivos en {prefix} (formato: {format})"
            )
            return pd.DataFrame()

        # Filtrar por extensión para evitar leer archivos de formato incorrecto
        expected_ext = ".json" if format == "json" else ".parquet"
        matching_keys = [k for k in keys if k.endswith(expected_ext)]

        if not matching_keys:
            logger.warning(
                f"No se encontraron archivos .{format} en {prefix} "
                f"({len(keys)} archivos encontrados con otra extensión)"
            )
            return pd.DataFrame()

        # Leer y concatenar todos los archivos
        dataframes = []
        for key in matching_keys:
            try:
                if format == "json":
                    records = self.load_jsonl(key)
                    if records:
                        dataframes.append(pd.DataFrame(records))
                else:
                    df = self.load_parquet(key)
                    if not df.empty:
                        dataframes.append(df)
            except Exception as e:
                logger.error(f"Error leyendo '{key}': {e}")
                continue

        if not dataframes:
            logger.warning(f"Todos los archivos en {prefix} estaban vacíos o fallaron")
            return pd.DataFrame()

        consolidated = pd.concat(dataframes, ignore_index=True)

        logger.info(
            f"Cargados {len(consolidated)} registros de {entity} "
            f"desde {layer}/ (fecha: {partition_date}, "
            f"archivos: {len(matching_keys)})"
        )

        return consolidated
