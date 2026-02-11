"""
Módulo encargado de procesar datos crudos desde raw/ hacia processed/.
Normaliza, deduplica y valida integridad referencial.
"""

from typing import Any, Dict

import pandas as pd

from utils.layer_storage_loader import LayerStorageLoader
from utils.layer_storage_writer import LayerStorageWriter
from utils.logger import processing_logger as logger


class HNProcessor:
    """
    Clase que contiene la lógica para el procesamiento de datos crudos:

    1. Leer archivos JSONL desde raw/ para una fecha de ingesta dada
    2. Normalizar a formato tabular con tipado explícito
    3. Deduplicar por (id, ingestion_date)
    4. Validar integridad referencial (comentarios -> historias/comentarios)
    5. Guardar como Parquet en processed/
    """

    STORY_COLUMNS = [
        "id",
        "type",
        "by",
        "time",
        "title",
        "url",
        "text",
        "score",
        "descendants",
        "kids",
        "dead",
        "deleted",
    ]

    COMMENT_COLUMNS = [
        "id",
        "type",
        "by",
        "time",
        "text",
        "parent",
        "kids",
        "dead",
        "deleted",
    ]

    def __init__(self, loader: LayerStorageLoader, writer: LayerStorageWriter):
        """
        Args:
            loader: Loader para leer datos desde S3
            writer: Writer para guardar datos en S3
        """
        self.loader = loader
        self.writer = writer

    def process(self, ingestion_date: str) -> Dict[str, Any]:
        """
        Ejecuta el procesamiento completo para una fecha de ingesta.

        Args:
            ingestion_date: Fecha de ingesta a procesar (YYYY-MM-DD)

        Returns:
            Diccionario con estadísticas del proceso:
            {
                "stories_raw": int,
                "stories_processed": int,
                "stories_duplicates_removed": int,
                "comments_raw": int,
                "comments_processed": int,
                "comments_duplicates_removed": int,
                "comments_orphaned": int,
            }
        """

        # 1. Cargar datos crudos
        raw_stories = self._load_raw_stories(ingestion_date)
        raw_comments = self._load_raw_comments(ingestion_date)

        stats = {
            "stories_raw": len(raw_stories),
            "stories_processed": 0,
            "stories_duplicates_removed": 0,
            "comments_raw": len(raw_comments),
            "comments_processed": 0,
            "comments_duplicates_removed": 0,
            "comments_orphaned": 0,
        }

        stories_deduped = pd.DataFrame()
        comments_valid = pd.DataFrame()

        # 2. Procesar historias
        if raw_stories.empty:
            logger.warning("No hay historias crudas para procesar")
        else:
            stories_df = self._normalize_stories(raw_stories)
            stories_df = self._add_ingestion_date(stories_df, ingestion_date)
            stories_deduped = self._dedup(stories_df, entity="stories")

            stats["stories_duplicates_removed"] = len(stories_df) - len(stories_deduped)
            stats["stories_processed"] = len(stories_deduped)

        # 3. Procesar comentarios
        if raw_comments.empty:
            logger.warning("No hay comentarios crudos para procesar")
        else:
            comments_df = self._normalize_comments(raw_comments)
            comments_df = self._add_ingestion_date(comments_df, ingestion_date)
            comments_deduped = self._dedup(comments_df, entity="comments")

            stats["comments_duplicates_removed"] = len(comments_df) - len(
                comments_deduped
            )

            # 4. Validar integridad referencial
            if not stories_deduped.empty:
                comments_valid, orphaned_count = self._validate_referential_integrity(
                    stories_deduped, comments_deduped
                )
                stats["comments_orphaned"] = orphaned_count
            else:
                comments_valid = comments_deduped
                logger.warning(
                    "Sin historias disponibles, no se puede validar integridad "
                    "referencial de comentarios"
                )

            stats["comments_processed"] = len(comments_valid)

        # 5. Guardar en processed/
        if not stories_deduped.empty:
            self._save_processed(stories_deduped, "stories", ingestion_date)

        if not comments_valid.empty:
            self._save_processed(comments_valid, "comments", ingestion_date)

        return stats

    def _load_raw_stories(self, ingestion_date: str) -> pd.DataFrame:
        """
        Carga historias crudas desde raw/ para la fecha indicada.

        Args:
            ingestion_date: Fecha de ingesta (YYYY-MM-DD)

        Returns:
            DataFrame con historias crudas
        """
        df = self.loader.load_partition(
            layer="raw",
            entity="stories",
            partition_date=ingestion_date,
            format="json",
        )
        logger.info(f"Cargadas {len(df)} historias crudas desde raw/")
        return df

    def _load_raw_comments(self, ingestion_date: str) -> pd.DataFrame:
        """
        Carga comentarios crudos desde raw/ para la fecha indicada.

        Args:
            ingestion_date: Fecha de ingesta (YYYY-MM-DD)

        Returns:
            DataFrame con comentarios crudos
        """
        df = self.loader.load_partition(
            layer="raw",
            entity="comments",
            partition_date=ingestion_date,
            format="json",
        )
        logger.info(f"Cargados {len(df)} comentarios crudos desde raw/")
        return df

    def _normalize_stories(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza historias: selecciona columnas, aplica tipado explícito.

        Args:
            df: DataFrame crudo de historias

        Returns:
            DataFrame normalizado
        """
        df = self._select_columns(df, self.STORY_COLUMNS)

        # Tipado explícito
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
        df["time"] = pd.to_datetime(
            pd.to_numeric(df["time"], errors="coerce"), unit="s", utc=True
        )
        df["score"] = (
            pd.to_numeric(df["score"], errors="coerce").fillna(0).astype("Int64")
        )
        df["descendants"] = (
            pd.to_numeric(df["descendants"], errors="coerce").fillna(0).astype("Int64")
        )

        # Booleanos opcionales
        for col in ["dead", "deleted"]:
            if col in df.columns:
                df[col] = df[col].astype("boolean")

        # Descartar registros sin id válido
        invalid_ids = df["id"].isna().sum()
        if invalid_ids > 0:
            logger.warning(f"Descartadas {invalid_ids} historias con id inválido")
            df = df.dropna(subset=["id"])

        logger.info(f"Normalizadas {len(df)} historias")
        return df

    def _normalize_comments(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza comentarios: selecciona columnas, aplica tipado explícito.

        Args:
            df: DataFrame crudo de comentarios

        Returns:
            DataFrame normalizado
        """
        df = self._select_columns(df, self.COMMENT_COLUMNS)

        # Tipado explícito
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
        df["time"] = pd.to_datetime(
            pd.to_numeric(df["time"], errors="coerce"), unit="s", utc=True
        )
        df["parent"] = pd.to_numeric(df["parent"], errors="coerce").astype("Int64")

        # Booleanos opcionales
        for col in ["dead", "deleted"]:
            if col in df.columns:
                df[col] = df[col].astype("boolean")

        # Descartar registros sin id o parent válido
        invalid_ids = df["id"].isna().sum()
        invalid_parents = df["parent"].isna().sum()
        if invalid_ids > 0:
            logger.warning(f"Descartados {invalid_ids} comentarios con id inválido")
        if invalid_parents > 0:
            logger.warning(
                f"Descartados {invalid_parents} comentarios con parent inválido"
            )
        df = df.dropna(subset=["id", "parent"])

        logger.info(f"Normalizados {len(df)} comentarios")
        return df

    def _select_columns(self, df: pd.DataFrame, expected_columns: list) -> pd.DataFrame:
        """
        Selecciona columnas esperadas. Agrega con None las faltantes,
        descarta las que no están en el esquema.

        Args:
            df: DataFrame original
            expected_columns: Lista de columnas esperadas

        Returns:
            DataFrame con columnas consistentes
        """
        # Agregar columnas faltantes con None
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
                logger.debug(f"Columna '{col}' no encontrada en raw, agregada con None")

        # Seleccionar solo las esperadas
        return df[expected_columns].copy()

    def _add_ingestion_date(
        self, df: pd.DataFrame, ingestion_date: str
    ) -> pd.DataFrame:
        """
        Agrega columna de fecha de ingesta al DataFrame.

        Args:
            df: DataFrame a modificar
            ingestion_date: Fecha de ingesta (YYYY-MM-DD)

        Returns:
            DataFrame con columna ingestion_date
        """
        df["ingestion_date"] = ingestion_date
        return df

    def _dedup(self, df: pd.DataFrame, entity: str) -> pd.DataFrame:
        """
        Deduplica registros por (id, ingestion_date), manteniendo la última ocurrencia.

        Args:
            df: DataFrame a deduplicar
            entity: Nombre de la entidad (para logging)

        Returns:
            DataFrame deduplicado
        """
        before = len(df)
        df = df.drop_duplicates(subset=["id", "ingestion_date"], keep="last")
        after = len(df)

        removed = before - after
        if removed > 0:
            logger.info(f"Deduplicación de {entity}: {removed} registros removidos")

        return df.reset_index(drop=True)

    def _validate_referential_integrity(
        self, stories_df: pd.DataFrame, comments_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, int]:
        """
        Valida que cada comentario tenga un parent válido:
        su parent debe apuntar a un story_id o a otro comment_id conocido.

        Args:
            stories_df: DataFrame de historias procesadas
            comments_df: DataFrame de comentarios procesados

        Returns:
            Tupla (DataFrame de comentarios válidos, cantidad de huérfanos)
        """
        valid_story_ids = set(stories_df["id"].dropna().astype(int).tolist())
        valid_comment_ids = set(comments_df["id"].dropna().astype(int).tolist())
        valid_parent_ids = valid_story_ids | valid_comment_ids

        is_valid = comments_df["parent"].astype(int).isin(valid_parent_ids)
        orphaned_count = (~is_valid).sum()

        if orphaned_count > 0:
            orphaned_ids = comments_df.loc[~is_valid, "id"].tolist()
            logger.warning(
                f"Descartados {orphaned_count} comentarios huérfanos "
                f"(ids: {orphaned_ids[:10]}{'...' if orphaned_count > 10 else ''})"
            )

        valid_comments = comments_df[is_valid].reset_index(drop=True)
        return valid_comments, int(orphaned_count)

    def _save_processed(
        self, df: pd.DataFrame, entity: str, ingestion_date: str
    ) -> None:
        """
        Guarda un DataFrame procesado en la capa processed/ como Parquet.

        Args:
            df: DataFrame a guardar
            entity: Nombre de la entidad (stories, comments)
            ingestion_date: Fecha de ingesta para particionamiento
        """
        records = df.to_dict(orient="records")

        self.writer.save(
            layer="processed",
            entity=entity,
            data=records,
            format="parquet",
            partition_date=ingestion_date,
            additional_metadata={
                "entity_type": entity,
                "source_layer": "raw",
            },
        )
