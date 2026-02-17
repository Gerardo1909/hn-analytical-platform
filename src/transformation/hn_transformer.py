"""
Módulo encargado de transformar datos procesados desde processed/ hacia output/.
Enriquece stories con métricas temporales y topics, y comments con sentiment.
"""

import html
import re
from datetime import datetime, timedelta
from typing import Any, Dict

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from quality.runner import QualityCheckError, QualityRunner
from utils.layer_storage_loader import LayerStorageLoader
from utils.layer_storage_writer import LayerStorageWriter
from utils.logger import transformation_logger as logger


class HNTransformer:
    """
    Clase que contiene la lógica para el enriquecimiento analítico:

    1. Leer stories y comments desde processed/ para una fecha de ingesta
    2. Cargar observaciones históricas de stories (ventana configurable)
    3. Calcular métricas temporales de engagement (velocity, peak, long tail)
    4. Extraer topics dominantes de títulos via TF-IDF
    5. Clasificar sentiment de comments via VADER
    6. Ejecutar checks de calidad sobre datos enriquecidos
    7. Guardar como Parquet en output/
    """

    SENTIMENT_POSITIVE_THRESHOLD = 0.05
    SENTIMENT_NEGATIVE_THRESHOLD = -0.05
    LONG_TAIL_HOURS_THRESHOLD = 48

    def __init__(
        self,
        loader: LayerStorageLoader,
        writer: LayerStorageWriter,
        quality_runner: QualityRunner,
        window_days: int = 7,
        top_n_topics: int = 3,
    ):
        """
        Args:
            loader: Loader para leer datos desde S3
            writer: Writer para guardar datos en S3
            quality_runner: Runner de checks de calidad
            window_days: Días hacia atrás para contexto temporal (default: 7)
            top_n_topics: Cantidad de topics dominantes por story (default: 3)
        """
        self.loader = loader
        self.writer = writer
        self.quality_runner = quality_runner
        self.window_days = window_days
        self.top_n_topics = top_n_topics

    def transform(self, ingestion_date: str) -> Dict[str, Any]:
        """
        Ejecuta la transformación completa para una fecha de ingesta.

        Lee datos procesados, enriquece con métricas temporales, topics
        y sentiment, valida calidad y persiste en output/.

        Args:
            ingestion_date: Fecha de ingesta a transformar (YYYY-MM-DD)

        Returns:
            Diccionario con estadísticas del proceso:
            {
                "stories_input": int,
                "stories_enriched": int,
                "comments_input": int,
                "comments_enriched": int,
                "historical_observations_loaded": int,
                "quality_stories": dict,
                "quality_comments": dict,
            }

        Raises:
            QualityCheckError: Si checks críticos de calidad fallan
        """
        target_stories = self._load_processed_stories(ingestion_date)
        target_comments = self._load_processed_comments(ingestion_date)

        stats = {
            "stories_input": len(target_stories),
            "stories_enriched": 0,
            "comments_input": len(target_comments),
            "comments_enriched": 0,
            "historical_observations_loaded": 0,
            "quality_stories": None,
            "quality_comments": None,
        }

        enriched_stories = pd.DataFrame()
        enriched_comments = pd.DataFrame()

        # Enriquecer stories: métricas temporales + topics
        if target_stories.empty:
            logger.warning("No hay stories procesadas para transformar")
        else:
            historical = self._load_historical_stories(ingestion_date)
            stats["historical_observations_loaded"] = len(historical)

            enriched_stories = self._enrich_stories_temporal(
                target_stories, historical, ingestion_date
            )
            enriched_stories = self._enrich_stories_topics(enriched_stories)
            stats["stories_enriched"] = len(enriched_stories)

        # Enriquecer comments: sentiment
        if target_comments.empty:
            logger.warning("No hay comments procesados para transformar")
        else:
            enriched_comments = self._enrich_comments_sentiment(target_comments)
            stats["comments_enriched"] = len(enriched_comments)

        # Quality checks sobre datos enriquecidos
        if not enriched_stories.empty:
            story_report = self.quality_runner.run_transformation_story_checks(
                enriched_stories, ingestion_date
            )
            stats["quality_stories"] = story_report.to_dict()
            self._save_quality_report(story_report.to_dict(), "stories", ingestion_date)

            if story_report.has_critical_failures:
                raise QualityCheckError(
                    f"Checks críticos fallidos en stories enriquecidas para "
                    f"{ingestion_date}. Revisar reporte en output/quality_reports/"
                )

        if not enriched_comments.empty:
            comment_report = self.quality_runner.run_transformation_comment_checks(
                enriched_comments, ingestion_date
            )
            stats["quality_comments"] = comment_report.to_dict()
            self._save_quality_report(
                comment_report.to_dict(), "comments", ingestion_date
            )

            if comment_report.has_critical_failures:
                raise QualityCheckError(
                    f"Checks críticos fallidos en comments enriquecidos para "
                    f"{ingestion_date}. Revisar reporte en output/quality_reports/"
                )

        # Persistir en output/
        if not enriched_stories.empty:
            self._save_output(enriched_stories, "stories", ingestion_date)

        if not enriched_comments.empty:
            self._save_output(enriched_comments, "comments", ingestion_date)

        return stats

    def _load_processed_stories(self, ingestion_date: str) -> pd.DataFrame:
        """
        Carga stories procesadas para la fecha indicada.

        Args:
            ingestion_date: Fecha de ingesta (YYYY-MM-DD)

        Returns:
            DataFrame con stories procesadas
        """
        df = self.loader.load_partition(
            layer="processed",
            entity="stories",
            partition_date=ingestion_date,
            format="parquet",
        )
        logger.info(f"Cargadas {len(df)} stories procesadas desde processed/")
        return df

    def _load_processed_comments(self, ingestion_date: str) -> pd.DataFrame:
        """
        Carga comments procesados para la fecha indicada.

        Args:
            ingestion_date: Fecha de ingesta (YYYY-MM-DD)

        Returns:
            DataFrame con comments procesados
        """
        df = self.loader.load_partition(
            layer="processed",
            entity="comments",
            partition_date=ingestion_date,
            format="parquet",
        )
        logger.info(f"Cargados {len(df)} comments procesados desde processed/")
        return df

    def _load_historical_stories(self, ingestion_date: str) -> pd.DataFrame:
        """
        Carga stories de los días previos dentro de la ventana temporal.

        Genera fechas desde (ingestion_date - window_days) hasta
        (ingestion_date - 1) y carga lo que exista en cada partición.

        Args:
            ingestion_date: Fecha de referencia (YYYY-MM-DD)

        Returns:
            DataFrame consolidado con observaciones históricas.
            DataFrame vacío si no hay datos previos.
        """
        target = datetime.strptime(ingestion_date, "%Y-%m-%d")
        dataframes = []

        for i in range(1, self.window_days + 1):
            date_str = (target - timedelta(days=i)).strftime("%Y-%m-%d")
            df = self.loader.load_partition(
                layer="processed",
                entity="stories",
                partition_date=date_str,
                format="parquet",
            )
            if not df.empty:
                dataframes.append(df)

        if not dataframes:
            logger.info(
                f"Sin observaciones históricas en ventana de {self.window_days} días"
            )
            return pd.DataFrame()

        historical = pd.concat(dataframes, ignore_index=True)
        logger.info(
            f"Cargadas {len(historical)} observaciones históricas "
            f"de {len(dataframes)} particiones"
        )
        return historical

    def _enrich_stories_temporal(
        self,
        target_stories: pd.DataFrame,
        historical_stories: pd.DataFrame,
        ingestion_date: str,
    ) -> pd.DataFrame:
        """
        Enriquece stories con métricas temporales de engagement.

        Combina observaciones del día target con históricas para calcular:
        - score_velocity: delta de score respecto a la observación previa
        - comment_velocity: delta de descendants respecto a la observación previa
        - hours_to_peak: horas entre creación de la story y la observación
          con score máximo
        - is_long_tail: True si la story sigue recibiendo comments 48h+
          después de su publicación
        - observations_in_window: cantidad de observaciones en la ventana considerada

        Args:
            target_stories: Stories de la fecha target
            historical_stories: Stories de fechas previas en la ventana
            ingestion_date: Fecha target (YYYY-MM-DD)

        Returns:
            DataFrame con stories enriquecidas (solo registros del target date)
        """
        # Combinar todas las observaciones disponibles
        if not historical_stories.empty:
            all_obs = pd.concat([historical_stories, target_stories], ignore_index=True)
        else:
            all_obs = target_stories.copy()

        # Dedup por si hay solapamiento entre ventana e historial
        all_obs = all_obs.drop_duplicates(subset=["id", "ingestion_date"], keep="last")
        all_obs = all_obs.sort_values(["id", "ingestion_date"]).reset_index(drop=True)

        # Asegurar tipos numéricos para operaciones aritméticas
        all_obs["score"] = pd.to_numeric(all_obs["score"], errors="coerce").fillna(0)
        all_obs["descendants"] = pd.to_numeric(
            all_obs["descendants"], errors="coerce"
        ).fillna(0)

        # Score y comment velocity via shift dentro de cada grupo
        all_obs["_prev_score"] = all_obs.groupby("id")["score"].shift(1)
        all_obs["_prev_descendants"] = all_obs.groupby("id")["descendants"].shift(1)

        all_obs["score_velocity"] = (
            (all_obs["score"] - all_obs["_prev_score"]).fillna(0).astype("Int64")
        )
        all_obs["comment_velocity"] = (
            (all_obs["descendants"] - all_obs["_prev_descendants"])
            .fillna(0)
            .astype("Int64")
        )

        # Observations in window por story
        obs_counts = (
            all_obs.groupby("id").size().reset_index(name="observations_in_window")
        )
        all_obs = all_obs.merge(obs_counts, on="id", how="left")
        all_obs["observations_in_window"] = all_obs["observations_in_window"].astype(
            "Int64"
        )

        # Hours to peak: horas entre creación y la fecha donde se observó max score
        peak_idx = all_obs.groupby("id")["score"].idxmax()
        peak_dates = all_obs.loc[peak_idx, ["id", "ingestion_date"]].rename(
            columns={"ingestion_date": "_peak_date"}
        )
        all_obs = all_obs.merge(peak_dates, on="id", how="left")

        # Convertir a datetime para cálculo de diferencia
        peak_dt = pd.to_datetime(all_obs["_peak_date"], utc=True)
        creation_time = pd.to_datetime(all_obs["time"], utc=True)
        all_obs["hours_to_peak"] = (
            ((peak_dt - creation_time).dt.total_seconds() / 3600).round(2).clip(lower=0)
        )

        # Is long tail: descendants creciendo 48h+ después de la creación
        current_dt = pd.to_datetime(all_obs["ingestion_date"], utc=True)
        hours_since_creation = (current_dt - creation_time).dt.total_seconds() / 3600
        descendants_growing = all_obs["comment_velocity"] > 0
        all_obs["is_long_tail"] = (
            (hours_since_creation > self.LONG_TAIL_HOURS_THRESHOLD)
            & descendants_growing
        ).astype("boolean")

        # Filtrar solo registros del target date, limpiar columnas auxiliares
        result = all_obs[all_obs["ingestion_date"] == ingestion_date].copy()
        result = result.drop(columns=["_prev_score", "_prev_descendants", "_peak_date"])

        logger.info(
            f"Enriquecidas {len(result)} stories con métricas temporales "
            f"(observaciones totales en ventana: {len(all_obs)})"
        )

        return result.reset_index(drop=True)

    def _enrich_stories_topics(self, stories_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae topics dominantes de títulos via TF-IDF.

        Ajusta un TfidfVectorizer sobre todos los títulos de la partición
        y asigna los top-N términos con mayor peso a cada story.

        Args:
            stories_df: DataFrame de stories (puede ya tener columnas temporales)

        Returns:
            DataFrame con columna dominant_topics agregada
        """
        titles = stories_df["title"].fillna("").astype(str).tolist()
        non_empty_count = sum(1 for t in titles if t.strip())

        if non_empty_count == 0:
            stories_df["dominant_topics"] = None
            logger.warning("Sin títulos válidos para extracción de topics")
            return stories_df

        try:
            vectorizer = TfidfVectorizer(
                max_features=100,
                stop_words="english",
                lowercase=True,
            )
            tfidf_matrix = vectorizer.fit_transform(titles)
            feature_names = vectorizer.get_feature_names_out()
        except ValueError:
            # Todos los títulos quedaron vacíos después de preprocesamiento
            stories_df["dominant_topics"] = None
            logger.warning("TF-IDF no pudo extraer features de los títulos")
            return stories_df

        topics = []
        for i in range(tfidf_matrix.shape[0]):
            row = tfidf_matrix[i].toarray().flatten()
            top_indices = row.argsort()[-self.top_n_topics :][::-1]
            top_terms = [feature_names[j] for j in top_indices if row[j] > 0]
            topics.append(",".join(top_terms) if top_terms else None)

        stories_df["dominant_topics"] = topics

        logger.info(
            f"Topics extraídos para {len(stories_df)} stories "
            f"(vocabulario: {len(feature_names)} términos)"
        )

        return stories_df

    def _enrich_comments_sentiment(self, comments_df: pd.DataFrame) -> pd.DataFrame:
        """
        Clasifica sentiment de comentarios via VADER.

        Produce un score compuesto [-1, 1] y una etiqueta categórica
        (positive, negative, neutral) basada en umbrales estándar.

        Args:
            comments_df: DataFrame de comments procesados

        Returns:
            DataFrame con columnas sentiment_score y sentiment_label agregadas
        """
        analyzer = SentimentIntensityAnalyzer()

        scores = []
        labels = []

        for raw_text in comments_df["text"].fillna(""):
            clean = self._clean_html(str(raw_text))

            if not clean:
                scores.append(0.0)
                labels.append("neutral")
                continue

            compound = analyzer.polarity_scores(clean)["compound"]
            scores.append(round(compound, 4))

            if compound >= self.SENTIMENT_POSITIVE_THRESHOLD:
                labels.append("positive")
            elif compound <= self.SENTIMENT_NEGATIVE_THRESHOLD:
                labels.append("negative")
            else:
                labels.append("neutral")

        comments_df["sentiment_score"] = scores
        comments_df["sentiment_label"] = labels

        label_counts = comments_df["sentiment_label"].value_counts().to_dict()
        logger.info(
            f"Sentiment clasificado para {len(comments_df)} comments: {label_counts}"
        )

        return comments_df

    def _save_output(self, df: pd.DataFrame, entity: str, ingestion_date: str) -> None:
        """
        Guarda un DataFrame enriquecido en la capa output/ como Parquet.

        Args:
            df: DataFrame a guardar
            entity: Nombre de la entidad (stories, comments)
            ingestion_date: Fecha de ingesta para particionamiento
        """
        records = df.to_dict(orient="records")

        self.writer.save(
            layer="output",
            entity=entity,
            data=records,
            format="parquet",
            partition_date=ingestion_date,
            additional_metadata={
                "entity_type": entity,
                "source_layer": "processed",
            },
        )

    def _save_quality_report(
        self, report_dict: dict, entity: str, ingestion_date: str
    ) -> None:
        """
        Persiste el reporte de calidad como JSON en output/quality_reports/.

        Args:
            report_dict: Reporte serializado como diccionario
            entity: Entidad evaluada (stories, comments)
            ingestion_date: Fecha de ingesta para particionamiento
        """
        self.writer.save(
            layer="output",
            entity=f"quality_reports_{entity}",
            data=[report_dict],
            format="json",
            partition_date=ingestion_date,
            additional_metadata={
                "report_type": "quality_transformation",
                "entity": entity,
            },
        )
        logger.info(
            f"Reporte de calidad de {entity} persistido en "
            f"output/quality_reports_{entity}/"
        )

    @staticmethod
    def _clean_html(text: str) -> str:
        """
        Elimina tags HTML y decodifica entidades para preprocesamiento NLP.

        Args:
            text: Texto potencialmente con HTML

        Returns:
            Texto limpio
        """
        if not text:
            return ""
        clean = re.sub(r"<[^>]+>", " ", text)
        clean = html.unescape(clean)
        clean = re.sub(r"\s+", " ", clean).strip()
        return clean
