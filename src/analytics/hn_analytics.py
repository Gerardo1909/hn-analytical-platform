"""
Módulo encargado de ejecutar consultas analíticas de negocio sobre output/.
Usa DuckDB para queries SQL sobre Parquet almacenados en S3 (MinIO).
"""

from typing import Optional

import duckdb
import pandas as pd

from utils.logger import analytics_logger as logger


class HNAnalytics:
    """
    Motor de consultas analíticas sobre el data lake.

    Conecta a S3 (MinIO) via DuckDB httpfs y ejecuta queries de negocio
    sobre los datos enriquecidos en output/. Cada método público representa
    una consulta de negocio y retorna un DataFrame con los resultados.
    """

    def __init__(
        self, bucket_name: str, endpoint_url: str, access_key: str, secret_key: str
    ):
        """
        Args:
            bucket_name: Nombre del bucket S3
            endpoint_url: Endpoint de S3/MinIO (ej: http://minio:9000)
            access_key: Access key de S3
            secret_key: Secret key de S3
        """
        self.bucket = bucket_name
        self.conn = duckdb.connect()

        self.conn.execute("INSTALL httpfs; LOAD httpfs;")
        self.conn.execute(f"SET s3_endpoint='{endpoint_url.replace('http://', '')}';")
        self.conn.execute(f"SET s3_access_key_id='{access_key}';")
        self.conn.execute(f"SET s3_secret_access_key='{secret_key}';")
        self.conn.execute("SET s3_use_ssl=false;")
        self.conn.execute("SET s3_url_style='path';")

        logger.info(f"Conexión DuckDB a S3 establecida (bucket: {bucket_name})")

    def _output_path(self, entity: str, date: Optional[str] = None) -> str:
        """
        Construye la ruta S3 hacia los Parquet de output/.

        Args:
            entity: Entidad (stories, comments)
            date: Fecha de partición (YYYY-MM-DD). Si None, lee todas.

        Returns:
            Ruta glob S3 para read_parquet
        """
        base = f"s3://{self.bucket}/output/{entity}"
        if date:
            return f"{base}/ingestion_date={date}/*.parquet"
        return f"{base}/**/*.parquet"

    def top_stories_by_score_velocity(self, date: str, limit: int = 20) -> pd.DataFrame:
        """
        Top stories ordenadas por velocidad de crecimiento de score.

        Args:
            date: Fecha de ingesta (YYYY-MM-DD)
            limit: Cantidad máxima de resultados

        Returns:
            DataFrame con las stories de mayor score velocity
        """
        path = self._output_path("stories", date)
        logger.info(
            f"Ejecutando top_stories_by_score_velocity (date={date}, limit={limit})"
        )

        return self.conn.execute(f"""
            SELECT id, title, score, score_velocity, comment_velocity,
                   hours_to_peak, is_long_tail, dominant_topics
            FROM read_parquet('{path}')
            ORDER BY score_velocity DESC
            LIMIT {limit}
        """).df()

    def engagement_speed(self, date: str) -> pd.DataFrame:
        """
        Clasifica stories por velocidad de engagement según hours_to_peak.

        Categorías: fast (<=6h), medium (6-24h), slow (>24h).

        Args:
            date: Fecha de ingesta (YYYY-MM-DD)

        Returns:
            DataFrame con conteo, score y comments promedio por categoría
        """
        path = self._output_path("stories", date)
        logger.info(f"Ejecutando engagement_speed (date={date})")

        return self.conn.execute(f"""
            SELECT
                CASE
                    WHEN hours_to_peak <= 6 THEN 'fast (<=6h)'
                    WHEN hours_to_peak <= 24 THEN 'medium (6-24h)'
                    ELSE 'slow (>24h)'
                END AS speed_category,
                COUNT(*) AS story_count,
                ROUND(AVG(score), 1) AS avg_score,
                ROUND(AVG(descendants), 1) AS avg_comments
            FROM read_parquet('{path}')
            GROUP BY speed_category
            ORDER BY story_count DESC
        """).df()

    def long_tail_stories(self, date: str) -> pd.DataFrame:
        """
        Stories con actividad sostenida (>48h de engagement).

        Args:
            date: Fecha de ingesta (YYYY-MM-DD)

        Returns:
            DataFrame con stories long tail ordenadas por comment velocity
        """
        path = self._output_path("stories", date)
        logger.info(f"Ejecutando long_tail_stories (date={date})")

        return self.conn.execute(f"""
            SELECT id, title, score, descendants, hours_to_peak,
                   comment_velocity, observations_in_window
            FROM read_parquet('{path}')
            WHERE is_long_tail = true
            ORDER BY comment_velocity DESC
        """).df()

    def sentiment_by_story(self, stories_date: str, comments_date: str) -> pd.DataFrame:
        """
        Análisis de sentimiento agregado por story.

        Cruza stories con comments para obtener distribución de sentimiento
        (positivo, negativo, neutro) por cada story.

        Args:
            stories_date: Fecha de partición de stories (YYYY-MM-DD)
            comments_date: Fecha de partición de comments (YYYY-MM-DD)

        Returns:
            DataFrame con métricas de sentimiento por story
        """
        stories_path = self._output_path("stories", stories_date)
        comments_path = self._output_path("comments", comments_date)
        logger.info(
            f"Ejecutando sentiment_by_story "
            f"(stories={stories_date}, comments={comments_date})"
        )

        return self.conn.execute(f"""
            SELECT s.id, s.title,
                   COUNT(c.id) AS total_comments,
                   ROUND(AVG(c.sentiment_score), 4) AS avg_sentiment,
                   SUM(CASE WHEN c.sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive,
                   SUM(CASE WHEN c.sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative,
                   SUM(CASE WHEN c.sentiment_label = 'neutral' THEN 1 ELSE 0 END) AS neutral
            FROM read_parquet('{stories_path}') s
            JOIN read_parquet('{comments_path}') c ON s.id = c.parent
            GROUP BY s.id, s.title
            ORDER BY total_comments DESC
        """).df()

    def topic_trends(self, date: str) -> pd.DataFrame:
        """
        Frecuencia y score promedio de los topics dominantes.

        Separa el campo dominant_topics (CSV) en filas individuales
        y calcula frecuencia y score promedio por topic.

        Args:
            date: Fecha de ingesta (YYYY-MM-DD)

        Returns:
            DataFrame con topic, frecuencia y score promedio
        """
        path = self._output_path("stories", date)
        logger.info(f"Ejecutando topic_trends (date={date})")

        return self.conn.execute(f"""
            WITH topics_split AS (
                SELECT
                    string_split(dominant_topics, ',') AS topic_list,
                    score
                FROM read_parquet('{path}')
                WHERE dominant_topics IS NOT NULL
            ),
            topics_flat AS (
                SELECT
                    UNNEST(topic_list) AS topic,
                    score
                FROM topics_split
            )
            SELECT
                TRIM(topic) AS topic,
                COUNT(*) AS frequency,
                ROUND(AVG(score), 1) AS avg_score
            FROM topics_flat
            WHERE TRIM(topic) != ''
            GROUP BY TRIM(topic)
            ORDER BY frequency DESC
            LIMIT 30
        """).df()
