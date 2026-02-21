"""
Lógica de negocio para fetch de historias y comentarios.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List

from ingestion.hn_client import HNClient
from utils.logger import ingestion_logger as logger


class HNFetcher:
    """
    Orquesta el fetch de historias y comentarios desde HackerNews.
    """

    def __init__(self, api_client: HNClient):
        self.api_client = api_client

    def fetch_top_stories_from_last_week(
        self, max_stories: int
    ) -> List[Dict[str, Any]]:
        """
        Obtiene las top historias de la última semana.

        Args:
            max_stories: Número máximo de historias a revisar del top 500 traido por HackerNews

        Returns:
            Lista de historias (dicts) de la última semana
        """
        logger.info(f"Extrayendo top {max_stories} historias...")
        top_stories = self.api_client.get_top_stories()
        if not top_stories:
            logger.error("No se pudieron obtener historias, verificar API")
            raise RuntimeError("No se pudieron obtener historias, verificar API")
        top_story_ids = top_stories[:max_stories]

        one_week_ago = datetime.utcnow() - timedelta(days=7)
        one_week_ago_ts = int(one_week_ago.timestamp())

        stories = []
        for story_id in top_story_ids:
            story = self.api_client.get_item(story_id)

            if not story:
                continue

            # Filtrar por tipo y fecha
            if story.get("type") == "story" and story.get("time", 0) >= one_week_ago_ts:
                stories.append(story)
                logger.info(f"Guardada historia de id {story_id}")

        logger.info(f"Se encontraron {len(stories)} historias de la última semana")
        return stories

    def fetch_comments_for_story(
        self, story: Dict[str, Any], max_depth: int = 10
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch recursivo de todos los comentarios de una historia.

        Args:
            story: Dict con los datos de la historia
            max_depth: Profundidad máxima de recursión

        Returns:
            Lista plana de todos los comentarios
        """
        story_id = story.get("id")
        kid_ids = story.get("kids", [])

        if not kid_ids:
            logger.info(f"La historia de id {story_id} no tiene comentarios")
            return

        logger.info(
            f"Extrayendo comentarios para historia {story_id} ({len(kid_ids)} respuestas directas)..."
        )

        yield from self._fetch_comments_recursive(kid_ids, depth=0, max_depth=max_depth)

    def _fetch_comments_recursive(
        self, comment_ids: List[int], depth: int, max_depth: int
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Extrae comentarios desde una historia de forma recursiva usando generador.
        """
        if depth >= max_depth:
            logger.warning(
                f"Se alcanzó el máximo de profundidad {max_depth}, deteniendo la recursión"
            )
            return

        for comment_id in comment_ids:
            comment = self.api_client.get_item(comment_id)

            if not comment:
                continue

            if comment.get("type") == "comment":
                # Yield el comentario inmediatamente
                yield comment

                # Recursión sobre replies
                kid_ids = comment.get("kids", [])
                if kid_ids:
                    yield from self._fetch_comments_recursive(
                        kid_ids, depth + 1, max_depth
                    )
