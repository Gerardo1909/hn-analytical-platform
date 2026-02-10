"""
Módulo encargado de ejecutar el proceso de ingesta desde HackerNews.
"""

from typing import Any, Dict, List, Set

from ingestion.hn_client import HNClient
from ingestion.hn_fetcher import HNFetcher
from ingestion.story_tracker import StoryTracker
from utils.layer_storage_writer import LayerStorageWriter
from utils.logger import ingestion_logger as logger


class HNIngestor:
    """
    Clase que contiene la lógica para el proceso de ingesta:

    1. Cargar historias actualmente en tracking
    2. Re-ingestar historias trackeadas para capturar cambios
    3. Fetch nuevas top stories de la última semana
    4. Actualizar tracking (agregar nuevas, remover inactivas)
    5. Fetch comentarios para historias activas
    6. Guardar en raw/ particionado
    """

    BATCH_SIZE = 1000

    def __init__(
        self,
        api_client: HNClient,
        fetcher: HNFetcher,
        tracker: StoryTracker,
        writer: LayerStorageWriter,
        max_stories: int,
    ):
        """
        Args:
            api_client: Cliente para interactuar con la API de HackerNews
            fetcher: Fetcher para obtener historias y comentarios
            tracker: Tracker para gestionar el seguimiento temporal
            writer: Writer para guardar datos en el bucket
            max_stories: Número máximo de top stories a consultar
        """
        self.api_client = api_client
        self.fetcher = fetcher
        self.tracker = tracker
        self.writer = writer
        self.max_stories = max_stories

    def ingest(self) -> Dict[str, Any]:
        """
        Ejecuta el proceso completo de ingesta con tracking temporal.

        Returns:
            Diccionario con estadísticas del proceso:
            {
                "total_stories": int,
                "new_stories": int,
                "updated_stories": int,
                "total_comments": int,
                "active_tracked_stories": int
            }
        """

        # 1. Cargar tracking existente
        tracked_metadata = self._load_tracking()
        tracked_ids = self.tracker.get_tracked_story_ids(tracked_metadata)

        # 2. Re-ingestar historias trackeadas
        updated_stories = self._reingest_tracked_stories(tracked_metadata)

        # 3. Fetch nuevas historias
        new_stories = self._fetch_new_stories()
        new_story_ids = {s["id"] for s in new_stories}

        # 4. Deduplicar historias
        # Regla: preferir la versión de `updated_stories` si una historia aparece en ambas listas.
        story_map = {}

        # Primero, insertar new_stories
        for s in new_stories:
            story_map[s["id"]] = s

        # Luego, sobrescribir con updated_stories
        for s in updated_stories:
            story_map[s["id"]] = s

        # Lista final deduplicada
        deduped_all_stories = list(story_map.values())

        # Nuevo conjunto de IDs verdaderamente nuevas
        newly_discovered_ids = new_story_ids - tracked_ids

        # 5. Actualizar tracking
        updated_tracking = self._update_tracking(
            tracked_metadata, newly_discovered_ids, deduped_all_stories
        )

        # 6. Guardar historias
        self._save_stories(deduped_all_stories)

        # 7. Fetch y guardar comentarios (filtrar activos sobre la lista deduplicada)
        active_stories = self._get_active_stories(deduped_all_stories, updated_tracking)
        total_comments = self._fetch_and_save_comments(active_stories)

        # Estadísticas
        stats = {
            "total_stories": len(deduped_all_stories),
            "new_stories": len(newly_discovered_ids),
            "updated_stories": len(updated_stories),
            "total_comments": total_comments,
            "active_tracked_stories": len(updated_tracking),
        }
        return stats

    def _load_tracking(self) -> Dict[int, Dict[str, Any]]:
        """
        Carga el estado actual del tracking desde S3.

        Returns:
            Diccionario con metadata de historias trackeadas
        """
        tracked_metadata = self.tracker.load_active_stories()
        tracked_ids = self.tracker.get_tracked_story_ids(tracked_metadata)
        logger.info(f"Historias en tracking: {len(tracked_ids)}")
        return tracked_metadata

    def _reingest_tracked_stories(
        self, tracked_metadata: Dict[int, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Re-ingesta historias que están siendo trackeadas para capturar cambios.

        Args:
            tracked_metadata: Metadata de historias trackeadas

        Returns:
            Lista de historias re-ingestadas
        """
        tracked_ids = self.tracker.get_tracked_story_ids(tracked_metadata)
        updated_stories = []

        for story_id in tracked_ids:
            story = self.api_client.get_item(story_id)
            if story and story.get("type") == "story":
                updated_stories.append(story)
                logger.debug(f"Cargada historia presente en tracking {story_id}")

        logger.info(f"Cargadas {len(updated_stories)} presentes en tracking")
        return updated_stories

    def _fetch_new_stories(self) -> List[Dict[str, Any]]:
        """
        Obtiene nuevas top stories de la última semana.

        Returns:
            Lista de nuevas historias
        """
        new_stories = self.fetcher.fetch_top_stories_from_last_week(
            max_stories=self.max_stories
        )
        logger.info(f"Nuevas historias encontradas: {len(new_stories)}")
        return new_stories

    def _update_tracking(
        self,
        tracked_metadata: Dict[int, Dict[str, Any]],
        new_stories_ids: Set[int],
        all_stories: List[Dict[str, Any]],
    ) -> Dict[int, Dict[str, Any]]:
        """
        Actualiza el tracking con nuevas historias y métricas.

        Args:
            tracked_metadata: Metadata actual del tracking
            new_stories_ids: IDs de nuevas historias descubiertas
            all_stories: Todas las historias (nuevas + actualizadas)

        Returns:
            Tracking actualizado
        """

        # Extraer métricas de todas las historias
        story_metrics = {
            s["id"]: {
                "score": s.get("score", 0),
                "descendants": s.get("descendants", 0),
            }
            for s in all_stories
        }

        # Actualizar tracking
        updated_tracking = self.tracker.update_tracking(
            existing_tracked=tracked_metadata,
            new_story_ids=new_stories_ids,
            story_metrics=story_metrics,
        )

        # Persistir
        self.tracker.save_tracking(updated_tracking)

        return updated_tracking

    def _save_stories(self, stories: List[Dict[str, Any]]) -> None:
        """
        Guarda las historias en la capa raw.

        Args:
            stories: Lista de historias a guardar
        """
        if not stories:
            logger.warning("No hay historias para guardar")
            return

        self.writer.save(
            layer="raw",
            entity="stories",
            data=stories,
            format="json",
            additional_metadata={"entity_type": "story"},
        )

    def _get_active_stories(
        self,
        all_stories: List[Dict[str, Any]],
        updated_tracking: Dict[int, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Filtra las historias activas según el tracking.

        Args:
            all_stories: Todas las historias disponibles
            updated_tracking: Tracking actualizado

        Returns:
            Lista de historias activas
        """
        active_story_ids = self.tracker.get_tracked_story_ids(updated_tracking)
        active_stories = [s for s in all_stories if s["id"] in active_story_ids]
        logger.info(
            f"Historias activas para fetch de comentarios: {len(active_stories)}"
        )
        return active_stories

    def _fetch_and_save_comments(self, active_stories: List[Dict[str, Any]]) -> int:
        """
        Fetch y guarda comentarios para historias activas en batches.

        Args:
            active_stories: Lista de historias activas

        Returns:
            Total de comentarios guardados
        """
        total_comments = 0

        for story in active_stories:
            story_id = story["id"]
            comment_batch = []

            # Iterar sobre el generator y guardar en batches
            for comment in self.fetcher.fetch_comments_for_story(story):
                comment_batch.append(comment)

                # Cuando el batch está lleno, guardar y limpiar
                if len(comment_batch) >= self.BATCH_SIZE:
                    self._save_comment_batch(comment_batch, story_id)
                    total_comments += len(comment_batch)
                    comment_batch = []  # Liberar memoria

            # Guardar el último batch si quedaron comentarios
            if comment_batch:
                self._save_comment_batch(comment_batch, story_id)
                total_comments += len(comment_batch)

        return total_comments

    def _save_comment_batch(
        self, comment_batch: List[Dict[str, Any]], story_id: int
    ) -> None:
        """
        Guarda un batch de comentarios en la capa raw.

        Args:
            comment_batch: Lista de comentarios a guardar
            story_id: ID de la historia padre
        """
        self.writer.save(
            layer="raw",
            entity="comments",
            data=comment_batch,
            format="json",
            additional_metadata={
                "entity_type": "comment",
                "parent_story_id": story_id,
            },
        )
