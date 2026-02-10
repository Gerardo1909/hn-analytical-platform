"""
Módulo para gestionar el tracking temporal de historias.
Decide qué historias deben seguirse y cuándo dejar de trackearlas.
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional, Set

from utils.logger import ingestion_logger as logger


class StoryTracker:
    """
    Gestiona el ciclo de vida del tracking de historias.
    Persiste el estado en el data lake para mantener memoria entre ejecuciones.
    """

    TRACKING_KEY = "metadata/story_tracking.json"

    def __init__(self, bucket_name: str, s3_client, tracking_days: int):
        """
        Args:
            bucket_name: Nombre del bucket S3
            s3_client: Cliente boto3 S3
            tracking_days: Días para mantener historias activas sin cambios
        """
        self.bucket_name = bucket_name
        self.s3 = s3_client
        self.tracking_days = tracking_days

    def load_active_stories(self) -> Dict[int, Dict[str, Any]]:
        """
        Carga el estado de tracking desde S3.

        Returns:
            Dict con story_id como key y metadata como value:
            {
                story_id: {
                    "first_seen": "2026-02-01",
                    "last_updated": "2026-02-03",
                    "last_score": 150,
                    "last_descendants": 45
                }
            }
        """
        try:
            response = self.s3.get_object(
                Bucket=self.bucket_name, Key=self.TRACKING_KEY
            )
            content = response["Body"].read().decode("utf-8")
            tracking_data = json.loads(content)

            # Convertir keys de string a int
            return {int(k): v for k, v in tracking_data["stories"].items()}

        except self.s3.exceptions.NoSuchKey:
            logger.info("No existe tracking previo, iniciando desde cero")
            return {}
        except Exception as e:
            logger.error(f"Error cargando tracking: {e}")
            return {}

    def update_tracking(
        self,
        existing_tracked: Dict[int, Dict[str, Any]],
        new_story_ids: Set[int],
        story_metrics: Dict[int, Dict[str, Any]],
    ) -> Dict[int, Dict[str, Any]]:
        """
        Actualiza el tracking con nuevas historias y métricas actualizadas.
        Aplica reglas de "fuera de interés" para limpieza.

        Args:
            existing_tracked: Tracking actual
            new_story_ids: IDs de nuevas historias descubiertas
            story_metrics: Métricas actuales de historias (score, descendants)

        Returns:
            Nuevo estado de tracking actualizado
        """
        today = datetime.utcnow().strftime("%Y-%m-%d")
        updated_tracking = {}

        # 1. Actualizar historias existentes
        for story_id, metadata in existing_tracked.items():
            if story_id in story_metrics:
                current_metrics = story_metrics[story_id]

                # Verificar si hubo cambios significativos
                has_changes = self._has_significant_changes(metadata, current_metrics)

                if has_changes:
                    # Actualizar con nuevas métricas
                    updated_tracking[story_id] = {
                        "first_seen": metadata["first_seen"],
                        "last_updated": today,
                        "last_score": current_metrics.get("score", 0),
                        "last_descendants": current_metrics.get("descendants", 0),
                    }
                    logger.debug(
                        f"Historia {story_id} actualizada: "
                        f"score={current_metrics.get('score')}, "
                        f"comments={current_metrics.get('descendants')}"
                    )
                else:
                    # Sin cambios, verificar si debe seguir en tracking
                    if self._should_keep_tracking(metadata, today):
                        updated_tracking[story_id] = metadata
                    else:
                        logger.info(
                            f"Historia {story_id} fuera de interés "
                            f"(sin cambios por {self.tracking_days} días)"
                        )

        # 2. Agregar nuevas historias
        for story_id in new_story_ids:
            if story_id not in updated_tracking:
                metrics = story_metrics.get(story_id, {})
                updated_tracking[story_id] = {
                    "first_seen": today,
                    "last_updated": today,
                    "last_score": metrics.get("score", 0),
                    "last_descendants": metrics.get("descendants", 0),
                }
                logger.info(f"Nueva historia agregada al tracking: {story_id}")

        logger.info(
            f"Tracking actualizado: {len(updated_tracking)} historias activas "
            f"({len(new_story_ids)} nuevas, "
            f"{len(existing_tracked) - len(updated_tracking)} removidas)"
        )

        return updated_tracking

    def save_tracking(self, tracking_data: Dict[int, Dict[str, Any]]) -> None:
        """
        Persiste el estado de tracking en S3.

        Args:
            tracking_data: Estado actual del tracking
        """
        # Convertir keys a string para JSON
        json_data = {str(k): v for k, v in tracking_data.items()}

        content = json.dumps(
            {
                "last_updated": datetime.utcnow().isoformat(),
                "total_stories": len(tracking_data),
                "stories": json_data,
            },
            indent=2,
        )

        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=self.TRACKING_KEY,
            Body=content.encode("utf-8"),
            ContentType="application/json",
            Metadata={"total_stories": str(len(tracking_data))},
        )

        logger.info(f"Tracking guardado en S3: {len(tracking_data)} historias activas")

    def _has_significant_changes(
        self, old_metadata: Dict[str, Any], current_metrics: Dict[str, Any]
    ) -> bool:
        """
        Determina si hubo cambios significativos en las métricas.

        Args:
            old_metadata: Metadata anterior
            current_metrics: Métricas actuales

        Returns:
            True si hay cambios significativos
        """
        old_score = old_metadata.get("last_score", 0)
        new_score = current_metrics.get("score", 0)

        old_descendants = old_metadata.get("last_descendants", 0)
        new_descendants = current_metrics.get("descendants", 0)

        # Cambio en score o al menos 5 comentarios nuevos
        score_changed = new_score != old_score
        comments_increased = (new_descendants - old_descendants) >= 5

        return score_changed or comments_increased

    def _should_keep_tracking(self, metadata: Dict[str, Any], today: str) -> bool:
        """
        Decide si una historia debe seguir en tracking.

        Args:
            metadata: Metadata de la historia
            today: Fecha actual (YYYY-MM-DD)

        Returns:
            True si debe seguir en tracking
        """
        last_updated = metadata.get("last_updated")

        if not last_updated:
            return True

        last_updated_date = datetime.strptime(last_updated, "%Y-%m-%d")
        today_date = datetime.strptime(today, "%Y-%m-%d")

        days_without_changes = (today_date - last_updated_date).days

        # Mantener en tracking si no ha pasado el límite de días
        return days_without_changes < self.tracking_days

    def get_tracked_story_ids(
        self, tracking_data: Optional[Dict[int, Dict[str, Any]]] = None
    ) -> Set[int]:
        """
        Obtiene el set de story_ids actualmente en tracking.

        Args:
            tracking_data: Datos de tracking (default: cargar desde S3)

        Returns:
            Set de story_ids activos
        """
        if tracking_data is None:
            tracking_data = self.load_active_stories()

        return set(tracking_data.keys())
