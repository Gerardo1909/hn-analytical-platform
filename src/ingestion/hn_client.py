"""
Cliente para interactuar con la API de Hacker News.
"""

import time
from typing import Any, Dict, List, Optional

import requests

from utils.logger import ingestion_logger as logger


class HNClient:
    """
    Cliente simple para la API de Hacker News.
    """

    BASE_URL = "https://hacker-news.firebaseio.com/v0"
    REQUEST_DELAY = 1.0

    def __init__(self, max_retries: int = 3, timeout: int = 10):
        """
        Args:
            max_retries: Número máximo de reintentos por request
            timeout: Timeout en segundos para cada request
        """
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        self.last_request_time = 0

    def _wait_for_rate_limit(self):
        """
        Espera el tiempo necesario para respetar rate limiting.
        """
        elapsed = time.time() - self.last_request_time
        if elapsed < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()

    def _make_request(self, endpoint: str) -> Optional[Any]:
        """
        Hace un request con retry y exponential backoff.

        Args:
            endpoint: Endpoint relativo a BASE_URL

        Returns:
            JSON parseado o None si falla después de todos los reintentos
        """
        url = f"{self.BASE_URL}/{endpoint}"

        for attempt in range(self.max_retries):
            try:
                self._wait_for_rate_limit()
                response = self.session.get(url, timeout=self.timeout)

                # Si es 404, el item no existe (puede ser deleted)
                if response.status_code == 404:
                    logger.warning(f"No se encontró el item: {endpoint}")
                    return None

                # Solo retry en errores de servidor o timeouts
                if response.status_code >= 500:
                    raise requests.HTTPError(
                        f"Error del servidor: {response.status_code}"
                    )

                response.raise_for_status()
                return response.json()

            except (
                requests.Timeout,
                requests.ConnectionError,
                requests.HTTPError,
            ) as e:
                wait_time = 2**attempt  # exponential backoff
                logger.warning(
                    f"La petición falló (intento {attempt + 1}/{self.max_retries}): {e}. "
                    f"Esperando {wait_time}s antes de reintentar..."
                )

                if attempt < self.max_retries - 1:
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Intento fallido despúes de {self.max_retries} intentos: {url}"
                    )
                    return None

        return None

    def get_top_stories(self) -> Optional[List[int]]:
        """
        Obtiene los IDs de las top historias actuales.
        """
        result = self._make_request("topstories.json")
        return result if result else []

    def get_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        """
        Obtiene los detalles de un item específico por su ID.
        """
        return self._make_request(f"item/{item_id}.json")
