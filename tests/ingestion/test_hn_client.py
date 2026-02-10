"""
Tests unitarios para HNClient.
Verifica retry, rate limiting, manejo de errores y parseo de respuestas.
"""

from unittest.mock import Mock, patch

import pytest
import pytest_check as check
import requests

from ingestion.hn_client import HNClient


@pytest.fixture
def hn_client():
    """Fixture que retorna una instancia de HNClient con parámetros de test."""
    return HNClient(max_retries=3, timeout=5)


class TestGetTopStories:
    """Tests para el método get_top_stories."""

    @patch("ingestion.hn_client.HNClient._make_request")
    def test_get_top_stories_should_return_list_of_ids_when_api_succeeds(
        self, mock_request
    ):
        """Verifica que retorna una lista de IDs cuando la API responde correctamente."""
        mock_request.return_value = [1, 2, 3, 4, 5]
        client = HNClient()

        result = client.get_top_stories()

        check.is_instance(result, list)
        check.equal(len(result), 5)
        check.equal(result, [1, 2, 3, 4, 5])
        mock_request.assert_called_once_with("topstories.json")

    @patch("ingestion.hn_client.HNClient._make_request")
    def test_get_top_stories_should_return_empty_list_when_api_returns_none(
        self, mock_request
    ):
        """Verifica que retorna lista vacía cuando la API falla."""
        mock_request.return_value = None
        client = HNClient()

        result = client.get_top_stories()

        check.equal(result, [])


class TestGetItem:
    """Tests para el método get_item."""

    @patch("ingestion.hn_client.HNClient._make_request")
    def test_get_item_should_return_story_dict_when_item_exists(self, mock_request):
        """Verifica que retorna un diccionario con datos de la historia."""
        mock_story = {"id": 123, "type": "story", "title": "Test Story", "score": 100}
        mock_request.return_value = mock_story
        client = HNClient()

        result = client.get_item(123)

        check.is_instance(result, dict)
        check.equal(result["id"], 123)
        check.equal(result["type"], "story")
        mock_request.assert_called_once_with("item/123.json")

    @patch("ingestion.hn_client.HNClient._make_request")
    def test_get_item_should_return_none_when_item_not_found(self, mock_request):
        """Verifica que retorna None cuando el item no existe."""
        mock_request.return_value = None
        client = HNClient()

        result = client.get_item(999999)

        check.is_none(result)


class TestRateLimiting:
    """Tests para el sistema de rate limiting."""

    @patch("time.sleep")
    @patch("time.time")
    def test_wait_for_rate_limit_should_sleep_when_request_too_fast(
        self, mock_time, mock_sleep
    ):
        """Verifica que espera el tiempo necesario para respetar rate limit."""
        client = HNClient()
        client.last_request_time = 100.0
        mock_time.return_value = 100.5  # 0.5 segundos después

        client._wait_for_rate_limit()

        # Debe dormir 0.5 segundos para completar el delay de 1.0
        mock_sleep.assert_called_once()
        check.almost_equal(mock_sleep.call_args[0][0], 0.5, rel=0.01)

    @patch("time.sleep")
    @patch("time.time")
    def test_wait_for_rate_limit_should_not_sleep_when_enough_time_passed(
        self, mock_time, mock_sleep
    ):
        """Verifica que no espera si ya pasó suficiente tiempo."""
        client = HNClient()
        client.last_request_time = 100.0
        mock_time.return_value = 101.5  # 1.5 segundos después

        client._wait_for_rate_limit()

        mock_sleep.assert_not_called()


class TestRetryLogic:
    """Tests para la lógica de reintentos con exponential backoff."""

    @patch("time.sleep")
    @patch("ingestion.hn_client.HNClient._wait_for_rate_limit")
    def test_make_request_should_retry_on_500_error_and_succeed(
        self, mock_rate_limit, mock_sleep
    ):
        """Verifica que reintenta en error 500 y eventualmente tiene éxito."""
        client = HNClient(max_retries=3)

        with patch.object(client.session, "get") as mock_get:
            # Primera llamada: error 500, segunda: éxito
            response_error = Mock()
            response_error.status_code = 500

            response_success = Mock()
            response_success.status_code = 200
            response_success.json.return_value = {"id": 123}

            mock_get.side_effect = [response_error, response_success]

            result = client._make_request("test.json")

            check.equal(result, {"id": 123})
            check.equal(mock_get.call_count, 2)
            mock_sleep.assert_called_once_with(1)  # 2^0 = 1

    @patch("time.sleep")
    @patch("ingestion.hn_client.HNClient._wait_for_rate_limit")
    def test_make_request_should_return_none_after_max_retries(
        self, mock_rate_limit, mock_sleep
    ):
        """Verifica que retorna None después de agotar todos los reintentos."""
        client = HNClient(max_retries=3)

        with patch.object(client.session, "get") as mock_get:
            # Todas las llamadas fallan con 500
            response_error = Mock()
            response_error.status_code = 500
            mock_get.return_value = response_error

            result = client._make_request("test.json")

            check.is_none(result)
            check.equal(mock_get.call_count, 3)
            # Debe haber dormido 2 veces: 2^0=1, 2^1=2 (no duerme en el último intento)
            check.equal(mock_sleep.call_count, 2)

    @patch("ingestion.hn_client.HNClient._wait_for_rate_limit")
    def test_make_request_should_return_none_on_404_without_retry(
        self, mock_rate_limit
    ):
        """Verifica que retorna None en 404 sin reintentar (item deleted)."""
        client = HNClient(max_retries=3)

        with patch.object(client.session, "get") as mock_get:
            response_404 = Mock()
            response_404.status_code = 404
            mock_get.return_value = response_404

            result = client._make_request("item/999.json")

            check.is_none(result)
            check.equal(mock_get.call_count, 1)  # No reintenta en 404

    @patch("time.sleep")
    @patch("ingestion.hn_client.HNClient._wait_for_rate_limit")
    def test_make_request_should_handle_timeout_with_exponential_backoff(
        self, mock_rate_limit, mock_sleep
    ):
        """Verifica que maneja timeouts con exponential backoff."""
        client = HNClient(max_retries=3)

        with patch.object(client.session, "get") as mock_get:
            mock_get.side_effect = requests.Timeout("Connection timeout")

            result = client._make_request("test.json")

            check.is_none(result)
            check.equal(mock_get.call_count, 3)
            # Debe dormir con backoff exponencial: 1, 2 segundos
            check.equal(mock_sleep.call_count, 2)
            check.equal(mock_sleep.call_args_list[0][0][0], 1)  # 2^0
            check.equal(mock_sleep.call_args_list[1][0][0], 2)  # 2^1
