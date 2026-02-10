"""
Tests unitarios para HNFetcher.
Verifica fetch de historias, filtrado por fecha, y generación de comentarios.
"""

from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest
import pytest_check as check

from ingestion.hn_fetcher import HNFetcher


@pytest.fixture
def mock_api_client():
    """Fixture que retorna un mock del API client."""
    return Mock()


@pytest.fixture
def hn_fetcher(mock_api_client):
    """Fixture que retorna una instancia de HNFetcher con cliente mockeado."""
    return HNFetcher(api_client=mock_api_client)


class TestFetchTopStoriesFromLastWeek:
    """Tests para el método fetch_top_stories_from_last_week."""

    def test_fetch_should_return_stories_from_last_week_when_available(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que retorna solo historias de la última semana."""
        # Timestamp de hace 3 días (dentro de la ventana)
        recent_time = int((datetime.utcnow() - timedelta(days=3)).timestamp())
        # Timestamp de hace 10 días (fuera de la ventana)
        old_time = int((datetime.utcnow() - timedelta(days=10)).timestamp())

        mock_api_client.get_top_stories.return_value = [1, 2, 3, 4]
        mock_api_client.get_item.side_effect = [
            {"id": 1, "type": "story", "time": recent_time, "title": "Recent Story 1"},
            {"id": 2, "type": "story", "time": old_time, "title": "Old Story"},
            {"id": 3, "type": "story", "time": recent_time, "title": "Recent Story 2"},
            {"id": 4, "type": "comment", "time": recent_time},  # No es story
        ]

        result = hn_fetcher.fetch_top_stories_from_last_week(max_stories=4)

        check.equal(len(result), 2)
        check.equal(result[0]["id"], 1)
        check.equal(result[1]["id"], 3)

    def test_fetch_should_respect_max_stories_limit_when_provided(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que respeta el límite de max_stories."""
        mock_api_client.get_top_stories.return_value = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        hn_fetcher.fetch_top_stories_from_last_week(max_stories=5)

        # Debe haber llamado solo a los primeros 5
        check.equal(mock_api_client.get_item.call_count, 5)

    def test_fetch_should_skip_none_items_when_api_returns_none(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que maneja correctamente items que no existen (None)."""
        recent_time = int((datetime.utcnow() - timedelta(days=2)).timestamp())

        mock_api_client.get_top_stories.return_value = [1, 2, 3]
        mock_api_client.get_item.side_effect = [
            {"id": 1, "type": "story", "time": recent_time},
            None,  # Item eliminado o no encontrado
            {"id": 3, "type": "story", "time": recent_time},
        ]

        result = hn_fetcher.fetch_top_stories_from_last_week(max_stories=3)

        check.equal(len(result), 2)
        check.equal(result[0]["id"], 1)
        check.equal(result[1]["id"], 3)

    def test_fetch_should_filter_by_type_story_when_mixed_types(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que filtra solo items de tipo 'story'."""
        recent_time = int((datetime.utcnow() - timedelta(days=2)).timestamp())

        mock_api_client.get_top_stories.return_value = [1, 2, 3, 4]
        mock_api_client.get_item.side_effect = [
            {"id": 1, "type": "story", "time": recent_time},
            {"id": 2, "type": "comment", "time": recent_time},
            {"id": 3, "type": "poll", "time": recent_time},
            {"id": 4, "type": "story", "time": recent_time},
        ]

        result = hn_fetcher.fetch_top_stories_from_last_week(max_stories=4)

        check.equal(len(result), 2)
        check.equal(result[0]["type"], "story")
        check.equal(result[1]["type"], "story")

    def test_fetch_should_raise_error_when_api_returns_none(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que lanza error cuando get_top_stories falla."""
        mock_api_client.get_top_stories.return_value = None

        with pytest.raises(RuntimeError) as exc_info:
            hn_fetcher.fetch_top_stories_from_last_week(max_stories=10)

        check.is_in("No se pudieron obtener historias", str(exc_info.value))

    def test_fetch_should_return_empty_list_when_no_stories_match_criteria(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que retorna lista vacía cuando ninguna historia cumple criterios."""
        old_time = int((datetime.utcnow() - timedelta(days=10)).timestamp())

        mock_api_client.get_top_stories.return_value = [1, 2, 3]
        mock_api_client.get_item.side_effect = [
            {"id": 1, "type": "story", "time": old_time},
            {"id": 2, "type": "story", "time": old_time},
            {"id": 3, "type": "comment", "time": old_time},
        ]

        result = hn_fetcher.fetch_top_stories_from_last_week(max_stories=3)

        check.equal(len(result), 0)


class TestFetchCommentsForStory:
    """Tests para el método fetch_comments_for_story."""

    def test_fetch_comments_should_return_generator_when_story_has_comments(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que retorna un generador de comentarios."""
        story = {"id": 100, "kids": [201, 202]}

        mock_api_client.get_item.side_effect = [
            {"id": 201, "type": "comment", "text": "Comment 1"},
            {"id": 202, "type": "comment", "text": "Comment 2"},
        ]

        result = hn_fetcher.fetch_comments_for_story(story)

        # Debe ser un generador
        check.is_true(hasattr(result, "__iter__"))
        check.is_true(hasattr(result, "__next__"))

        comments = list(result)
        check.equal(len(comments), 2)
        check.equal(comments[0]["id"], 201)
        check.equal(comments[1]["id"], 202)

    def test_fetch_comments_should_return_empty_when_story_has_no_kids(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que retorna vacío cuando la historia no tiene comentarios."""
        story = {"id": 100, "kids": []}

        result = list(hn_fetcher.fetch_comments_for_story(story))

        check.equal(len(result), 0)
        mock_api_client.get_item.assert_not_called()

    def test_fetch_comments_should_handle_nested_comments_recursively(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que obtiene comentarios anidados recursivamente."""
        story = {"id": 100, "kids": [201]}

        # Comment 201 tiene un reply (comment 301)
        mock_api_client.get_item.side_effect = [
            {"id": 201, "type": "comment", "text": "Parent", "kids": [301]},
            {"id": 301, "type": "comment", "text": "Child"},
        ]

        result = list(hn_fetcher.fetch_comments_for_story(story))

        check.equal(len(result), 2)
        check.equal(result[0]["id"], 201)
        check.equal(result[1]["id"], 301)

    def test_fetch_comments_should_stop_at_max_depth_when_reached(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que detiene la recursión al alcanzar max_depth."""
        story = {"id": 100, "kids": [201]}

        # Cadena profunda: 201 -> 301 -> 401
        mock_api_client.get_item.side_effect = [
            {"id": 201, "type": "comment", "kids": [301]},
            {"id": 301, "type": "comment", "kids": [401]},
            {"id": 401, "type": "comment", "kids": []},
        ]

        # max_depth=2: debe obtener 201 y 301, pero no 401
        result = list(hn_fetcher.fetch_comments_for_story(story, max_depth=2))

        check.equal(len(result), 2)
        check.equal(result[0]["id"], 201)
        check.equal(result[1]["id"], 301)

    def test_fetch_comments_should_skip_none_items_when_deleted(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que maneja comentarios eliminados (None) correctamente."""
        story = {"id": 100, "kids": [201, 202, 203]}

        mock_api_client.get_item.side_effect = [
            {"id": 201, "type": "comment", "text": "Comment 1"},
            None,  # Comentario eliminado
            {"id": 203, "type": "comment", "text": "Comment 3"},
        ]

        result = list(hn_fetcher.fetch_comments_for_story(story))

        check.equal(len(result), 2)
        check.equal(result[0]["id"], 201)
        check.equal(result[1]["id"], 203)

    def test_fetch_comments_should_filter_only_comment_type_when_mixed_types(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que solo procesa items de tipo 'comment'."""
        story = {"id": 100, "kids": [201, 202, 203]}

        mock_api_client.get_item.side_effect = [
            {"id": 201, "type": "comment", "text": "Comment"},
            {"id": 202, "type": "poll", "text": "Poll"},
            {"id": 203, "type": "comment", "text": "Another Comment"},
        ]

        result = list(hn_fetcher.fetch_comments_for_story(story))

        check.equal(len(result), 2)
        check.equal(result[0]["type"], "comment")
        check.equal(result[1]["type"], "comment")

    def test_fetch_comments_should_yield_comments_one_by_one_without_accumulating(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que yielde comentarios uno por uno (memoria constante)."""
        story = {"id": 100, "kids": [201, 202]}

        mock_api_client.get_item.side_effect = [
            {"id": 201, "type": "comment"},
            {"id": 202, "type": "comment"},
        ]

        generator = hn_fetcher.fetch_comments_for_story(story)

        # Debe poder obtener el primer elemento sin consumir el segundo
        first = next(generator)
        check.equal(first["id"], 201)

        second = next(generator)
        check.equal(second["id"], 202)

        # No debe haber más elementos
        with pytest.raises(StopIteration):
            next(generator)


class TestFetchCommentsRecursive:
    """Tests para el método privado _fetch_comments_recursive."""

    def test_recursive_should_handle_multiple_branches_correctly(
        self, hn_fetcher, mock_api_client
    ):
        """Verifica que maneja múltiples ramas de comentarios correctamente."""
        # Estructura:
        #   201 -> 301, 302
        #   202 -> 303
        mock_api_client.get_item.side_effect = [
            {"id": 201, "type": "comment", "kids": [301, 302]},
            {"id": 301, "type": "comment"},
            {"id": 302, "type": "comment"},
            {"id": 202, "type": "comment", "kids": [303]},
            {"id": 303, "type": "comment"},
        ]

        result = list(
            hn_fetcher._fetch_comments_recursive([201, 202], depth=0, max_depth=10)
        )

        check.equal(len(result), 5)
        comment_ids = [c["id"] for c in result]
        check.is_in(201, comment_ids)
        check.is_in(301, comment_ids)
        check.is_in(302, comment_ids)
        check.is_in(202, comment_ids)
        check.is_in(303, comment_ids)
