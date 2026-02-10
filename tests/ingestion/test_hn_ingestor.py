"""
Tests unitarios para HNIngestor.
Verifica el flujo completo de ingesta, deduplicación y coordinación de componentes.
"""

from unittest.mock import Mock

import pytest
import pytest_check as check

from ingestion.hn_ingestor import HNIngestor


@pytest.fixture
def mock_api_client():
    """Fixture que retorna un mock del API client."""
    return Mock()


@pytest.fixture
def mock_fetcher():
    """Fixture que retorna un mock del fetcher."""
    return Mock()


@pytest.fixture
def mock_tracker():
    """Fixture que retorna un mock del tracker."""
    return Mock()


@pytest.fixture
def mock_writer():
    """Fixture que retorna un mock del writer."""
    return Mock()


@pytest.fixture
def hn_ingestor(mock_api_client, mock_fetcher, mock_tracker, mock_writer):
    """Fixture que retorna una instancia de HNIngestor con mocks."""
    return HNIngestor(
        api_client=mock_api_client,
        fetcher=mock_fetcher,
        tracker=mock_tracker,
        writer=mock_writer,
        max_stories=100,
    )


class TestIngestMethod:
    """Tests para el método principal ingest."""

    def test_ingest_should_return_stats_when_successful(
        self, hn_ingestor, mock_fetcher, mock_tracker, mock_api_client
    ):
        """Verifica que retorna estadísticas correctas al completar ingesta."""
        # Setup mocks
        mock_tracker.load_active_stories.return_value = {}
        mock_tracker.get_tracked_story_ids.return_value = set()
        mock_fetcher.fetch_top_stories_from_last_week.return_value = [
            {"id": 1, "type": "story", "score": 100, "descendants": 10},
            {"id": 2, "type": "story", "score": 200, "descendants": 20},
        ]
        mock_tracker.update_tracking.return_value = {1: {}, 2: {}}
        mock_fetcher.fetch_comments_for_story.side_effect = lambda story: iter([])

        result = hn_ingestor.ingest()

        check.is_instance(result, dict)
        check.is_in("total_stories", result)
        check.is_in("new_stories", result)
        check.is_in("updated_stories", result)
        check.is_in("total_comments", result)
        check.is_in("active_tracked_stories", result)

    def test_ingest_should_coordinate_all_components_when_executed(
        self, hn_ingestor, mock_fetcher, mock_tracker, mock_writer, mock_api_client
    ):
        """Verifica que coordina llamadas a todos los componentes en orden."""
        # Setup mocks
        mock_tracker.load_active_stories.return_value = {}
        mock_tracker.get_tracked_story_ids.return_value = set()
        mock_fetcher.fetch_top_stories_from_last_week.return_value = [
            {"id": 1, "type": "story", "score": 100, "descendants": 10}
        ]
        mock_tracker.update_tracking.return_value = {1: {}}
        mock_fetcher.fetch_comments_for_story.side_effect = lambda story: iter([])

        hn_ingestor.ingest()

        # Verificar que se llamaron todos los componentes
        mock_tracker.load_active_stories.assert_called_once()
        mock_fetcher.fetch_top_stories_from_last_week.assert_called_once_with(
            max_stories=100
        )
        mock_tracker.update_tracking.assert_called_once()
        mock_tracker.save_tracking.assert_called_once()
        mock_writer.save.assert_called()  # Llamado al menos una vez


class TestLoadTracking:
    """Tests para el método _load_tracking."""

    def test_load_tracking_should_return_tracking_metadata_when_exists(
        self, hn_ingestor, mock_tracker
    ):
        """Verifica que carga correctamente el tracking metadata."""
        mock_metadata = {
            100: {"first_seen": "2026-01-30", "last_updated": "2026-02-01"}
        }
        mock_tracker.load_active_stories.return_value = mock_metadata
        mock_tracker.get_tracked_story_ids.return_value = {100}

        result = hn_ingestor._load_tracking()

        check.equal(result, mock_metadata)
        mock_tracker.load_active_stories.assert_called_once()
        mock_tracker.get_tracked_story_ids.assert_called_once_with(mock_metadata)


class TestReingestTrackedStories:
    """Tests para el método _reingest_tracked_stories."""

    def test_reingest_should_fetch_all_tracked_stories_when_called(
        self, hn_ingestor, mock_api_client, mock_tracker
    ):
        """Verifica que re-ingesta todas las historias en tracking."""
        tracked_metadata = {
            100: {"first_seen": "2026-01-30"},
            200: {"first_seen": "2026-01-29"},
        }
        mock_tracker.get_tracked_story_ids.return_value = {100, 200}
        mock_api_client.get_item.side_effect = [
            {"id": 100, "type": "story", "score": 150},
            {"id": 200, "type": "story", "score": 200},
        ]

        result = hn_ingestor._reingest_tracked_stories(tracked_metadata)

        check.equal(len(result), 2)
        check.equal(result[0]["id"], 100)
        check.equal(result[1]["id"], 200)
        check.equal(mock_api_client.get_item.call_count, 2)

    def test_reingest_should_skip_none_items_when_api_returns_none(
        self, hn_ingestor, mock_api_client, mock_tracker
    ):
        """Verifica que omite historias que retornan None."""
        tracked_metadata = {100: {}, 200: {}, 300: {}}
        mock_tracker.get_tracked_story_ids.return_value = {100, 200, 300}
        mock_api_client.get_item.side_effect = [
            {"id": 100, "type": "story"},
            None,  # Historia 200 no encontrada
            {"id": 300, "type": "story"},
        ]

        result = hn_ingestor._reingest_tracked_stories(tracked_metadata)

        check.equal(len(result), 2)
        check.equal(result[0]["id"], 100)
        check.equal(result[1]["id"], 300)

    def test_reingest_should_filter_only_story_type_when_mixed_types(
        self, hn_ingestor, mock_api_client, mock_tracker
    ):
        """Verifica que solo incluye items de tipo 'story'."""
        tracked_metadata = {100: {}, 200: {}}
        mock_tracker.get_tracked_story_ids.return_value = {100, 200}
        mock_api_client.get_item.side_effect = [
            {"id": 100, "type": "story"},
            {"id": 200, "type": "comment"},  # No es story
        ]

        result = hn_ingestor._reingest_tracked_stories(tracked_metadata)

        check.equal(len(result), 1)
        check.equal(result[0]["type"], "story")


class TestFetchNewStories:
    """Tests para el método _fetch_new_stories."""

    def test_fetch_new_should_call_fetcher_with_max_stories_when_executed(
        self, hn_ingestor, mock_fetcher
    ):
        """Verifica que llama al fetcher con el límite correcto."""
        mock_fetcher.fetch_top_stories_from_last_week.return_value = []

        hn_ingestor._fetch_new_stories()

        mock_fetcher.fetch_top_stories_from_last_week.assert_called_once_with(
            max_stories=100
        )


class TestUpdateTracking:
    """Tests para el método _update_tracking."""

    def test_update_tracking_should_persist_after_updating_when_called(
        self, hn_ingestor, mock_tracker
    ):
        """Verifica que persiste el tracking después de actualizar."""
        updated_tracking = {1: {}, 2: {}}
        mock_tracker.update_tracking.return_value = updated_tracking

        hn_ingestor._update_tracking({}, set(), [])

        mock_tracker.save_tracking.assert_called_once_with(updated_tracking)


class TestDeduplication:
    """Tests para la lógica de deduplicación en ingest."""

    def test_ingest_should_deduplicate_stories_when_same_id_in_both_lists(
        self, hn_ingestor, mock_fetcher, mock_tracker, mock_api_client, mock_writer
    ):
        """Verifica que deduplica historias con mismo ID en updated y new."""
        # Setup: historia 100 está en tracking Y en top stories
        mock_tracker.load_active_stories.return_value = {100: {}}
        mock_tracker.get_tracked_story_ids.side_effect = [
            {100},  # Primera llamada en _load_tracking
            {100},  # Segunda llamada en ingest() línea 66
            {100},  # Tercera llamada en _reingest_tracked_stories
            {100},  # Cuarta llamada en _get_active_stories
        ]
        mock_api_client.get_item.return_value = {
            "id": 100,
            "type": "story",
            "score": 200,
            "descendants": 50,
        }
        mock_fetcher.fetch_top_stories_from_last_week.return_value = [
            {"id": 100, "type": "story", "score": 150, "descendants": 40}
        ]
        mock_tracker.update_tracking.return_value = {100: {}}
        mock_fetcher.fetch_comments_for_story.side_effect = lambda story: iter([])

        result = hn_ingestor.ingest()

        # Debe haber solo 1 historia guardada (deduplicada)
        # Verificar que save fue llamado con 1 historia
        save_calls = [
            call
            for call in mock_writer.save.call_args_list
            if call[1]["entity"] == "stories"
        ]
        check.equal(len(save_calls), 1)
        saved_stories = save_calls[0][1]["data"]
        check.equal(len(saved_stories), 1)
        check.equal(saved_stories[0]["id"], 100)
        # Debe preferir la versión de updated_stories (score 200)
        check.equal(saved_stories[0]["score"], 200)

    def test_ingest_should_calculate_newly_discovered_correctly_when_deduplicating(
        self, hn_ingestor, mock_fetcher, mock_tracker, mock_api_client
    ):
        """Verifica que calcula correctamente las nuevas IDs descubiertas."""
        mock_tracker.load_active_stories.return_value = {100: {}}
        mock_tracker.get_tracked_story_ids.side_effect = [
            {100},  # Primera llamada en _load_tracking
            {100},  # Segunda llamada en ingest() línea 66
            {100},  # Tercera llamada en _reingest_tracked_stories
            {100, 200},  # Cuarta llamada en _get_active_stories (después de update)
        ]
        mock_api_client.get_item.return_value = {"id": 100, "type": "story"}
        mock_fetcher.fetch_top_stories_from_last_week.return_value = [
            {"id": 100, "type": "story"},  # Ya estaba en tracking
            {"id": 200, "type": "story"},  # Nueva
        ]
        mock_tracker.update_tracking.return_value = {100: {}, 200: {}}
        mock_fetcher.fetch_comments_for_story.side_effect = lambda story: iter([])

        result = hn_ingestor.ingest()

        # Solo 200 es realmente nueva
        check.equal(result["new_stories"], 1)


class TestSaveStories:
    """Tests para el método _save_stories."""

    def test_save_stories_should_call_writer_with_correct_params_when_stories_exist(
        self, hn_ingestor, mock_writer
    ):
        """Verifica que llama al writer con parámetros correctos."""
        stories = [{"id": 1}, {"id": 2}]

        hn_ingestor._save_stories(stories)

        mock_writer.save.assert_called_once_with(
            layer="raw",
            entity="stories",
            data=stories,
            format="json",
            additional_metadata={"entity_type": "story"},
        )

    def test_save_stories_should_not_call_writer_when_stories_empty(
        self, hn_ingestor, mock_writer
    ):
        """Verifica que no llama al writer si la lista está vacía."""
        hn_ingestor._save_stories([])

        mock_writer.save.assert_not_called()


class TestGetActiveStories:
    """Tests para el método _get_active_stories."""

    def test_get_active_should_filter_by_tracking_ids_when_called(
        self, hn_ingestor, mock_tracker
    ):
        """Verifica que filtra historias según IDs en tracking."""
        all_stories = [
            {"id": 1, "title": "Story 1"},
            {"id": 2, "title": "Story 2"},
            {"id": 3, "title": "Story 3"},
        ]
        updated_tracking = {1: {}, 3: {}}  # Solo 1 y 3 están activas
        mock_tracker.get_tracked_story_ids.return_value = {1, 3}

        result = hn_ingestor._get_active_stories(all_stories, updated_tracking)

        check.equal(len(result), 2)
        check.equal(result[0]["id"], 1)
        check.equal(result[1]["id"], 3)


class TestFetchAndSaveComments:
    """Tests para el método _fetch_and_save_comments."""

    def test_fetch_comments_should_process_all_active_stories_when_called(
        self, hn_ingestor, mock_fetcher, mock_writer
    ):
        """Verifica que procesa comentarios de todas las historias activas."""
        active_stories = [{"id": 100}, {"id": 200}]
        mock_fetcher.fetch_comments_for_story.side_effect = [
            iter([{"id": 1001}, {"id": 1002}]),
            iter([{"id": 2001}]),
        ]

        total = hn_ingestor._fetch_and_save_comments(active_stories)

        check.equal(total, 3)
        check.equal(mock_fetcher.fetch_comments_for_story.call_count, 2)

    def test_fetch_comments_should_save_in_batches_when_exceeds_batch_size(
        self, hn_ingestor, mock_fetcher, mock_writer
    ):
        """Verifica que guarda en batches cuando excede BATCH_SIZE."""
        hn_ingestor.BATCH_SIZE = 2  # Reducir para testing
        active_stories = [{"id": 100}]
        comments = [{"id": i} for i in range(5)]  # 5 comentarios
        mock_fetcher.fetch_comments_for_story.return_value = iter(comments)

        hn_ingestor._fetch_and_save_comments(active_stories)

        # Debe haber guardado 3 veces: 2 batches de 2 + 1 batch de 1
        check.equal(mock_writer.save.call_count, 3)

    def test_fetch_comments_should_handle_empty_comments_when_story_has_none(
        self, hn_ingestor, mock_fetcher, mock_writer
    ):
        """Verifica que maneja historias sin comentarios correctamente."""
        active_stories = [{"id": 100}]
        mock_fetcher.fetch_comments_for_story.return_value = iter([])

        total = hn_ingestor._fetch_and_save_comments(active_stories)

        check.equal(total, 0)
        mock_writer.save.assert_not_called()


class TestSaveCommentBatch:
    """Tests para el método _save_comment_batch."""

    def test_save_batch_should_include_parent_story_id_when_saving(
        self, hn_ingestor, mock_writer
    ):
        """Verifica que incluye parent_story_id en metadata."""
        batch = [{"id": 1001}, {"id": 1002}]
        story_id = 100

        hn_ingestor._save_comment_batch(batch, story_id)

        call_args = mock_writer.save.call_args
        check.equal(call_args[1]["layer"], "raw")
        check.equal(call_args[1]["entity"], "comments")
        check.equal(call_args[1]["data"], batch)
        check.equal(call_args[1]["additional_metadata"]["parent_story_id"], 100)
