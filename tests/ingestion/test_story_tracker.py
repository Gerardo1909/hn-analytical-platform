"""
Tests unitarios para StoryTracker.
Verifica tracking temporal, actualización de métricas y reglas de limpieza.
"""

import json
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest
import pytest_check as check

from ingestion.story_tracker import StoryTracker


@pytest.fixture
def mock_s3_client():
    """Fixture que retorna un mock del cliente S3."""
    return Mock()


@pytest.fixture
def story_tracker(mock_s3_client):
    """Fixture que retorna una instancia de StoryTracker con cliente mockeado."""
    return StoryTracker(
        bucket_name="test-bucket", s3_client=mock_s3_client, tracking_days=7
    )


class TestLoadActiveStories:
    """Tests para el método load_active_stories."""

    def test_load_should_return_tracking_data_when_file_exists(
        self, story_tracker, mock_s3_client
    ):
        """Verifica que carga correctamente los datos de tracking desde S3."""
        tracking_data = {
            "last_updated": "2026-02-01T10:00:00",
            "total_stories": 2,
            "stories": {
                "123": {
                    "first_seen": "2026-01-30",
                    "last_updated": "2026-02-01",
                    "last_score": 150,
                    "last_descendants": 45,
                },
                "456": {
                    "first_seen": "2026-01-29",
                    "last_updated": "2026-01-31",
                    "last_score": 200,
                    "last_descendants": 60,
                },
            },
        }

        mock_body = Mock()
        mock_body.read.return_value = json.dumps(tracking_data).encode("utf-8")
        mock_response = {"Body": mock_body}
        mock_s3_client.get_object.return_value = mock_response

        result = story_tracker.load_active_stories()

        check.is_instance(result, dict)
        check.equal(len(result), 2)
        check.is_in(123, result)  # Keys convertidos a int
        check.is_in(456, result)
        check.equal(result[123]["last_score"], 150)
        check.equal(result[456]["last_descendants"], 60)

    def test_load_should_return_empty_dict_when_file_not_found(
        self, story_tracker, mock_s3_client
    ):
        """Verifica que retorna dict vacío cuando no existe tracking previo."""

        class NoSuchKey(Exception):
            pass

        mock_s3_client.exceptions = type("Exceptions", (), {"NoSuchKey": NoSuchKey})
        mock_s3_client.get_object.side_effect = NoSuchKey("Not found")

        result = story_tracker.load_active_stories()

        check.equal(result, {})


class TestUpdateTracking:
    """Tests para el método update_tracking."""

    def test_update_should_add_new_stories_when_discovered(self, story_tracker):
        """Verifica que agrega nuevas historias al tracking."""
        existing_tracked = {}
        new_story_ids = {100, 200}
        story_metrics = {
            100: {"score": 50, "descendants": 10},
            200: {"score": 75, "descendants": 20},
        }

        result = story_tracker.update_tracking(
            existing_tracked, new_story_ids, story_metrics
        )

        check.equal(len(result), 2)
        check.is_in(100, result)
        check.is_in(200, result)
        check.equal(result[100]["last_score"], 50)
        check.equal(result[200]["last_descendants"], 20)

    def test_update_should_update_existing_story_when_metrics_changed(
        self, story_tracker
    ):
        """Verifica que actualiza historias existentes cuando hay cambios significativos."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        existing_tracked = {
            100: {
                "first_seen": "2026-01-30",
                "last_updated": "2026-01-31",
                "last_score": 50,
                "last_descendants": 10,
            }
        }
        new_story_ids = set()
        story_metrics = {
            100: {"score": 75, "descendants": 10}  # Score cambió
        }

        result = story_tracker.update_tracking(
            existing_tracked, new_story_ids, story_metrics
        )

        check.equal(len(result), 1)
        check.equal(result[100]["last_score"], 75)
        check.equal(result[100]["last_updated"], today)
        check.equal(result[100]["first_seen"], "2026-01-30")  # first_seen no cambia

    def test_update_should_keep_story_when_no_changes_within_tracking_days(
        self, story_tracker
    ):
        """Verifica que mantiene historia sin cambios si está dentro del período."""
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        existing_tracked = {
            100: {
                "first_seen": "2026-01-20",
                "last_updated": yesterday,
                "last_score": 50,
                "last_descendants": 10,
            }
        }
        new_story_ids = set()
        story_metrics = {
            100: {"score": 50, "descendants": 10}  # Sin cambios
        }

        result = story_tracker.update_tracking(
            existing_tracked, new_story_ids, story_metrics
        )

        check.equal(len(result), 1)
        check.is_in(100, result)
        check.equal(result[100]["last_updated"], yesterday)  # No se actualiza

    def test_update_should_remove_story_when_no_changes_exceeds_tracking_days(
        self, story_tracker
    ):
        """Verifica que remueve historia cuando excede días sin cambios."""
        old_date = (datetime.utcnow() - timedelta(days=10)).strftime("%Y-%m-%d")
        existing_tracked = {
            100: {
                "first_seen": "2026-01-01",
                "last_updated": old_date,
                "last_score": 50,
                "last_descendants": 10,
            }
        }
        new_story_ids = set()
        story_metrics = {
            100: {"score": 50, "descendants": 10}  # Sin cambios
        }

        result = story_tracker.update_tracking(
            existing_tracked, new_story_ids, story_metrics
        )

        check.equal(len(result), 0)  # Historia removida

    def test_update_should_detect_significant_changes_when_comments_increase(
        self, story_tracker
    ):
        """Verifica que detecta cambios significativos cuando aumentan comentarios."""
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        existing_tracked = {
            100: {
                "first_seen": "2026-01-30",
                "last_updated": yesterday,
                "last_score": 50,
                "last_descendants": 10,
            }
        }
        new_story_ids = set()
        story_metrics = {
            100: {"score": 50, "descendants": 20}  # +10 comentarios
        }

        result = story_tracker.update_tracking(
            existing_tracked, new_story_ids, story_metrics
        )

        today = datetime.utcnow().strftime("%Y-%m-%d")
        check.equal(result[100]["last_updated"], today)
        check.equal(result[100]["last_descendants"], 20)

    def test_update_should_not_duplicate_when_story_in_both_existing_and_new(
        self, story_tracker
    ):
        """Verifica que no duplica historia si está en existing y new_story_ids."""
        existing_tracked = {
            100: {
                "first_seen": "2026-01-30",
                "last_updated": "2026-01-31",
                "last_score": 50,
                "last_descendants": 10,
            }
        }
        new_story_ids = {100}  # Misma historia como "nueva"
        story_metrics = {100: {"score": 75, "descendants": 15}}

        result = story_tracker.update_tracking(
            existing_tracked, new_story_ids, story_metrics
        )

        # Debe haber solo 1 entrada para el ID 100
        check.equal(len(result), 1)
        check.is_in(100, result)


class TestSaveTracking:
    """Tests para el método save_tracking."""

    def test_save_should_persist_tracking_data_to_s3_when_called(
        self, story_tracker, mock_s3_client
    ):
        """Verifica que persiste correctamente los datos de tracking en S3."""
        tracking_data = {
            123: {
                "first_seen": "2026-01-30",
                "last_updated": "2026-02-01",
                "last_score": 150,
                "last_descendants": 45,
            },
            456: {
                "first_seen": "2026-01-29",
                "last_updated": "2026-01-31",
                "last_score": 200,
                "last_descendants": 60,
            },
        }

        story_tracker.save_tracking(tracking_data)

        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args

        check.equal(call_args.kwargs["Bucket"], "test-bucket")
        check.equal(call_args.kwargs["Key"], "metadata/story_tracking.json")
        check.equal(call_args.kwargs["ContentType"], "application/json")

        # Verificar contenido JSON
        saved_content = json.loads(call_args.kwargs["Body"])
        check.is_in("last_updated", saved_content)
        check.equal(saved_content["total_stories"], 2)
        check.is_in("123", saved_content["stories"])  # Keys como strings
        check.is_in("456", saved_content["stories"])

    def test_save_should_include_metadata_when_persisting(
        self, story_tracker, mock_s3_client
    ):
        """Verifica que incluye metadata correcta al guardar."""
        tracking_data = {
            100: {"first_seen": "2026-01-30", "last_updated": "2026-02-01"}
        }

        story_tracker.save_tracking(tracking_data)

        call_args = mock_s3_client.put_object.call_args
        check.is_in("Metadata", call_args.kwargs)
        check.equal(call_args.kwargs["Metadata"]["total_stories"], "1")


class TestHasSignificantChanges:
    """Tests para el método privado _has_significant_changes."""

    def test_should_return_true_when_score_changed(self, story_tracker):
        """Verifica que detecta cambio cuando el score es diferente."""
        old_metadata = {"last_score": 50, "last_descendants": 10}
        current_metrics = {"score": 75, "descendants": 10}

        result = story_tracker._has_significant_changes(old_metadata, current_metrics)

        check.is_true(result)

    def test_should_return_true_when_comments_increase_by_five_or_more(
        self, story_tracker
    ):
        """Verifica que detecta cambio cuando aumentan 5+ comentarios."""
        old_metadata = {"last_score": 50, "last_descendants": 10}
        current_metrics = {"score": 50, "descendants": 15}

        result = story_tracker._has_significant_changes(old_metadata, current_metrics)

        check.is_true(result)

    def test_should_return_false_when_comments_increase_by_less_than_five(
        self, story_tracker
    ):
        """Verifica que no detecta cambio cuando aumentan menos de 5 comentarios."""
        old_metadata = {"last_score": 50, "last_descendants": 10}
        current_metrics = {"score": 50, "descendants": 13}

        result = story_tracker._has_significant_changes(old_metadata, current_metrics)

        check.is_false(result)

    def test_should_return_false_when_no_changes(self, story_tracker):
        """Verifica que no detecta cambio cuando métricas son idénticas."""
        old_metadata = {"last_score": 50, "last_descendants": 10}
        current_metrics = {"score": 50, "descendants": 10}

        result = story_tracker._has_significant_changes(old_metadata, current_metrics)

        check.is_false(result)


class TestShouldKeepTracking:
    """Tests para el método privado _should_keep_tracking."""

    def test_should_return_true_when_within_tracking_days(self, story_tracker):
        """Verifica que mantiene tracking si está dentro del período."""
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        today = datetime.utcnow().strftime("%Y-%m-%d")
        metadata = {"last_updated": yesterday}

        result = story_tracker._should_keep_tracking(metadata, today)

        check.is_true(result)

    def test_should_return_false_when_exceeds_tracking_days(self, story_tracker):
        """Verifica que remueve tracking si excede el período."""
        old_date = (datetime.utcnow() - timedelta(days=10)).strftime("%Y-%m-%d")
        today = datetime.utcnow().strftime("%Y-%m-%d")
        metadata = {"last_updated": old_date}

        result = story_tracker._should_keep_tracking(metadata, today)

        check.is_false(result)

    def test_should_return_true_when_no_last_updated_field(self, story_tracker):
        """Verifica que mantiene tracking si no hay campo last_updated."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        metadata = {}

        result = story_tracker._should_keep_tracking(metadata, today)

        check.is_true(result)


class TestGetTrackedStoryIds:
    """Tests para el método get_tracked_story_ids."""

    def test_should_return_set_of_ids_when_tracking_data_provided(self, story_tracker):
        """Verifica que retorna un set de IDs desde tracking data."""
        tracking_data = {
            100: {"first_seen": "2026-01-30"},
            200: {"first_seen": "2026-01-29"},
            300: {"first_seen": "2026-01-28"},
        }

        result = story_tracker.get_tracked_story_ids(tracking_data)

        check.is_instance(result, set)
        check.equal(len(result), 3)
        check.is_in(100, result)
        check.is_in(200, result)
        check.is_in(300, result)

    def test_should_load_from_s3_when_no_tracking_data_provided(
        self, story_tracker, mock_s3_client
    ):
        """Verifica que carga desde S3 cuando no se provee tracking data."""
        tracking_data = {
            "last_updated": "2026-02-01",
            "total_stories": 1,
            "stories": {"100": {"first_seen": "2026-01-30"}},
        }

        mock_body = Mock()
        mock_body.read.return_value = json.dumps(tracking_data).encode("utf-8")
        mock_response = {"Body": mock_body}
        mock_s3_client.get_object.return_value = mock_response

        result = story_tracker.get_tracked_story_ids()

        check.equal(len(result), 1)
        check.is_in(100, result)
        mock_s3_client.get_object.assert_called_once()

    def test_should_return_empty_set_when_tracking_data_empty(self, story_tracker):
        """Verifica que retorna set vacío cuando tracking data está vacío."""
        result = story_tracker.get_tracked_story_ids({})

        check.is_instance(result, set)
        check.equal(len(result), 0)
