"""
Tests unitarios para HNProcessor.
Verifica normalización, deduplicación, integridad referencial y flujo completo.
"""

from unittest.mock import Mock

import pandas as pd
import pytest
import pytest_check as check

from processing.hn_processor import HNProcessor


@pytest.fixture
def mock_loader():
    """Fixture que retorna un mock del loader."""
    return Mock()


@pytest.fixture
def mock_writer():
    """Fixture que retorna un mock del writer."""
    return Mock()


@pytest.fixture
def mock_quality_runner():
    """Fixture que retorna un mock del runner de calidad."""
    mock = Mock()
    report_mock = Mock()
    report_mock.to_dict.return_value = {}
    report_mock.has_critical_failures = False
    mock.run_story_checks.return_value = report_mock
    mock.run_comment_checks.return_value = report_mock
    return mock


@pytest.fixture
def hn_processor(mock_loader, mock_writer, mock_quality_runner):
    """Fixture que retorna una instancia de HNProcessor con mocks."""
    return HNProcessor(
        loader=mock_loader, writer=mock_writer, quality_runner=mock_quality_runner
    )


@pytest.fixture
def sample_raw_stories():
    """Fixture que retorna historias crudas de ejemplo como DataFrame."""
    return pd.DataFrame(
        [
            {
                "id": 100,
                "type": "story",
                "by": "user1",
                "time": 1706745600,
                "title": "Test Story 1",
                "url": "https://example.com/1",
                "text": None,
                "score": 150,
                "descendants": 45,
                "kids": [201, 202],
                "dead": None,
                "deleted": None,
            },
            {
                "id": 200,
                "type": "story",
                "by": "user2",
                "time": 1706832000,
                "title": "Test Story 2",
                "url": "https://example.com/2",
                "text": None,
                "score": 75,
                "descendants": 20,
                "kids": [203],
                "dead": None,
                "deleted": None,
            },
        ]
    )


@pytest.fixture
def sample_raw_comments():
    """Fixture que retorna comentarios crudos de ejemplo como DataFrame."""
    return pd.DataFrame(
        [
            {
                "id": 201,
                "type": "comment",
                "by": "commenter1",
                "time": 1706749200,
                "text": "Great story!",
                "parent": 100,
                "kids": [301],
                "dead": None,
                "deleted": None,
            },
            {
                "id": 202,
                "type": "comment",
                "by": "commenter2",
                "time": 1706752800,
                "text": "I disagree",
                "parent": 100,
                "kids": None,
                "dead": None,
                "deleted": None,
            },
            {
                "id": 301,
                "type": "comment",
                "by": "commenter3",
                "time": 1706756400,
                "text": "Reply to comment",
                "parent": 201,
                "kids": None,
                "dead": None,
                "deleted": None,
            },
        ]
    )


class TestNormalizeStories:
    """Tests para el método _normalize_stories."""

    def test_normalize_should_cast_id_to_int64_when_called(
        self, hn_processor, sample_raw_stories
    ):
        """Verifica que castea id a Int64."""
        result = hn_processor._normalize_stories(sample_raw_stories)

        check.equal(result["id"].dtype.name, "Int64")
        check.equal(result.iloc[0]["id"], 100)
        check.equal(result.iloc[1]["id"], 200)

    def test_normalize_should_convert_time_to_datetime_when_called(
        self, hn_processor, sample_raw_stories
    ):
        """Verifica que convierte time de Unix timestamp a datetime."""
        result = hn_processor._normalize_stories(sample_raw_stories)

        check.is_true(pd.api.types.is_datetime64_any_dtype(result["time"]))
        check.equal(result.iloc[0]["time"].year, 2024)

    def test_normalize_should_cast_score_to_int64_when_called(
        self, hn_processor, sample_raw_stories
    ):
        """Verifica que castea score a Int64."""
        result = hn_processor._normalize_stories(sample_raw_stories)

        check.equal(result["score"].dtype.name, "Int64")
        check.equal(result.iloc[0]["score"], 150)

    def test_normalize_should_cast_descendants_to_int64_when_called(
        self, hn_processor, sample_raw_stories
    ):
        """Verifica que castea descendants a Int64."""
        result = hn_processor._normalize_stories(sample_raw_stories)

        check.equal(result["descendants"].dtype.name, "Int64")
        check.equal(result.iloc[0]["descendants"], 45)

    def test_normalize_should_fill_missing_score_with_zero_when_null(
        self, hn_processor
    ):
        """Verifica que rellena score nulo con 0."""
        df = pd.DataFrame(
            [{"id": 100, "type": "story", "time": 1706745600, "score": None}]
        )

        result = hn_processor._normalize_stories(df)

        check.equal(result.iloc[0]["score"], 0)

    def test_normalize_should_fill_missing_descendants_with_zero_when_null(
        self, hn_processor
    ):
        """Verifica que rellena descendants nulo con 0."""
        df = pd.DataFrame(
            [{"id": 100, "type": "story", "time": 1706745600, "descendants": None}]
        )

        result = hn_processor._normalize_stories(df)

        check.equal(result.iloc[0]["descendants"], 0)

    def test_normalize_should_drop_rows_with_invalid_id_when_non_numeric(
        self, hn_processor
    ):
        """Verifica que descarta registros con id no numérico."""
        df = pd.DataFrame(
            [
                {"id": 100, "type": "story", "time": 1706745600},
                {"id": "invalid", "type": "story", "time": 1706745600},
                {"id": 200, "type": "story", "time": 1706745600},
            ]
        )

        result = hn_processor._normalize_stories(df)

        check.equal(len(result), 2)
        ids = result["id"].tolist()
        check.is_in(100, ids)
        check.is_in(200, ids)

    def test_normalize_should_cast_dead_to_boolean_when_present(self, hn_processor):
        """Verifica que castea dead a tipo boolean."""
        df = pd.DataFrame(
            [
                {"id": 100, "type": "story", "time": 1706745600, "dead": True},
                {"id": 200, "type": "story", "time": 1706745600, "dead": None},
            ]
        )

        result = hn_processor._normalize_stories(df)

        check.equal(result["dead"].dtype.name, "boolean")

    def test_normalize_should_keep_only_expected_columns_when_extra_present(
        self, hn_processor
    ):
        """Verifica que descarta columnas que no están en el esquema."""
        df = pd.DataFrame(
            [
                {
                    "id": 100,
                    "type": "story",
                    "time": 1706745600,
                    "extra_column": "should be dropped",
                    "another_extra": 42,
                }
            ]
        )

        result = hn_processor._normalize_stories(df)

        check.is_not_in("extra_column", result.columns)
        check.is_not_in("another_extra", result.columns)
        check.is_in("id", result.columns)

    def test_normalize_should_add_missing_columns_with_none_when_absent(
        self, hn_processor
    ):
        """Verifica que agrega columnas faltantes con None."""
        df = pd.DataFrame([{"id": 100, "type": "story", "time": 1706745600}])

        result = hn_processor._normalize_stories(df)

        check.is_in("url", result.columns)
        check.is_in("text", result.columns)
        check.is_in("kids", result.columns)
        check.is_true(pd.isna(result.iloc[0]["url"]))


class TestNormalizeComments:
    """Tests para el método _normalize_comments."""

    def test_normalize_should_cast_id_to_int64_when_called(
        self, hn_processor, sample_raw_comments
    ):
        """Verifica que castea id a Int64."""
        result = hn_processor._normalize_comments(sample_raw_comments)

        check.equal(result["id"].dtype.name, "Int64")
        check.equal(result.iloc[0]["id"], 201)

    def test_normalize_should_cast_parent_to_int64_when_called(
        self, hn_processor, sample_raw_comments
    ):
        """Verifica que castea parent a Int64."""
        result = hn_processor._normalize_comments(sample_raw_comments)

        check.equal(result["parent"].dtype.name, "Int64")
        check.equal(result.iloc[0]["parent"], 100)

    def test_normalize_should_convert_time_to_datetime_when_called(
        self, hn_processor, sample_raw_comments
    ):
        """Verifica que convierte time de Unix timestamp a datetime."""
        result = hn_processor._normalize_comments(sample_raw_comments)

        check.is_true(pd.api.types.is_datetime64_any_dtype(result["time"]))

    def test_normalize_should_drop_rows_with_invalid_id_when_non_numeric(
        self, hn_processor
    ):
        """Verifica que descarta comentarios con id inválido."""
        df = pd.DataFrame(
            [
                {"id": 201, "type": "comment", "time": 1706749200, "parent": 100},
                {"id": "bad", "type": "comment", "time": 1706749200, "parent": 100},
            ]
        )

        result = hn_processor._normalize_comments(df)

        check.equal(len(result), 1)
        check.equal(result.iloc[0]["id"], 201)

    def test_normalize_should_drop_rows_with_invalid_parent_when_non_numeric(
        self, hn_processor
    ):
        """Verifica que descarta comentarios con parent inválido."""
        df = pd.DataFrame(
            [
                {"id": 201, "type": "comment", "time": 1706749200, "parent": 100},
                {"id": 202, "type": "comment", "time": 1706749200, "parent": None},
            ]
        )

        result = hn_processor._normalize_comments(df)

        check.equal(len(result), 1)
        check.equal(result.iloc[0]["id"], 201)

    def test_normalize_should_keep_only_expected_columns_when_extra_present(
        self, hn_processor
    ):
        """Verifica que descarta columnas extra del esquema de comentarios."""
        df = pd.DataFrame(
            [
                {
                    "id": 201,
                    "type": "comment",
                    "time": 1706749200,
                    "parent": 100,
                    "extra": "discard me",
                }
            ]
        )

        result = hn_processor._normalize_comments(df)

        check.is_not_in("extra", result.columns)
        check.is_in("id", result.columns)
        check.is_in("parent", result.columns)


class TestSelectColumns:
    """Tests para el método _select_columns."""

    def test_select_should_keep_expected_columns_when_all_present(self, hn_processor):
        """Verifica que mantiene solo las columnas esperadas."""
        df = pd.DataFrame([{"a": 1, "b": 2, "c": 3}])

        result = hn_processor._select_columns(df, ["a", "b"])

        check.equal(list(result.columns), ["a", "b"])
        check.is_not_in("c", result.columns)

    def test_select_should_add_missing_columns_with_none_when_absent(
        self, hn_processor
    ):
        """Verifica que agrega columnas faltantes con None."""
        df = pd.DataFrame([{"a": 1}])

        result = hn_processor._select_columns(df, ["a", "b", "c"])

        check.equal(list(result.columns), ["a", "b", "c"])
        check.is_true(pd.isna(result.iloc[0]["b"]))
        check.is_true(pd.isna(result.iloc[0]["c"]))

    def test_select_should_return_copy_not_view_when_called(self, hn_processor):
        """Verifica que retorna una copia independiente del DataFrame original."""
        df = pd.DataFrame([{"a": 1, "b": 2}])

        result = hn_processor._select_columns(df, ["a", "b"])
        result["a"] = 999

        # El original no debe cambiar
        check.equal(df.iloc[0]["a"], 1)


class TestAddIngestionDate:
    """Tests para el método _add_ingestion_date."""

    def test_add_should_include_ingestion_date_column_when_called(self, hn_processor):
        """Verifica que agrega la columna ingestion_date."""
        df = pd.DataFrame([{"id": 100}, {"id": 200}])

        result = hn_processor._add_ingestion_date(df, "2026-02-01")

        check.is_in("ingestion_date", result.columns)
        check.equal(result.iloc[0]["ingestion_date"], "2026-02-01")
        check.equal(result.iloc[1]["ingestion_date"], "2026-02-01")


class TestDedup:
    """Tests para el método _dedup."""

    def test_dedup_should_remove_duplicates_by_id_and_date_when_present(
        self, hn_processor
    ):
        """Verifica que remueve duplicados por (id, ingestion_date)."""
        df = pd.DataFrame(
            [
                {"id": 100, "ingestion_date": "2026-02-01", "score": 50},
                {"id": 100, "ingestion_date": "2026-02-01", "score": 75},
                {"id": 200, "ingestion_date": "2026-02-01", "score": 30},
            ]
        )

        result = hn_processor._dedup(df, entity="stories")

        check.equal(len(result), 2)
        ids = result["id"].tolist()
        check.is_in(100, ids)
        check.is_in(200, ids)

    def test_dedup_should_keep_last_occurrence_when_duplicates_found(
        self, hn_processor
    ):
        """Verifica que mantiene la última ocurrencia al deduplicar."""
        df = pd.DataFrame(
            [
                {"id": 100, "ingestion_date": "2026-02-01", "score": 50},
                {"id": 100, "ingestion_date": "2026-02-01", "score": 75},
            ]
        )

        result = hn_processor._dedup(df, entity="stories")

        check.equal(len(result), 1)
        check.equal(result.iloc[0]["score"], 75)

    def test_dedup_should_preserve_different_dates_for_same_id_when_present(
        self, hn_processor
    ):
        """Verifica que no deduplicada registros del mismo id en fechas distintas."""
        df = pd.DataFrame(
            [
                {"id": 100, "ingestion_date": "2026-02-01", "score": 50},
                {"id": 100, "ingestion_date": "2026-02-02", "score": 75},
            ]
        )

        result = hn_processor._dedup(df, entity="stories")

        check.equal(len(result), 2)

    def test_dedup_should_return_unchanged_when_no_duplicates(self, hn_processor):
        """Verifica que retorna igual cuando no hay duplicados."""
        df = pd.DataFrame(
            [
                {"id": 100, "ingestion_date": "2026-02-01"},
                {"id": 200, "ingestion_date": "2026-02-01"},
                {"id": 300, "ingestion_date": "2026-02-01"},
            ]
        )

        result = hn_processor._dedup(df, entity="stories")

        check.equal(len(result), 3)

    def test_dedup_should_reset_index_when_rows_removed(self, hn_processor):
        """Verifica que resetea el índice después de deduplicar."""
        df = pd.DataFrame(
            [
                {"id": 100, "ingestion_date": "2026-02-01"},
                {"id": 100, "ingestion_date": "2026-02-01"},
                {"id": 200, "ingestion_date": "2026-02-01"},
            ]
        )

        result = hn_processor._dedup(df, entity="stories")

        check.equal(list(result.index), [0, 1])


class TestValidateReferentialIntegrity:
    """Tests para el método _validate_referential_integrity."""

    def test_validate_should_keep_comments_with_valid_story_parent_when_exists(
        self, hn_processor
    ):
        """Verifica que mantiene comentarios cuyo parent es una historia conocida."""
        stories_df = pd.DataFrame([{"id": 100}, {"id": 200}])
        comments_df = pd.DataFrame(
            [
                {"id": 201, "parent": 100},
                {"id": 202, "parent": 200},
            ]
        )

        valid_comments, orphaned = hn_processor._validate_referential_integrity(
            stories_df, comments_df
        )

        check.equal(len(valid_comments), 2)
        check.equal(orphaned, 0)

    def test_validate_should_keep_comments_with_valid_comment_parent_when_exists(
        self, hn_processor
    ):
        """Verifica que mantiene comentarios cuyo parent es otro comentario conocido."""
        stories_df = pd.DataFrame([{"id": 100}])
        comments_df = pd.DataFrame(
            [
                {"id": 201, "parent": 100},
                {"id": 301, "parent": 201},  # Parent es un comment, no una story
            ]
        )

        valid_comments, orphaned = hn_processor._validate_referential_integrity(
            stories_df, comments_df
        )

        check.equal(len(valid_comments), 2)
        check.equal(orphaned, 0)

    def test_validate_should_discard_orphaned_comments_when_parent_unknown(
        self, hn_processor
    ):
        """Verifica que descarta comentarios cuyo parent no existe en el batch."""
        stories_df = pd.DataFrame([{"id": 100}])
        comments_df = pd.DataFrame(
            [
                {"id": 201, "parent": 100},  # Válido
                {"id": 202, "parent": 999},  # Huérfano: parent 999 no existe
            ]
        )

        valid_comments, orphaned = hn_processor._validate_referential_integrity(
            stories_df, comments_df
        )

        check.equal(len(valid_comments), 1)
        check.equal(valid_comments.iloc[0]["id"], 201)
        check.equal(orphaned, 1)

    def test_validate_should_return_all_orphaned_count_when_multiple_orphans(
        self, hn_processor
    ):
        """Verifica que cuenta correctamente múltiples huérfanos."""
        stories_df = pd.DataFrame([{"id": 100}])
        comments_df = pd.DataFrame(
            [
                {"id": 201, "parent": 100},
                {"id": 202, "parent": 888},
                {"id": 203, "parent": 999},
                {"id": 204, "parent": 777},
            ]
        )

        valid_comments, orphaned = hn_processor._validate_referential_integrity(
            stories_df, comments_df
        )

        check.equal(len(valid_comments), 1)
        check.equal(orphaned, 3)

    def test_validate_should_reset_index_when_orphans_removed(self, hn_processor):
        """Verifica que resetea el índice después de remover huérfanos."""
        stories_df = pd.DataFrame([{"id": 100}])
        comments_df = pd.DataFrame(
            [
                {"id": 201, "parent": 999},  # Huérfano
                {"id": 202, "parent": 100},  # Válido
            ]
        )

        valid_comments, _ = hn_processor._validate_referential_integrity(
            stories_df, comments_df
        )

        check.equal(list(valid_comments.index), [0])

    def test_validate_should_return_empty_and_full_count_when_all_orphaned(
        self, hn_processor
    ):
        """Verifica que retorna DataFrame vacío cuando todos son huérfanos."""
        stories_df = pd.DataFrame([{"id": 100}])
        comments_df = pd.DataFrame(
            [
                {"id": 201, "parent": 888},
                {"id": 202, "parent": 999},
            ]
        )

        valid_comments, orphaned = hn_processor._validate_referential_integrity(
            stories_df, comments_df
        )

        check.equal(len(valid_comments), 0)
        check.equal(orphaned, 2)


class TestSaveProcessed:
    """Tests para el método _save_processed."""

    def test_save_should_call_writer_with_parquet_format_when_called(
        self, hn_processor, mock_writer
    ):
        """Verifica que llama al writer con formato parquet."""
        df = pd.DataFrame([{"id": 100, "score": 50}])

        hn_processor._save_processed(df, "stories", "2026-02-01")

        mock_writer.save.assert_called_once()
        call_args = mock_writer.save.call_args
        check.equal(call_args[1]["format"], "parquet")

    def test_save_should_call_writer_with_processed_layer_when_called(
        self, hn_processor, mock_writer
    ):
        """Verifica que guarda en la capa processed."""
        df = pd.DataFrame([{"id": 100}])

        hn_processor._save_processed(df, "stories", "2026-02-01")

        call_args = mock_writer.save.call_args
        check.equal(call_args[1]["layer"], "processed")

    def test_save_should_pass_correct_entity_and_date_when_called(
        self, hn_processor, mock_writer
    ):
        """Verifica que pasa entidad y fecha de partición correctas."""
        df = pd.DataFrame([{"id": 100}])

        hn_processor._save_processed(df, "comments", "2026-02-01")

        call_args = mock_writer.save.call_args
        check.equal(call_args[1]["entity"], "comments")
        check.equal(call_args[1]["partition_date"], "2026-02-01")

    def test_save_should_include_source_layer_metadata_when_called(
        self, hn_processor, mock_writer
    ):
        """Verifica que incluye metadata indicando la capa fuente."""
        df = pd.DataFrame([{"id": 100}])

        hn_processor._save_processed(df, "stories", "2026-02-01")

        call_args = mock_writer.save.call_args
        check.equal(call_args[1]["additional_metadata"]["source_layer"], "raw")

    def test_save_should_convert_dataframe_to_records_when_called(
        self, hn_processor, mock_writer
    ):
        """Verifica que convierte el DataFrame a lista de dicts para el writer."""
        df = pd.DataFrame([{"id": 100, "score": 50}, {"id": 200, "score": 75}])

        hn_processor._save_processed(df, "stories", "2026-02-01")

        call_args = mock_writer.save.call_args
        data = call_args[1]["data"]
        check.is_instance(data, list)
        check.equal(len(data), 2)
        check.is_instance(data[0], dict)


class TestProcessMethod:
    """Tests para el método principal process."""

    def test_process_should_return_stats_when_successful(
        self, hn_processor, mock_loader
    ):
        """Verifica que retorna estadísticas correctas al completar procesamiento."""
        stories_df = pd.DataFrame(
            [
                {
                    "id": 100,
                    "type": "story",
                    "time": 1706745600,
                    "score": 150,
                    "descendants": 45,
                }
            ]
        )
        comments_df = pd.DataFrame(
            [
                {
                    "id": 201,
                    "type": "comment",
                    "time": 1706749200,
                    "parent": 100,
                }
            ]
        )
        mock_loader.load_partition.side_effect = [stories_df, comments_df]

        result = hn_processor.process("2026-02-01")

        check.is_instance(result, dict)
        check.is_in("stories_raw", result)
        check.is_in("stories_processed", result)
        check.is_in("stories_duplicates_removed", result)
        check.is_in("comments_raw", result)
        check.is_in("comments_processed", result)
        check.is_in("comments_duplicates_removed", result)
        check.is_in("comments_orphaned", result)

    def test_process_should_call_loader_for_stories_and_comments_when_executed(
        self, hn_processor, mock_loader
    ):
        """Verifica que carga historias y comentarios desde raw."""
        mock_loader.load_partition.return_value = pd.DataFrame()

        hn_processor.process("2026-02-01")

        check.equal(mock_loader.load_partition.call_count, 2)

        calls = mock_loader.load_partition.call_args_list
        # Primera llamada: stories
        check.equal(calls[0][1]["layer"], "raw")
        check.equal(calls[0][1]["entity"], "stories")
        check.equal(calls[0][1]["partition_date"], "2026-02-01")
        check.equal(calls[0][1]["format"], "json")
        # Segunda llamada: comments
        check.equal(calls[1][1]["entity"], "comments")

    def test_process_should_save_stories_as_parquet_when_stories_exist(
        self, hn_processor, mock_loader, mock_writer
    ):
        """Verifica que guarda historias procesadas como Parquet."""
        stories_df = pd.DataFrame(
            [{"id": 100, "type": "story", "time": 1706745600, "score": 50}]
        )
        mock_loader.load_partition.side_effect = [stories_df, pd.DataFrame()]

        hn_processor.process("2026-02-01")

        # Debe llamar a writer.save al menos una vez para stories
        save_calls = [
            call
            for call in mock_writer.save.call_args_list
            if call[1]["entity"] == "stories"
        ]
        check.equal(len(save_calls), 1)
        check.equal(save_calls[0][1]["layer"], "processed")
        check.equal(save_calls[0][1]["format"], "parquet")

    def test_process_should_save_comments_as_parquet_when_comments_exist(
        self, hn_processor, mock_loader, mock_writer
    ):
        """Verifica que guarda comentarios procesados como Parquet."""
        stories_df = pd.DataFrame(
            [{"id": 100, "type": "story", "time": 1706745600, "score": 50}]
        )
        comments_df = pd.DataFrame(
            [{"id": 201, "type": "comment", "time": 1706749200, "parent": 100}]
        )
        mock_loader.load_partition.side_effect = [stories_df, comments_df]

        hn_processor.process("2026-02-01")

        save_calls = [
            call
            for call in mock_writer.save.call_args_list
            if call[1]["entity"] == "comments"
        ]
        check.equal(len(save_calls), 1)
        check.equal(save_calls[0][1]["format"], "parquet")

    def test_process_should_handle_empty_stories_when_no_data(
        self, hn_processor, mock_loader, mock_writer
    ):
        """Verifica que maneja correctamente cuando no hay historias."""
        mock_loader.load_partition.side_effect = [pd.DataFrame(), pd.DataFrame()]

        result = hn_processor.process("2026-02-01")

        check.equal(result["stories_raw"], 0)
        check.equal(result["stories_processed"], 0)
        check.equal(result["comments_raw"], 0)
        check.equal(result["comments_processed"], 0)
        mock_writer.save.assert_not_called()

    def test_process_should_handle_empty_comments_when_only_stories(
        self, hn_processor, mock_loader, mock_writer
    ):
        """Verifica que maneja correctamente cuando hay stories pero no comments."""
        stories_df = pd.DataFrame(
            [{"id": 100, "type": "story", "time": 1706745600, "score": 50}]
        )
        mock_loader.load_partition.side_effect = [stories_df, pd.DataFrame()]

        result = hn_processor.process("2026-02-01")

        check.equal(result["stories_processed"], 1)
        check.equal(result["comments_raw"], 0)
        check.equal(result["comments_processed"], 0)

        # Solo debe guardar stories, no comments
        save_calls = mock_writer.save.call_args_list
        entities_saved = [call[1]["entity"] for call in save_calls]
        check.is_in("stories", entities_saved)
        check.is_not_in("comments", entities_saved)

    def test_process_should_remove_duplicates_and_report_in_stats_when_present(
        self, hn_processor, mock_loader, mock_writer
    ):
        """Verifica que deduplica y reporta duplicados en estadísticas."""
        stories_df = pd.DataFrame(
            [
                {"id": 100, "type": "story", "time": 1706745600, "score": 50},
                {"id": 100, "type": "story", "time": 1706745600, "score": 75},
                {"id": 200, "type": "story", "time": 1706832000, "score": 30},
            ]
        )
        mock_loader.load_partition.side_effect = [stories_df, pd.DataFrame()]

        result = hn_processor.process("2026-02-01")

        check.equal(result["stories_raw"], 3)
        check.equal(result["stories_processed"], 2)
        check.equal(result["stories_duplicates_removed"], 1)

    def test_process_should_discard_orphaned_comments_and_report_in_stats(
        self, hn_processor, mock_loader, mock_writer
    ):
        """Verifica que descarta huérfanos y reporta en estadísticas."""
        stories_df = pd.DataFrame(
            [{"id": 100, "type": "story", "time": 1706745600, "score": 50}]
        )
        comments_df = pd.DataFrame(
            [
                {"id": 201, "type": "comment", "time": 1706749200, "parent": 100},
                {"id": 202, "type": "comment", "time": 1706752800, "parent": 999},
            ]
        )
        mock_loader.load_partition.side_effect = [stories_df, comments_df]

        result = hn_processor.process("2026-02-01")

        check.equal(result["comments_raw"], 2)
        check.equal(result["comments_processed"], 1)
        check.equal(result["comments_orphaned"], 1)

    def test_process_should_skip_integrity_check_when_no_stories(
        self, hn_processor, mock_loader, mock_writer
    ):
        """Verifica que no valida integridad cuando no hay stories disponibles."""
        comments_df = pd.DataFrame(
            [
                {"id": 201, "type": "comment", "time": 1706749200, "parent": 100},
                {"id": 202, "type": "comment", "time": 1706752800, "parent": 200},
            ]
        )
        mock_loader.load_partition.side_effect = [pd.DataFrame(), comments_df]

        result = hn_processor.process("2026-02-01")

        # Sin stories, los comentarios se guardan sin validar integridad
        check.equal(result["comments_processed"], 2)
        check.equal(result["comments_orphaned"], 0)

    def test_process_should_add_ingestion_date_to_saved_stories_when_processed(
        self, hn_processor, mock_loader, mock_writer
    ):
        """Verifica que los datos guardados incluyen columna ingestion_date."""
        stories_df = pd.DataFrame(
            [{"id": 100, "type": "story", "time": 1706745600, "score": 50}]
        )
        mock_loader.load_partition.side_effect = [stories_df, pd.DataFrame()]

        hn_processor.process("2026-02-01")

        save_calls = [
            call
            for call in mock_writer.save.call_args_list
            if call[1]["entity"] == "stories"
        ]
        saved_data = save_calls[0][1]["data"]
        check.is_in("ingestion_date", saved_data[0])
        check.equal(saved_data[0]["ingestion_date"], "2026-02-01")
