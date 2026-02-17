"""
Tests unitarios para transformation.hn_transformer.

Verifica enriquecimiento temporal, extracción de topics, clasificación
de sentiment, limpieza HTML y flujo de orquestación.
"""

from unittest.mock import Mock

import pandas as pd
import pytest
import pytest_check as check

from quality.runner import QualityCheckError
from transformation.hn_transformer import HNTransformer


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
    """Fixture que retorna un mock del runner de calidad sin fallos críticos."""
    mock = Mock()
    report_mock = Mock()
    report_mock.to_dict.return_value = {}
    report_mock.has_critical_failures = False
    mock.run_transformation_story_checks.return_value = report_mock
    mock.run_transformation_comment_checks.return_value = report_mock
    return mock


@pytest.fixture
def transformer(mock_loader, mock_writer, mock_quality_runner):
    """Fixture que retorna HNTransformer con dependencias mockeadas."""
    return HNTransformer(
        loader=mock_loader,
        writer=mock_writer,
        quality_runner=mock_quality_runner,
        window_days=3,
        top_n_topics=2,
    )


@pytest.fixture
def sample_target_stories():
    """Stories procesadas del día target con campos mínimos requeridos."""
    return pd.DataFrame(
        [
            {
                "id": 100,
                "title": "Show HN: A new Python framework",
                "score": 150,
                "descendants": 45,
                "time": pd.Timestamp("2026-02-05 10:00:00", tz="UTC"),
                "ingestion_date": "2026-02-07",
            },
            {
                "id": 200,
                "title": "Rust is eating the world",
                "score": 80,
                "descendants": 20,
                "time": pd.Timestamp("2026-02-06 08:00:00", tz="UTC"),
                "ingestion_date": "2026-02-07",
            },
        ]
    )


@pytest.fixture
def sample_historical_stories():
    """Observaciones históricas de las mismas stories en días previos."""
    return pd.DataFrame(
        [
            {
                "id": 100,
                "title": "Show HN: A new Python framework",
                "score": 50,
                "descendants": 10,
                "time": pd.Timestamp("2026-02-05 10:00:00", tz="UTC"),
                "ingestion_date": "2026-02-05",
            },
            {
                "id": 100,
                "title": "Show HN: A new Python framework",
                "score": 100,
                "descendants": 30,
                "time": pd.Timestamp("2026-02-05 10:00:00", tz="UTC"),
                "ingestion_date": "2026-02-06",
            },
        ]
    )


@pytest.fixture
def sample_comments():
    """Comments procesados con texto variado para sentiment."""
    return pd.DataFrame(
        [
            {
                "id": 301,
                "text": "This is absolutely amazing and wonderful!",
                "parent": 100,
                "ingestion_date": "2026-02-07",
            },
            {
                "id": 302,
                "text": "Terrible, worst thing I have ever seen, awful.",
                "parent": 100,
                "ingestion_date": "2026-02-07",
            },
            {
                "id": 303,
                "text": "The code is at github.com/example",
                "parent": 200,
                "ingestion_date": "2026-02-07",
            },
        ]
    )


class TestCleanHtml:
    """Tests para el método estático _clean_html."""

    def test_clean_html_should_remove_tags_when_html_present(self):
        # Arrange
        text = "<p>Hello <b>world</b></p>"

        # Act
        result = HNTransformer._clean_html(text)

        # Assert
        assert "Hello" in result
        assert "world" in result
        assert "<" not in result

    def test_clean_html_should_decode_entities_when_encoded(self):
        # Arrange
        text = "Tom &amp; Jerry &lt;3"

        # Act
        result = HNTransformer._clean_html(text)

        # Assert
        check.equal(result, "Tom & Jerry <3")

    def test_clean_html_should_normalize_whitespace_when_multiple_spaces(self):
        # Arrange
        text = "hello   <br>   world   "

        # Act
        result = HNTransformer._clean_html(text)

        # Assert
        check.equal(result, "hello world")

    def test_clean_html_should_return_empty_string_when_input_empty(self):
        # Act / Assert
        check.equal(HNTransformer._clean_html(""), "")

    def test_clean_html_should_return_empty_string_when_input_none(self):
        # Act / Assert
        check.equal(HNTransformer._clean_html(None), "")

    def test_clean_html_should_return_plain_text_when_no_html(self):
        # Arrange
        text = "Just plain text here"

        # Act
        result = HNTransformer._clean_html(text)

        # Assert
        check.equal(result, "Just plain text here")

    def test_clean_html_should_handle_nested_tags_when_deeply_nested(self):
        # Arrange
        text = "<div><p><a href='url'>link <em>text</em></a></p></div>"

        # Act
        result = HNTransformer._clean_html(text)

        # Assert
        assert "<" not in result
        assert "link" in result
        assert "text" in result


class TestEnrichStoriesTemporal:
    """Tests para _enrich_stories_temporal."""

    def test_should_calculate_score_velocity_when_historical_exists(
        self, transformer, sample_target_stories, sample_historical_stories
    ):
        """Velocity = score actual - score observación previa."""
        # Act
        result = transformer._enrich_stories_temporal(
            sample_target_stories, sample_historical_stories, "2026-02-07"
        )

        # Assert: story 100 tenía score 100 el día 06, ahora 150 -> velocity = 50
        story_100 = result[result["id"] == 100].iloc[0]
        check.equal(story_100["score_velocity"], 50)

    def test_should_calculate_comment_velocity_when_historical_exists(
        self, transformer, sample_target_stories, sample_historical_stories
    ):
        """Comment velocity = descendants actual - descendants previo."""
        # Act
        result = transformer._enrich_stories_temporal(
            sample_target_stories, sample_historical_stories, "2026-02-07"
        )

        # Assert: story 100 tenía 30 descendants el día 06, ahora 45 -> velocity = 15
        story_100 = result[result["id"] == 100].iloc[0]
        check.equal(story_100["comment_velocity"], 15)

    def test_should_set_velocity_to_zero_when_no_previous_observation(
        self, transformer, sample_target_stories
    ):
        """Sin historial, velocity debe ser 0."""
        # Arrange
        empty_historical = pd.DataFrame()

        # Act
        result = transformer._enrich_stories_temporal(
            sample_target_stories, empty_historical, "2026-02-07"
        )

        # Assert
        story_200 = result[result["id"] == 200].iloc[0]
        check.equal(story_200["score_velocity"], 0)
        check.equal(story_200["comment_velocity"], 0)

    def test_should_count_observations_in_window_when_historical_exists(
        self, transformer, sample_target_stories, sample_historical_stories
    ):
        """Story 100 aparece en 3 días (05, 06, 07) -> 3 observaciones."""
        # Act
        result = transformer._enrich_stories_temporal(
            sample_target_stories, sample_historical_stories, "2026-02-07"
        )

        # Assert
        story_100 = result[result["id"] == 100].iloc[0]
        check.equal(story_100["observations_in_window"], 3)

    def test_should_set_observations_to_one_when_no_historical(
        self, transformer, sample_target_stories
    ):
        """Sin historial, solo 1 observación (la del target)."""
        # Act
        result = transformer._enrich_stories_temporal(
            sample_target_stories, pd.DataFrame(), "2026-02-07"
        )

        # Assert
        story_200 = result[result["id"] == 200].iloc[0]
        check.equal(story_200["observations_in_window"], 1)

    def test_should_return_only_target_date_records(
        self, transformer, sample_target_stories, sample_historical_stories
    ):
        """El resultado solo contiene registros del ingestion_date target."""
        # Act
        result = transformer._enrich_stories_temporal(
            sample_target_stories, sample_historical_stories, "2026-02-07"
        )

        # Assert
        unique_dates = result["ingestion_date"].unique().tolist()
        check.equal(unique_dates, ["2026-02-07"])
        check.equal(len(result), 2)

    def test_should_drop_auxiliary_columns_when_complete(
        self, transformer, sample_target_stories, sample_historical_stories
    ):
        """No debe quedar _prev_score, _prev_descendants ni _peak_date."""
        # Act
        result = transformer._enrich_stories_temporal(
            sample_target_stories, sample_historical_stories, "2026-02-07"
        )

        # Assert
        check.is_not_in("_prev_score", result.columns)
        check.is_not_in("_prev_descendants", result.columns)
        check.is_not_in("_peak_date", result.columns)

    def test_should_add_enrichment_columns_when_complete(
        self, transformer, sample_target_stories, sample_historical_stories
    ):
        """Verifica que todas las columnas de enriquecimiento están presentes."""
        # Act
        result = transformer._enrich_stories_temporal(
            sample_target_stories, sample_historical_stories, "2026-02-07"
        )

        # Assert
        expected_cols = [
            "score_velocity",
            "comment_velocity",
            "hours_to_peak",
            "is_long_tail",
            "observations_in_window",
        ]
        for col in expected_cols:
            check.is_in(col, result.columns)

    def test_should_calculate_hours_to_peak_when_peak_on_target_date(
        self, transformer, sample_target_stories, sample_historical_stories
    ):
        """hours_to_peak debe ser >= 0 y calculado desde time de creación."""
        # Act
        result = transformer._enrich_stories_temporal(
            sample_target_stories, sample_historical_stories, "2026-02-07"
        )

        # Assert: story 100 peak score es 150 en 2026-02-07.
        # time de creación: 2026-02-05 10:00 UTC, peak en 2026-02-07 -> ~48h
        story_100 = result[result["id"] == 100].iloc[0]
        check.greater(story_100["hours_to_peak"], 0)

    def test_should_set_is_long_tail_true_when_comments_growing_after_48h(
        self, transformer
    ):
        """is_long_tail = True si comment_velocity > 0 y han pasado 48h+."""
        # Arrange: story creada hace 72h con descendants creciendo
        target = pd.DataFrame(
            [
                {
                    "id": 500,
                    "title": "Old story still active",
                    "score": 200,
                    "descendants": 60,
                    "time": pd.Timestamp("2026-02-04 08:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        historical = pd.DataFrame(
            [
                {
                    "id": 500,
                    "title": "Old story still active",
                    "score": 180,
                    "descendants": 50,
                    "time": pd.Timestamp("2026-02-04 08:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-06",
                }
            ]
        )

        # Act
        result = transformer._enrich_stories_temporal(target, historical, "2026-02-07")

        # Assert
        check.is_true(result.iloc[0]["is_long_tail"])

    def test_should_set_is_long_tail_false_when_comments_not_growing(self, transformer):
        """is_long_tail = False si comment_velocity <= 0 aunque > 48h."""
        # Arrange: story creada hace 72h pero descendants sin cambio
        target = pd.DataFrame(
            [
                {
                    "id": 600,
                    "title": "Stale story",
                    "score": 200,
                    "descendants": 50,
                    "time": pd.Timestamp("2026-02-04 08:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        historical = pd.DataFrame(
            [
                {
                    "id": 600,
                    "title": "Stale story",
                    "score": 190,
                    "descendants": 50,
                    "time": pd.Timestamp("2026-02-04 08:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-06",
                }
            ]
        )

        # Act
        result = transformer._enrich_stories_temporal(target, historical, "2026-02-07")

        # Assert
        check.is_false(result.iloc[0]["is_long_tail"])

    def test_should_set_is_long_tail_false_when_story_younger_than_48h(
        self, transformer
    ):
        """is_long_tail = False si la story tiene menos de 48h."""
        # Arrange: story creada hace 12h con descendants creciendo
        target = pd.DataFrame(
            [
                {
                    "id": 700,
                    "title": "Fresh story",
                    "score": 100,
                    "descendants": 30,
                    "time": pd.Timestamp("2026-02-06 20:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        historical = pd.DataFrame(
            [
                {
                    "id": 700,
                    "title": "Fresh story",
                    "score": 50,
                    "descendants": 10,
                    "time": pd.Timestamp("2026-02-06 20:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-06",
                }
            ]
        )

        # Act
        result = transformer._enrich_stories_temporal(target, historical, "2026-02-07")

        # Assert
        check.is_false(result.iloc[0]["is_long_tail"])

    def test_should_deduplicate_by_id_and_ingestion_date_when_overlap(
        self, transformer
    ):
        """Si historial y target solapan, dedup conserva 'last'."""
        # Arrange: misma id + ingestion_date en ambos
        target = pd.DataFrame(
            [
                {
                    "id": 800,
                    "title": "Overlap story",
                    "score": 100,
                    "descendants": 20,
                    "time": pd.Timestamp("2026-02-06 10:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        historical = pd.DataFrame(
            [
                {
                    "id": 800,
                    "title": "Overlap story",
                    "score": 90,
                    "descendants": 15,
                    "time": pd.Timestamp("2026-02-06 10:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )

        # Act
        result = transformer._enrich_stories_temporal(target, historical, "2026-02-07")

        # Assert: solo un registro para id 800 en la fecha target
        check.equal(len(result[result["id"] == 800]), 1)

    def test_should_handle_non_numeric_score_gracefully_when_coerced(self, transformer):
        """Scores no numéricos deben ser tratados como 0 sin error."""
        # Arrange
        target = pd.DataFrame(
            [
                {
                    "id": 900,
                    "title": "Bad data",
                    "score": "invalid",
                    "descendants": None,
                    "time": pd.Timestamp("2026-02-06 10:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )

        # Act
        result = transformer._enrich_stories_temporal(
            target, pd.DataFrame(), "2026-02-07"
        )

        # Assert: no crash, score coerced to 0
        check.equal(len(result), 1)
        check.equal(result.iloc[0]["score"], 0)


class TestEnrichStoriesTopics:
    """Tests para _enrich_stories_topics."""

    def test_should_add_dominant_topics_column_when_titles_present(
        self, transformer, sample_target_stories
    ):
        """Verifica que se agrega la columna dominant_topics."""
        # Act
        result = transformer._enrich_stories_topics(sample_target_stories.copy())

        # Assert
        check.is_in("dominant_topics", result.columns)

    def test_should_extract_topics_as_comma_separated_when_valid_titles(
        self, transformer
    ):
        """Topics deben ser string con términos separados por coma."""
        # Arrange
        stories = pd.DataFrame(
            [
                {"title": "Machine learning advances in Python"},
                {"title": "Deep learning with neural networks"},
                {"title": "Python data science and machine learning"},
            ]
        )

        # Act
        result = transformer._enrich_stories_topics(stories)

        # Assert: cada story debe tener topics como string
        for _, row in result.iterrows():
            if row["dominant_topics"] is not None:
                check.is_instance(row["dominant_topics"], str)

    def test_should_respect_top_n_topics_limit_when_configured(self, transformer):
        """No debe devolver más de top_n_topics (configurado en 2 para fixture)."""
        # Arrange
        stories = pd.DataFrame(
            [
                {"title": "Python machine learning data science engineering"},
                {"title": "Kubernetes docker containers cloud infrastructure"},
            ]
        )

        # Act
        result = transformer._enrich_stories_topics(stories)

        # Assert
        for _, row in result.iterrows():
            if row["dominant_topics"] is not None:
                topic_count = len(row["dominant_topics"].split(","))
                check.less_equal(topic_count, 2)

    def test_should_set_none_when_all_titles_empty(self, transformer):
        """Si todos los títulos son vacíos, dominant_topics debe ser None."""
        # Arrange
        stories = pd.DataFrame([{"title": ""}, {"title": None}, {"title": "   "}])

        # Act
        result = transformer._enrich_stories_topics(stories)

        # Assert
        for val in result["dominant_topics"]:
            check.is_none(val)

    def test_should_not_crash_when_single_story(self, transformer):
        """TF-IDF con un solo documento no debe fallar."""
        # Arrange
        stories = pd.DataFrame([{"title": "Standalone story about Golang"}])

        # Act
        result = transformer._enrich_stories_topics(stories)

        # Assert
        check.equal(len(result), 1)
        check.is_in("dominant_topics", result.columns)

    def test_should_preserve_existing_columns_when_enriching(self, transformer):
        """Las columnas originales del DataFrame deben mantenerse."""
        # Arrange
        stories = pd.DataFrame(
            [{"id": 1, "title": "Test", "score": 100, "extra_col": "keep_me"}]
        )

        # Act
        result = transformer._enrich_stories_topics(stories)

        # Assert
        check.is_in("id", result.columns)
        check.is_in("score", result.columns)
        check.is_in("extra_col", result.columns)


class TestEnrichCommentsSentiment:
    """Tests para _enrich_comments_sentiment."""

    def test_should_add_sentiment_columns_when_called(
        self, transformer, sample_comments
    ):
        """Verifica que agrega sentiment_score y sentiment_label."""
        # Act
        result = transformer._enrich_comments_sentiment(sample_comments.copy())

        # Assert
        check.is_in("sentiment_score", result.columns)
        check.is_in("sentiment_label", result.columns)

    def test_should_classify_positive_when_clearly_positive_text(self, transformer):
        """Texto claramente positivo debe tener label 'positive'."""
        # Arrange
        comments = pd.DataFrame(
            [{"text": "This is absolutely amazing and wonderful! I love it so much!"}]
        )

        # Act
        result = transformer._enrich_comments_sentiment(comments)

        # Assert
        check.equal(result.iloc[0]["sentiment_label"], "positive")
        check.greater_equal(
            result.iloc[0]["sentiment_score"],
            HNTransformer.SENTIMENT_POSITIVE_THRESHOLD,
        )

    def test_should_classify_negative_when_clearly_negative_text(self, transformer):
        """Texto claramente negativo debe tener label 'negative'."""
        # Arrange
        comments = pd.DataFrame(
            [{"text": "This is terrible, awful, disgusting and horrible."}]
        )

        # Act
        result = transformer._enrich_comments_sentiment(comments)

        # Assert
        check.equal(result.iloc[0]["sentiment_label"], "negative")
        check.less_equal(
            result.iloc[0]["sentiment_score"],
            HNTransformer.SENTIMENT_NEGATIVE_THRESHOLD,
        )

    def test_should_classify_neutral_when_factual_text(self, transformer):
        """Texto factual/neutral debe tener label 'neutral'."""
        # Arrange
        comments = pd.DataFrame([{"text": "The repository link is on GitHub."}])

        # Act
        result = transformer._enrich_comments_sentiment(comments)

        # Assert
        check.equal(result.iloc[0]["sentiment_label"], "neutral")

    def test_should_set_neutral_and_zero_when_text_is_null(self, transformer):
        """Texto None debe producir score 0 y label 'neutral'."""
        # Arrange
        comments = pd.DataFrame([{"text": None}])

        # Act
        result = transformer._enrich_comments_sentiment(comments)

        # Assert
        check.equal(result.iloc[0]["sentiment_score"], 0.0)
        check.equal(result.iloc[0]["sentiment_label"], "neutral")

    def test_should_set_neutral_and_zero_when_text_is_empty(self, transformer):
        """Texto vacío debe producir score 0 y label 'neutral'."""
        # Arrange
        comments = pd.DataFrame([{"text": ""}])

        # Act
        result = transformer._enrich_comments_sentiment(comments)

        # Assert
        check.equal(result.iloc[0]["sentiment_score"], 0.0)
        check.equal(result.iloc[0]["sentiment_label"], "neutral")

    def test_should_clean_html_before_analysis_when_html_present(self, transformer):
        """HTML debe ser limpiado antes del análisis de sentiment."""
        # Arrange: texto positivo envuelto en HTML
        comments = pd.DataFrame(
            [{"text": "<p>This is <b>absolutely wonderful</b> and amazing!</p>"}]
        )

        # Act
        result = transformer._enrich_comments_sentiment(comments)

        # Assert: el sentiment debe reflejar el texto limpio, no los tags
        check.equal(result.iloc[0]["sentiment_label"], "positive")

    def test_should_round_score_to_four_decimals_when_calculated(self, transformer):
        """El score debe estar redondeado a 4 decimales."""
        # Arrange
        comments = pd.DataFrame([{"text": "Good work on this project!"}])

        # Act
        result = transformer._enrich_comments_sentiment(comments)

        # Assert
        score_str = str(result.iloc[0]["sentiment_score"])
        if "." in score_str:
            decimals = len(score_str.split(".")[1])
            check.less_equal(decimals, 4)

    def test_should_preserve_all_rows_when_multiple_comments(
        self, transformer, sample_comments
    ):
        """Todos los comments deben procesarse sin perder filas."""
        # Act
        result = transformer._enrich_comments_sentiment(sample_comments.copy())

        # Assert
        check.equal(len(result), len(sample_comments))


class TestLoadHistoricalStories:
    """Tests para _load_historical_stories."""

    def test_should_load_partitions_for_each_day_in_window(
        self, transformer, mock_loader
    ):
        """Debe llamar al loader una vez por día en la ventana (3 días)."""
        # Arrange
        mock_loader.load_partition.return_value = pd.DataFrame()

        # Act
        transformer._load_historical_stories("2026-02-07")

        # Assert: window_days=3 -> carga 2026-02-06, 2026-02-05, 2026-02-04
        check.equal(mock_loader.load_partition.call_count, 3)

        dates_called = [
            c[1]["partition_date"] for c in mock_loader.load_partition.call_args_list
        ]
        check.is_in("2026-02-06", dates_called)
        check.is_in("2026-02-05", dates_called)
        check.is_in("2026-02-04", dates_called)

    def test_should_return_empty_dataframe_when_no_historical_data(
        self, transformer, mock_loader
    ):
        """Si no hay datos en ninguna partición, retorna DataFrame vacío."""
        # Arrange
        mock_loader.load_partition.return_value = pd.DataFrame()

        # Act
        result = transformer._load_historical_stories("2026-02-07")

        # Assert
        check.is_true(result.empty)

    def test_should_concatenate_multiple_days_when_data_exists(
        self, transformer, mock_loader
    ):
        """Debe concatenar DataFrames de múltiples días."""
        # Arrange
        day1 = pd.DataFrame([{"id": 1, "score": 10}])
        day2 = pd.DataFrame([{"id": 2, "score": 20}])
        mock_loader.load_partition.side_effect = [day1, day2, pd.DataFrame()]

        # Act
        result = transformer._load_historical_stories("2026-02-07")

        # Assert
        check.equal(len(result), 2)

    def test_should_skip_empty_partitions_when_mixed_availability(
        self, transformer, mock_loader
    ):
        """Particiones vacías no deben incluirse en el resultado."""
        # Arrange
        day_with_data = pd.DataFrame([{"id": 1, "score": 50}])
        mock_loader.load_partition.side_effect = [
            pd.DataFrame(),
            day_with_data,
            pd.DataFrame(),
        ]

        # Act
        result = transformer._load_historical_stories("2026-02-07")

        # Assert
        check.equal(len(result), 1)
        check.equal(result.iloc[0]["id"], 1)

    def test_should_request_parquet_format_when_loading(self, transformer, mock_loader):
        """Debe solicitar formato parquet al loader."""
        # Arrange
        mock_loader.load_partition.return_value = pd.DataFrame()

        # Act
        transformer._load_historical_stories("2026-02-07")

        # Assert
        for c in mock_loader.load_partition.call_args_list:
            check.equal(c[1]["format"], "parquet")
            check.equal(c[1]["layer"], "processed")
            check.equal(c[1]["entity"], "stories")


class TestTransform:
    """Tests para el método principal transform."""

    def test_should_return_stats_dict_when_successful(self, transformer, mock_loader):
        """Verifica que retorna diccionario con todas las claves de stats."""
        # Arrange
        stories = pd.DataFrame(
            [
                {
                    "id": 100,
                    "title": "Test Story",
                    "score": 50,
                    "descendants": 10,
                    "time": pd.Timestamp("2026-02-06 10:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        comments = pd.DataFrame(
            [{"id": 301, "text": "Nice", "parent": 100, "ingestion_date": "2026-02-07"}]
        )
        # Calls: target_stories, target_comments, then 3 historical partitions
        mock_loader.load_partition.side_effect = [
            stories,
            comments,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        ]

        # Act
        result = transformer.transform("2026-02-07")

        # Assert
        expected_keys = [
            "stories_input",
            "stories_enriched",
            "comments_input",
            "comments_enriched",
            "historical_observations_loaded",
            "quality_stories",
            "quality_comments",
        ]
        for key in expected_keys:
            check.is_in(key, result)

    def test_should_count_stories_input_correctly_when_data_present(
        self, transformer, mock_loader
    ):
        """stories_input debe reflejar la cantidad de stories cargadas."""
        # Arrange
        stories = pd.DataFrame(
            [
                {
                    "id": i,
                    "title": f"Story {i}",
                    "score": 10,
                    "descendants": 5,
                    "time": pd.Timestamp("2026-02-06 10:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
                for i in range(1, 4)
            ]
        )
        mock_loader.load_partition.side_effect = [
            stories,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        ]

        # Act
        result = transformer.transform("2026-02-07")

        # Assert
        check.equal(result["stories_input"], 3)
        check.equal(result["stories_enriched"], 3)

    def test_should_handle_empty_stories_when_no_data(
        self, transformer, mock_loader, mock_writer
    ):
        """Con stories vacías, no debe intentar enriquecer ni guardar stories."""
        # Arrange
        comments = pd.DataFrame(
            [
                {
                    "id": 301,
                    "text": "Comment",
                    "parent": 100,
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        mock_loader.load_partition.return_value = pd.DataFrame()
        mock_loader.load_partition.side_effect = [pd.DataFrame(), comments]

        # Act
        result = transformer.transform("2026-02-07")

        # Assert
        check.equal(result["stories_input"], 0)
        check.equal(result["stories_enriched"], 0)

    def test_should_handle_empty_comments_when_no_data(self, transformer, mock_loader):
        """Con comments vacíos, no debe intentar enriquecer comments."""
        # Arrange
        stories = pd.DataFrame(
            [
                {
                    "id": 100,
                    "title": "Test",
                    "score": 10,
                    "descendants": 5,
                    "time": pd.Timestamp("2026-02-06 10:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        mock_loader.load_partition.side_effect = [
            stories,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        ]

        # Act
        result = transformer.transform("2026-02-07")

        # Assert
        check.equal(result["comments_input"], 0)
        check.equal(result["comments_enriched"], 0)

    def test_should_raise_quality_error_when_stories_critical_failure(
        self, transformer, mock_loader, mock_quality_runner
    ):
        """Debe lanzar QualityCheckError si checks de stories son críticos."""
        # Arrange
        stories = pd.DataFrame(
            [
                {
                    "id": 100,
                    "title": "Test",
                    "score": 10,
                    "descendants": 5,
                    "time": pd.Timestamp("2026-02-06 10:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        mock_loader.load_partition.side_effect = [
            stories,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        ]

        critical_report = Mock()
        critical_report.to_dict.return_value = {}
        critical_report.has_critical_failures = True
        mock_quality_runner.run_transformation_story_checks.return_value = (
            critical_report
        )

        # Act / Assert
        with pytest.raises(QualityCheckError):
            transformer.transform("2026-02-07")

    def test_should_raise_quality_error_when_comments_critical_failure(
        self, transformer, mock_loader, mock_quality_runner
    ):
        """Debe lanzar QualityCheckError si checks de comments son críticos."""
        # Arrange
        comments = pd.DataFrame(
            [{"id": 301, "text": "Test", "parent": 100, "ingestion_date": "2026-02-07"}]
        )
        mock_loader.load_partition.side_effect = [
            pd.DataFrame(),
            comments,
        ]

        critical_report = Mock()
        critical_report.to_dict.return_value = {}
        critical_report.has_critical_failures = True
        mock_quality_runner.run_transformation_comment_checks.return_value = (
            critical_report
        )

        # Act / Assert
        with pytest.raises(QualityCheckError):
            transformer.transform("2026-02-07")

    def test_should_save_stories_to_output_when_enriched(
        self, transformer, mock_loader, mock_writer
    ):
        """Debe llamar a writer.save para stories enriquecidas."""
        # Arrange
        stories = pd.DataFrame(
            [
                {
                    "id": 100,
                    "title": "Test",
                    "score": 10,
                    "descendants": 5,
                    "time": pd.Timestamp("2026-02-06 10:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        mock_loader.load_partition.side_effect = [
            stories,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        ]

        # Act
        transformer.transform("2026-02-07")

        # Assert: al menos una llamada a save con layer=output y entity=stories
        save_calls = mock_writer.save.call_args_list
        story_saves = [
            c
            for c in save_calls
            if c[1].get("entity") == "stories" and c[1].get("layer") == "output"
        ]
        check.greater(len(story_saves), 0)

    def test_should_save_comments_to_output_when_enriched(
        self, transformer, mock_loader, mock_writer
    ):
        """Debe llamar a writer.save para comments enriquecidos."""
        # Arrange
        comments = pd.DataFrame(
            [
                {
                    "id": 301,
                    "text": "Great!",
                    "parent": 100,
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        mock_loader.load_partition.side_effect = [
            pd.DataFrame(),
            comments,
        ]

        # Act
        transformer.transform("2026-02-07")

        # Assert
        save_calls = mock_writer.save.call_args_list
        comment_saves = [
            c
            for c in save_calls
            if c[1].get("entity") == "comments" and c[1].get("layer") == "output"
        ]
        check.greater(len(comment_saves), 0)

    def test_should_save_quality_report_when_stories_enriched(
        self, transformer, mock_loader, mock_writer
    ):
        """Debe persistir reporte de calidad para stories."""
        # Arrange
        stories = pd.DataFrame(
            [
                {
                    "id": 100,
                    "title": "Test",
                    "score": 10,
                    "descendants": 5,
                    "time": pd.Timestamp("2026-02-06 10:00:00", tz="UTC"),
                    "ingestion_date": "2026-02-07",
                }
            ]
        )
        mock_loader.load_partition.side_effect = [
            stories,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        ]

        # Act
        transformer.transform("2026-02-07")

        # Assert
        save_calls = mock_writer.save.call_args_list
        report_saves = [
            c for c in save_calls if "quality_reports" in str(c[1].get("entity", ""))
        ]
        check.greater(len(report_saves), 0)

    def test_should_handle_both_empty_when_no_data_at_all(
        self, transformer, mock_loader, mock_writer
    ):
        """Sin stories ni comments, retorna stats en cero y no guarda nada."""
        # Arrange
        mock_loader.load_partition.return_value = pd.DataFrame()

        # Act
        result = transformer.transform("2026-02-07")

        # Assert
        check.equal(result["stories_input"], 0)
        check.equal(result["comments_input"], 0)
        check.equal(result["stories_enriched"], 0)
        check.equal(result["comments_enriched"], 0)
        check.equal(mock_writer.save.call_count, 0)
