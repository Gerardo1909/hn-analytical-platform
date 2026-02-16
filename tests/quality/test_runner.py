"""
Tests unitarios para quality.runner.
Verifica que el runner orquesta correctamente los checks de quality.checks
y produce reportes consolidados con política de severidad correcta.
"""

import pandas as pd
import pytest
import pytest_check as check

from quality.runner import QualityReport, QualityRunner


@pytest.fixture
def runner():
    """Fixture que retorna una instancia de QualityRunner."""
    return QualityRunner()


@pytest.fixture
def sample_stories():
    """Fixture que retorna stories procesadas válidas."""
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
                "ingestion_date": "2026-02-01",
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
                "ingestion_date": "2026-02-01",
            },
        ]
    )


@pytest.fixture
def sample_comments():
    """Fixture que retorna comments procesados válidos."""
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
                "ingestion_date": "2026-02-01",
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
                "ingestion_date": "2026-02-01",
            },
        ]
    )


@pytest.fixture
def valid_parent_df(sample_stories, sample_comments):
    """Fixture que retorna DataFrame de parents válidos (stories + comments)."""
    return pd.concat(
        [sample_stories[["id"]], sample_comments[["id"]]], ignore_index=True
    )


class TestQualityReport:
    """Tests para el dataclass QualityReport."""

    def test_to_dict_should_return_all_fields_when_called(self):
        """Verifica que to_dict serializa todos los campos del reporte."""
        report = QualityReport(
            ingestion_date="2026-02-01",
            generated_at="2026-02-01T12:00:00",
            total_checks=5,
            passed_checks=4,
            failed_checks=1,
            has_critical_failures=False,
            entities={"stories": {"passed": 4, "failed": 1}},
            checks=[{"name": "check_not_null", "passed": True}],
        )

        d = report.to_dict()

        check.equal(d["ingestion_date"], "2026-02-01")
        check.equal(d["generated_at"], "2026-02-01T12:00:00")
        check.equal(d["total_checks"], 5)
        check.equal(d["passed_checks"], 4)
        check.equal(d["failed_checks"], 1)
        check.equal(d["has_critical_failures"], False)
        check.is_in("stories", d["entities"])
        check.equal(len(d["checks"]), 1)


class TestRunStoryChecks:
    """Tests para QualityRunner.run_story_checks."""

    def test_should_return_quality_report_when_called(self, runner, sample_stories):
        """Verifica que retorna un QualityReport."""
        report = runner.run_story_checks(sample_stories, "2026-02-01")

        check.is_instance(report, QualityReport)

    def test_should_pass_all_checks_when_stories_valid(self, runner, sample_stories):
        """Verifica que todos los checks pasan con stories válidas."""
        report = runner.run_story_checks(sample_stories, "2026-02-01")

        check.equal(report.failed_checks, 0)
        check.is_false(report.has_critical_failures)
        check.greater(report.total_checks, 0)
        check.equal(report.passed_checks, report.total_checks)

    def test_should_include_ingestion_date_in_report(self, runner, sample_stories):
        """Verifica que el reporte incluye la fecha de ingesta."""
        report = runner.run_story_checks(sample_stories, "2026-02-01")

        check.equal(report.ingestion_date, "2026-02-01")

    def test_should_include_generated_at_timestamp(self, runner, sample_stories):
        """Verifica que el reporte incluye timestamp de generación."""
        report = runner.run_story_checks(sample_stories, "2026-02-01")

        check.is_not_none(report.generated_at)
        check.greater(len(report.generated_at), 0)

    def test_should_run_not_null_check_on_required_columns(self, runner):
        """Verifica que detecta nulos en columnas requeridas de stories."""
        stories_with_null_id = pd.DataFrame(
            [
                {
                    "id": None,
                    "type": "story",
                    "time": 1706745600,
                    "score": 50,
                    "descendants": 10,
                    "ingestion_date": "2026-02-01",
                }
            ]
        )

        report = runner.run_story_checks(stories_with_null_id, "2026-02-01")

        check.is_true(report.has_critical_failures)
        failed_names = [c["name"] for c in report.checks if not c["passed"]]
        check.is_in("check_not_null", failed_names)

    def test_should_run_unique_check_on_id_and_ingestion_date(self, runner):
        """Verifica que detecta duplicados por (id, ingestion_date)."""
        stories_with_dupes = pd.DataFrame(
            [
                {
                    "id": 100,
                    "type": "story",
                    "time": 1706745600,
                    "score": 50,
                    "descendants": 10,
                    "ingestion_date": "2026-02-01",
                },
                {
                    "id": 100,
                    "type": "story",
                    "time": 1706745600,
                    "score": 75,
                    "descendants": 20,
                    "ingestion_date": "2026-02-01",
                },
            ]
        )

        report = runner.run_story_checks(stories_with_dupes, "2026-02-01")

        check.is_true(report.has_critical_failures)
        failed_names = [c["name"] for c in report.checks if not c["passed"]]
        check.is_in("check_unique", failed_names)

    def test_should_run_range_check_on_score(self, runner):
        """Verifica que detecta score negativo como warning."""
        stories_negative_score = pd.DataFrame(
            [
                {
                    "id": 100,
                    "type": "story",
                    "time": 1706745600,
                    "score": -5,
                    "descendants": 10,
                    "ingestion_date": "2026-02-01",
                }
            ]
        )

        report = runner.run_story_checks(stories_negative_score, "2026-02-01")

        # score range es warning, no critical
        check.is_false(report.has_critical_failures)
        failed_checks = [c for c in report.checks if not c["passed"]]
        range_fails = [c for c in failed_checks if c["name"] == "check_range"]
        check.greater(len(range_fails), 0)

    def test_should_run_range_check_on_descendants(self, runner):
        """Verifica que detecta descendants negativo como warning."""
        stories_negative_desc = pd.DataFrame(
            [
                {
                    "id": 100,
                    "type": "story",
                    "time": 1706745600,
                    "score": 50,
                    "descendants": -1,
                    "ingestion_date": "2026-02-01",
                }
            ]
        )

        report = runner.run_story_checks(stories_negative_desc, "2026-02-01")

        check.is_false(report.has_critical_failures)
        failed_checks = [c for c in report.checks if not c["passed"]]
        check.greater(len(failed_checks), 0)

    def test_should_run_volume_check(self, runner):
        """Verifica que el check de volumen se ejecuta."""
        report = runner.run_story_checks(
            pd.DataFrame(
                columns=["id", "type", "time", "ingestion_date", "score", "descendants"]
            ),
            "2026-02-01",
        )

        volume_checks = [c for c in report.checks if c["name"] == "check_volume"]
        check.equal(len(volume_checks), 1)

    def test_should_have_five_checks_for_valid_stories(self, runner, sample_stories):
        """Verifica que se ejecutan exactamente 5 checks para stories."""
        report = runner.run_story_checks(sample_stories, "2026-02-01")

        # not_null, unique, range(score), range(descendants), volume
        check.equal(report.total_checks, 5)

    def test_should_serialize_report_to_dict(self, runner, sample_stories):
        """Verifica que el reporte se serializa correctamente."""
        report = runner.run_story_checks(sample_stories, "2026-02-01")
        d = report.to_dict()

        check.is_instance(d, dict)
        check.is_in("ingestion_date", d)
        check.is_in("total_checks", d)
        check.is_in("checks", d)
        check.is_instance(d["checks"], list)


class TestRunCommentChecks:
    """Tests para QualityRunner.run_comment_checks."""

    def test_should_return_quality_report_when_called(
        self, runner, sample_comments, valid_parent_df
    ):
        """Verifica que retorna un QualityReport."""
        report = runner.run_comment_checks(
            sample_comments, valid_parent_df, "2026-02-01"
        )

        check.is_instance(report, QualityReport)

    def test_should_pass_all_checks_when_comments_valid(
        self, runner, sample_comments, valid_parent_df
    ):
        """Verifica que todos los checks pasan con comments válidos."""
        report = runner.run_comment_checks(
            sample_comments, valid_parent_df, "2026-02-01"
        )

        check.equal(report.failed_checks, 0)
        check.is_false(report.has_critical_failures)

    def test_should_run_not_null_check_on_required_columns(self, runner):
        """Verifica que detecta nulos en columnas requeridas de comments."""
        comments_null_parent = pd.DataFrame(
            [
                {
                    "id": 201,
                    "type": "comment",
                    "time": 1706749200,
                    "parent": None,
                    "ingestion_date": "2026-02-01",
                }
            ]
        )
        parent_df = pd.DataFrame([{"id": 100}])

        report = runner.run_comment_checks(
            comments_null_parent, parent_df, "2026-02-01"
        )

        check.is_true(report.has_critical_failures)
        failed_names = [c["name"] for c in report.checks if not c["passed"]]
        check.is_in("check_not_null", failed_names)

    def test_should_run_unique_check_on_id_and_ingestion_date(self, runner):
        """Verifica que detecta duplicados en comments."""
        duped_comments = pd.DataFrame(
            [
                {
                    "id": 201,
                    "type": "comment",
                    "time": 1706749200,
                    "parent": 100,
                    "ingestion_date": "2026-02-01",
                },
                {
                    "id": 201,
                    "type": "comment",
                    "time": 1706749200,
                    "parent": 100,
                    "ingestion_date": "2026-02-01",
                },
            ]
        )
        parent_df = pd.DataFrame([{"id": 100}])

        report = runner.run_comment_checks(duped_comments, parent_df, "2026-02-01")

        check.is_true(report.has_critical_failures)
        failed_names = [c["name"] for c in report.checks if not c["passed"]]
        check.is_in("check_unique", failed_names)

    def test_should_run_referential_integrity_check(self, runner):
        """Verifica que detecta comentarios huérfanos."""
        comments = pd.DataFrame(
            [
                {
                    "id": 201,
                    "type": "comment",
                    "time": 1706749200,
                    "parent": 999,
                    "ingestion_date": "2026-02-01",
                }
            ]
        )
        parent_df = pd.DataFrame([{"id": 100}])

        report = runner.run_comment_checks(comments, parent_df, "2026-02-01")

        check.is_true(report.has_critical_failures)
        failed_names = [c["name"] for c in report.checks if not c["passed"]]
        check.is_in("check_referential_integrity", failed_names)

    def test_should_have_four_checks_for_valid_comments(
        self, runner, sample_comments, valid_parent_df
    ):
        """Verifica que se ejecutan exactamente 4 checks para comments."""
        report = runner.run_comment_checks(
            sample_comments, valid_parent_df, "2026-02-01"
        )

        # not_null, unique, referential_integrity, volume
        check.equal(report.total_checks, 4)


class TestBuildReport:
    """Tests para la lógica interna de construcción de reportes."""

    def test_should_count_passed_and_failed_correctly(self, runner, sample_stories):
        """Verifica conteo correcto de checks pasados y fallidos."""
        report = runner.run_story_checks(sample_stories, "2026-02-01")

        check.equal(report.passed_checks + report.failed_checks, report.total_checks)

    def test_should_set_critical_failures_false_when_only_warnings_fail(self, runner):
        """Verifica que no marca critical cuando solo fallan warnings."""
        stories_negative_score = pd.DataFrame(
            [
                {
                    "id": 100,
                    "type": "story",
                    "time": 1706745600,
                    "score": -5,
                    "descendants": -1,
                    "ingestion_date": "2026-02-01",
                }
            ]
        )

        report = runner.run_story_checks(stories_negative_score, "2026-02-01")

        # score y descendants fuera de rango son warnings
        check.is_false(report.has_critical_failures)
        check.greater(report.failed_checks, 0)

    def test_should_set_critical_failures_true_when_critical_check_fails(self, runner):
        """Verifica que marca critical cuando un check crítico falla."""
        stories_null_pk = pd.DataFrame(
            [
                {
                    "id": None,
                    "type": "story",
                    "time": 1706745600,
                    "score": 50,
                    "descendants": 10,
                    "ingestion_date": "2026-02-01",
                }
            ]
        )

        report = runner.run_story_checks(stories_null_pk, "2026-02-01")

        check.is_true(report.has_critical_failures)

    def test_should_serialize_all_check_results_in_checks_list(
        self, runner, sample_stories
    ):
        """Verifica que todos los resultados están serializados en checks."""
        report = runner.run_story_checks(sample_stories, "2026-02-01")

        check.equal(len(report.checks), report.total_checks)
        for c in report.checks:
            check.is_in("name", c)
            check.is_in("passed", c)
            check.is_in("severity", c)
            check.is_in("description", c)
            check.is_in("affected_records", c)

    def test_should_handle_empty_dataframe_without_error(self, runner):
        """Verifica que no falla con DataFrame vacío."""
        empty_df = pd.DataFrame(
            columns=[
                "id",
                "type",
                "time",
                "score",
                "descendants",
                "ingestion_date",
            ]
        )

        report = runner.run_story_checks(empty_df, "2026-02-01")

        check.is_instance(report, QualityReport)
        # Volume check should fail (0 < 1)
        volume_checks = [c for c in report.checks if c["name"] == "check_volume"]
        check.equal(len(volume_checks), 1)
        check.is_false(volume_checks[0]["passed"])


class TestCriticalFailurePolicy:
    """Tests para la política de severidad del runner."""

    def test_null_in_pk_should_be_critical(self, runner):
        """Verifica que nulos en PK (id) son marcados como critical."""
        stories = pd.DataFrame(
            [
                {
                    "id": None,
                    "type": "story",
                    "time": 1706745600,
                    "score": 50,
                    "descendants": 10,
                    "ingestion_date": "2026-02-01",
                }
            ]
        )

        report = runner.run_story_checks(stories, "2026-02-01")

        critical_fails = [
            c for c in report.checks if not c["passed"] and c["severity"] == "critical"
        ]
        check.greater(len(critical_fails), 0)
        check.is_true(report.has_critical_failures)

    def test_duplicate_pk_should_be_critical(self, runner):
        """Verifica que duplicados en PK son marcados como critical."""
        stories = pd.DataFrame(
            [
                {
                    "id": 100,
                    "type": "story",
                    "time": 1706745600,
                    "score": 50,
                    "descendants": 10,
                    "ingestion_date": "2026-02-01",
                },
                {
                    "id": 100,
                    "type": "story",
                    "time": 1706745600,
                    "score": 75,
                    "descendants": 20,
                    "ingestion_date": "2026-02-01",
                },
            ]
        )

        report = runner.run_story_checks(stories, "2026-02-01")

        check.is_true(report.has_critical_failures)

    def test_negative_score_should_be_warning_not_critical(self, runner):
        """Verifica que score negativo es warning, no critical."""
        stories = pd.DataFrame(
            [
                {
                    "id": 100,
                    "type": "story",
                    "time": 1706745600,
                    "score": -10,
                    "descendants": 10,
                    "ingestion_date": "2026-02-01",
                }
            ]
        )

        report = runner.run_story_checks(stories, "2026-02-01")

        check.is_false(report.has_critical_failures)
        warning_fails = [
            c for c in report.checks if not c["passed"] and c["severity"] == "warning"
        ]
        check.greater(len(warning_fails), 0)

    def test_low_volume_should_be_warning_not_critical(self, runner):
        """Verifica que volumen bajo es warning, no critical."""
        empty = pd.DataFrame(
            columns=[
                "id",
                "type",
                "time",
                "score",
                "descendants",
                "ingestion_date",
            ]
        )

        report = runner.run_story_checks(empty, "2026-02-01")

        volume_checks = [c for c in report.checks if c["name"] == "check_volume"]
        check.equal(len(volume_checks), 1)
        check.equal(volume_checks[0]["severity"], "warning")

    def test_orphan_comment_should_be_critical(self, runner):
        """Verifica que integridad referencial rota es critical."""
        comments = pd.DataFrame(
            [
                {
                    "id": 201,
                    "type": "comment",
                    "time": 1706749200,
                    "parent": 999,
                    "ingestion_date": "2026-02-01",
                }
            ]
        )
        parent_df = pd.DataFrame([{"id": 100}])

        report = runner.run_comment_checks(comments, parent_df, "2026-02-01")

        ref_checks = [
            c for c in report.checks if c["name"] == "check_referential_integrity"
        ]
        check.equal(len(ref_checks), 1)
        check.equal(ref_checks[0]["severity"], "critical")
        check.is_true(report.has_critical_failures)
