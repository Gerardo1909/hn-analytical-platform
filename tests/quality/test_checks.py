"""
Tests unitarios para quality.checks.
Verifica cada check individual con casos de éxito, fallo y edge cases.
"""

import pandas as pd
import pytest_check as check

from quality.checks import (
    CheckResult,
    check_not_null,
    check_range,
    check_referential_integrity,
    check_unique,
    check_volume,
)


class TestCheckResult:
    """Tests para el dataclass CheckResult."""

    def test_to_dict_should_return_all_fields_when_called(self):
        """Verifica que to_dict serializa todos los campos."""
        result = CheckResult(
            name="test_check",
            passed=True,
            severity="critical",
            description="Test description",
            affected_records=0,
            details={"key": "value"},
            sample_ids=[1, 2, 3],
        )

        d = result.to_dict()

        check.equal(d["name"], "test_check")
        check.equal(d["passed"], True)
        check.equal(d["severity"], "critical")
        check.equal(d["description"], "Test description")
        check.equal(d["affected_records"], 0)
        check.equal(d["details"], {"key": "value"})
        check.equal(d["sample_ids"], [1, 2, 3])

    def test_to_dict_should_use_defaults_when_optional_fields_omitted(self):
        """Verifica que los campos opcionales tienen defaults correctos."""
        result = CheckResult(
            name="test",
            passed=True,
            severity="warning",
            description="desc",
        )

        d = result.to_dict()

        check.equal(d["affected_records"], 0)
        check.equal(d["details"], {})
        check.equal(d["sample_ids"], [])


class TestCheckNotNull:
    """Tests para check_not_null."""

    def test_should_pass_when_no_nulls_in_checked_columns(self):
        """Verifica que pasa cuando no hay nulos."""
        df = pd.DataFrame(
            [
                {"id": 100, "type": "story", "time": 1706745600},
                {"id": 200, "type": "story", "time": 1706832000},
            ]
        )

        result = check_not_null(df, columns=["id", "type", "time"])

        check.is_true(result.passed)
        check.equal(result.affected_records, 0)
        check.equal(result.name, "check_not_null")

    def test_should_fail_when_nulls_present_in_checked_columns(self):
        """Verifica que falla cuando hay nulos."""
        df = pd.DataFrame(
            [
                {"id": 100, "type": "story", "time": 1706745600},
                {"id": None, "type": "story", "time": 1706832000},
                {"id": 300, "type": None, "time": 1706918400},
            ]
        )

        result = check_not_null(df, columns=["id", "type"])

        check.is_false(result.passed)
        check.equal(result.affected_records, 2)

    def test_should_fail_when_column_missing_from_dataframe(self):
        """Verifica que falla cuando una columna requerida no existe."""
        df = pd.DataFrame([{"id": 100, "type": "story"}])

        result = check_not_null(df, columns=["id", "nonexistent"])

        check.is_false(result.passed)
        check.equal(result.affected_records, len(df))
        check.is_in("nonexistent", result.details["missing_columns"])

    def test_should_use_provided_severity_when_specified(self):
        """Verifica que usa la severidad indicada."""
        df = pd.DataFrame([{"id": 100}])

        result = check_not_null(df, columns=["id"], severity="warning")

        check.equal(result.severity, "warning")

    def test_should_include_sample_ids_when_failures_exist(self):
        """Verifica que incluye IDs afectados en la muestra."""
        df = pd.DataFrame(
            [
                {"id": 100, "type": "story"},
                {"id": 200, "type": None},
                {"id": 300, "type": None},
            ]
        )

        result = check_not_null(df, columns=["type"])

        check.equal(len(result.sample_ids), 2)
        check.is_in(200, result.sample_ids)
        check.is_in(300, result.sample_ids)

    def test_should_limit_sample_ids_to_sample_size(self):
        """Verifica que limita la muestra al tamaño indicado."""
        df = pd.DataFrame([{"id": i, "val": None} for i in range(20)])

        result = check_not_null(df, columns=["val"], sample_size=5)

        check.less_equal(len(result.sample_ids), 5)

    def test_should_pass_with_empty_dataframe_when_no_rows(self):
        """Verifica que pasa con DataFrame vacío (0 nulos = pass)."""
        df = pd.DataFrame(columns=["id", "type"])

        result = check_not_null(df, columns=["id", "type"])

        check.is_true(result.passed)
        check.equal(result.affected_records, 0)


class TestCheckUnique:
    """Tests para check_unique."""

    def test_should_pass_when_no_duplicates_exist(self):
        """Verifica que pasa sin duplicados."""
        df = pd.DataFrame(
            [
                {"id": 100, "ingestion_date": "2026-02-01"},
                {"id": 200, "ingestion_date": "2026-02-01"},
                {"id": 100, "ingestion_date": "2026-02-02"},
            ]
        )

        result = check_unique(df, columns=["id", "ingestion_date"])

        check.is_true(result.passed)
        check.equal(result.affected_records, 0)

    def test_should_fail_when_duplicates_detected(self):
        """Verifica que falla con duplicados."""
        df = pd.DataFrame(
            [
                {"id": 100, "ingestion_date": "2026-02-01"},
                {"id": 100, "ingestion_date": "2026-02-01"},
                {"id": 200, "ingestion_date": "2026-02-01"},
            ]
        )

        result = check_unique(df, columns=["id", "ingestion_date"])

        check.is_false(result.passed)
        check.equal(result.affected_records, 2)

    def test_should_fail_when_column_missing(self):
        """Verifica que falla cuando la columna de unicidad no existe."""
        df = pd.DataFrame([{"id": 100}])

        result = check_unique(df, columns=["id", "missing_col"])

        check.is_false(result.passed)
        check.is_in("missing_col", result.details["missing_columns"])

    def test_should_include_sample_ids_of_duplicated_rows(self):
        """Verifica que incluye IDs de filas duplicadas."""
        df = pd.DataFrame(
            [
                {"id": 100, "ingestion_date": "2026-02-01"},
                {"id": 100, "ingestion_date": "2026-02-01"},
            ]
        )

        result = check_unique(df, columns=["id", "ingestion_date"])

        check.equal(len(result.sample_ids), 2)
        check.is_in(100, result.sample_ids)

    def test_should_use_severity_when_specified(self):
        """Verifica que respeta la severidad indicada."""
        df = pd.DataFrame([{"id": 100}])

        result = check_unique(df, columns=["id"], severity="warning")

        check.equal(result.severity, "warning")


class TestCheckRange:
    """Tests para check_range."""

    def test_should_pass_when_values_within_range(self):
        """Verifica que pasa cuando todos los valores están en rango."""
        df = pd.DataFrame(
            [
                {"id": 100, "score": 0},
                {"id": 200, "score": 50},
                {"id": 300, "score": 1000},
            ]
        )

        result = check_range(df, column="score", min_value=0, max_value=10000)

        check.is_true(result.passed)
        check.equal(result.affected_records, 0)

    def test_should_fail_when_values_below_min(self):
        """Verifica que falla con valores por debajo del mínimo."""
        df = pd.DataFrame(
            [
                {"id": 100, "score": -5},
                {"id": 200, "score": 10},
                {"id": 300, "score": -1},
            ]
        )

        result = check_range(df, column="score", min_value=0)

        check.is_false(result.passed)
        check.equal(result.affected_records, 2)

    def test_should_fail_when_values_above_max(self):
        """Verifica que falla con valores por encima del máximo."""
        df = pd.DataFrame(
            [
                {"id": 100, "score": 50},
                {"id": 200, "score": 200},
            ]
        )

        result = check_range(df, column="score", max_value=100)

        check.is_false(result.passed)
        check.equal(result.affected_records, 1)

    def test_should_fail_when_column_missing(self):
        """Verifica que falla cuando la columna no existe."""
        df = pd.DataFrame([{"id": 100}])

        result = check_range(df, column="score", min_value=0)

        check.is_false(result.passed)
        check.equal(result.affected_records, len(df))

    def test_should_pass_when_only_min_specified_and_all_above(self):
        """Verifica que pasa con solo min y todos por encima."""
        df = pd.DataFrame([{"id": 100, "score": 0}, {"id": 200, "score": 100}])

        result = check_range(df, column="score", min_value=0)

        check.is_true(result.passed)

    def test_should_pass_when_only_max_specified_and_all_below(self):
        """Verifica que pasa con solo max y todos por debajo."""
        df = pd.DataFrame([{"id": 100, "score": 50}, {"id": 200, "score": 100}])

        result = check_range(df, column="score", max_value=100)

        check.is_true(result.passed)

    def test_should_include_sample_ids_for_out_of_range_values(self):
        """Verifica que incluye IDs de registros fuera de rango."""
        df = pd.DataFrame(
            [
                {"id": 100, "score": -10},
                {"id": 200, "score": 50},
                {"id": 300, "score": -5},
            ]
        )

        result = check_range(df, column="score", min_value=0)

        check.is_in(100, result.sample_ids)
        check.is_in(300, result.sample_ids)
        check.is_not_in(200, result.sample_ids)

    def test_should_use_default_warning_severity(self):
        """Verifica que usa warning como severidad por defecto."""
        df = pd.DataFrame([{"id": 100, "score": 0}])

        result = check_range(df, column="score", min_value=0)

        check.equal(result.severity, "warning")


class TestCheckReferentialIntegrity:
    """Tests para check_referential_integrity."""

    def test_should_pass_when_all_children_have_valid_parent(self):
        """Verifica que pasa cuando todos los hijos tienen parent válido."""
        parent_df = pd.DataFrame([{"id": 100}, {"id": 200}])
        child_df = pd.DataFrame(
            [
                {"id": 301, "parent": 100},
                {"id": 302, "parent": 200},
            ]
        )

        result = check_referential_integrity(
            df_child=child_df,
            df_parent=parent_df,
            child_key="parent",
            parent_key="id",
        )

        check.is_true(result.passed)
        check.equal(result.affected_records, 0)

    def test_should_fail_when_orphan_references_exist(self):
        """Verifica que falla con referencias huérfanas."""
        parent_df = pd.DataFrame([{"id": 100}])
        child_df = pd.DataFrame(
            [
                {"id": 301, "parent": 100},
                {"id": 302, "parent": 999},
                {"id": 303, "parent": 888},
            ]
        )

        result = check_referential_integrity(
            df_child=child_df,
            df_parent=parent_df,
            child_key="parent",
            parent_key="id",
        )

        check.is_false(result.passed)
        check.equal(result.affected_records, 2)

    def test_should_fail_when_child_key_column_missing(self):
        """Verifica que falla cuando la FK no existe en df_child."""
        parent_df = pd.DataFrame([{"id": 100}])
        child_df = pd.DataFrame([{"id": 301}])

        result = check_referential_integrity(
            df_child=child_df,
            df_parent=parent_df,
            child_key="parent",
            parent_key="id",
        )

        check.is_false(result.passed)
        check.equal(result.affected_records, len(child_df))

    def test_should_fail_when_parent_key_column_missing(self):
        """Verifica que falla cuando la PK no existe en df_parent."""
        parent_df = pd.DataFrame([{"name": "story1"}])
        child_df = pd.DataFrame([{"id": 301, "parent": 100}])

        result = check_referential_integrity(
            df_child=child_df,
            df_parent=parent_df,
            child_key="parent",
            parent_key="id",
        )

        check.is_false(result.passed)

    def test_should_include_sample_ids_of_orphan_children(self):
        """Verifica que incluye IDs de hijos huérfanos."""
        parent_df = pd.DataFrame([{"id": 100}])
        child_df = pd.DataFrame(
            [
                {"id": 301, "parent": 100},
                {"id": 302, "parent": 999},
            ]
        )

        result = check_referential_integrity(
            df_child=child_df,
            df_parent=parent_df,
            child_key="parent",
            parent_key="id",
        )

        check.is_in(302, result.sample_ids)
        check.is_not_in(301, result.sample_ids)

    def test_should_include_valid_parent_count_in_details(self):
        """Verifica que reporta la cantidad de parents válidos."""
        parent_df = pd.DataFrame([{"id": 100}, {"id": 200}, {"id": 300}])
        child_df = pd.DataFrame([{"id": 301, "parent": 100}])

        result = check_referential_integrity(
            df_child=child_df,
            df_parent=parent_df,
            child_key="parent",
            parent_key="id",
        )

        check.equal(result.details["valid_parent_count"], 3)

    def test_should_use_default_critical_severity(self):
        """Verifica que usa critical como severidad por defecto."""
        parent_df = pd.DataFrame([{"id": 100}])
        child_df = pd.DataFrame([{"id": 301, "parent": 100}])

        result = check_referential_integrity(
            df_child=child_df,
            df_parent=parent_df,
            child_key="parent",
            parent_key="id",
        )

        check.equal(result.severity, "critical")


class TestCheckVolume:
    """Tests para check_volume."""

    def test_should_pass_when_volume_meets_minimum(self):
        """Verifica que pasa cuando el volumen cumple el mínimo."""
        df = pd.DataFrame([{"id": 100}, {"id": 200}, {"id": 300}])

        result = check_volume(df, entity="stories", min_expected=3)

        check.is_true(result.passed)
        check.equal(result.affected_records, 0)

    def test_should_pass_when_volume_exceeds_minimum(self):
        """Verifica que pasa cuando el volumen supera el mínimo."""
        df = pd.DataFrame([{"id": 100}, {"id": 200}])

        result = check_volume(df, entity="stories", min_expected=1)

        check.is_true(result.passed)

    def test_should_fail_when_volume_below_minimum(self):
        """Verifica que falla cuando el volumen es menor al mínimo."""
        df = pd.DataFrame([{"id": 100}])

        result = check_volume(df, entity="stories", min_expected=10)

        check.is_false(result.passed)
        check.equal(result.affected_records, 9)

    def test_should_fail_when_dataframe_empty_and_min_greater_than_zero(self):
        """Verifica que falla con DataFrame vacío y mínimo > 0."""
        df = pd.DataFrame()

        result = check_volume(df, entity="comments", min_expected=1)

        check.is_false(result.passed)

    def test_should_include_entity_in_details(self):
        """Verifica que incluye la entidad en los detalles."""
        df = pd.DataFrame([{"id": 100}])

        result = check_volume(df, entity="stories", min_expected=1)

        check.equal(result.details["entity"], "stories")
        check.equal(result.details["actual_count"], 1)
        check.equal(result.details["min_expected"], 1)

    def test_should_use_default_warning_severity(self):
        """Verifica que usa warning como severidad por defecto."""
        df = pd.DataFrame([{"id": 100}])

        result = check_volume(df, entity="stories", min_expected=1)

        check.equal(result.severity, "warning")

    def test_should_include_description_with_counts(self):
        """Verifica que la descripción incluye conteos."""
        df = pd.DataFrame([{"id": 100}])

        result = check_volume(df, entity="stories", min_expected=5)

        check.is_in("1", result.description)
        check.is_in("5", result.description)
