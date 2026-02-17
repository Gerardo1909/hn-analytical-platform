"""
Runner de calidad que orquesta la ejecución de checks y produce
un reporte consolidado apto para decisiones de orquestación.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from quality.checks import (
    CheckResult,
    check_not_null,
    check_range,
    check_referential_integrity,
    check_unique,
    check_volume,
)
from utils.logger import quality_logger as logger


class QualityCheckError(Exception):
    """Se lanza cuando checks de calidad críticos fallan."""

    pass


@dataclass
class QualityReport:
    """
    Reporte consolidado de ejecución de checks de calidad.

    Attributes:
        ingestion_date: Fecha de ingesta evaluada
        generated_at: Timestamp de generación del reporte
        total_checks: Cantidad total de checks ejecutados
        passed_checks: Cantidad de checks que pasaron
        failed_checks: Cantidad de checks que fallaron
        has_critical_failures: True si algún check crítico falló
        entities: Resumen de checks por entidad
        checks: Lista de resultados individuales serializados
    """

    ingestion_date: str
    generated_at: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    has_critical_failures: bool
    entities: Dict[str, Dict[str, int]]
    checks: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        """Serializa el reporte completo a diccionario."""
        return {
            "ingestion_date": self.ingestion_date,
            "generated_at": self.generated_at,
            "total_checks": self.total_checks,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "has_critical_failures": self.has_critical_failures,
            "entities": self.entities,
            "checks": self.checks,
        }


class QualityRunner:
    """
    Orquestador de checks de calidad.

    Ejecuta checks definidos en quality.checks sobre DataFrames de
    stories y comments, y construye un reporte consolidado.

    Política de severidad:
    - critical + failed => bloqueante (has_critical_failures=True)
    - warning + failed => no bloqueante, solo informativo
    """

    def run_story_checks(
        self, stories_df: pd.DataFrame, ingestion_date: str
    ) -> QualityReport:
        """
        Ejecuta checks estándar para stories.

        Checks aplicados:
        - NOT NULL en id, type, time, ingestion_date (critical)
        - Unicidad por (id, ingestion_date) (critical)
        - score >= 0 (warning)
        - descendants >= 0 (warning)
        - Volumen mínimo 1 registro (warning)

        Args:
            stories_df: DataFrame de stories normalizado y deduplicado
            ingestion_date: Fecha de ingesta evaluada

        Returns:
            QualityReport consolidado
        """
        results: List[CheckResult] = [
            check_not_null(
                df=stories_df,
                columns=["id", "type", "time", "ingestion_date"],
                severity="critical",
            ),
            check_unique(
                df=stories_df,
                columns=["id", "ingestion_date"],
                severity="critical",
            ),
            check_range(
                df=stories_df,
                column="score",
                min_value=0,
                severity="warning",
            ),
            check_range(
                df=stories_df,
                column="descendants",
                min_value=0,
                severity="warning",
            ),
            check_volume(
                df=stories_df,
                entity="stories",
                min_expected=1,
                severity="warning",
            ),
        ]

        return self._build_report(ingestion_date, results)

    def run_comment_checks(
        self,
        comments_df: pd.DataFrame,
        valid_parent_df: pd.DataFrame,
        ingestion_date: str,
    ) -> QualityReport:
        """
        Ejecuta checks estándar para comments.

        Checks aplicados:
        - NOT NULL en id, type, time, parent, ingestion_date (critical)
        - Unicidad por (id, ingestion_date) (critical)
        - Integridad referencial parent -> id en valid_parent_df (critical)
        - Volumen mínimo 1 registro (warning)

        Args:
            comments_df: DataFrame de comments normalizado y deduplicado
            valid_parent_df: DataFrame con IDs válidos (stories + comments)
            ingestion_date: Fecha de ingesta evaluada

        Returns:
            QualityReport consolidado
        """
        results: List[CheckResult] = [
            check_not_null(
                df=comments_df,
                columns=["id", "type", "time", "parent", "ingestion_date"],
                severity="critical",
            ),
            check_unique(
                df=comments_df,
                columns=["id", "ingestion_date"],
                severity="critical",
            ),
            check_referential_integrity(
                df_child=comments_df,
                df_parent=valid_parent_df,
                child_key="parent",
                parent_key="id",
                severity="critical",
            ),
            check_volume(
                df=comments_df,
                entity="comments",
                min_expected=1,
                severity="warning",
            ),
        ]

        return self._build_report(ingestion_date, results)

    def run_transformation_story_checks(
        self, stories_df: pd.DataFrame, ingestion_date: str
    ) -> QualityReport:
        """
        Ejecuta checks sobre stories enriquecidas por la capa de transformación.

        Verifica que las columnas agregadas durante el enriquecimiento
        temporal y de topics estén presentes y sean consistentes.

        Checks aplicados:
        - NOT NULL en score_velocity, comment_velocity, hours_to_peak,
        is_long_tail, observations_in_window (critical)
        - hours_to_peak >= 0 (warning)
        - observations_in_window >= 1 (warning)
        - Volumen mínimo 1 registro (warning)

        Args:
            stories_df: DataFrame de stories enriquecidas
            ingestion_date: Fecha de ingesta evaluada

        Returns:
            QualityReport consolidado
        """
        results: List[CheckResult] = [
            check_not_null(
                df=stories_df,
                columns=[
                    "score_velocity",
                    "comment_velocity",
                    "hours_to_peak",
                    "is_long_tail",
                    "observations_in_window",
                ],
                severity="critical",
            ),
            check_range(
                df=stories_df,
                column="hours_to_peak",
                min_value=0,
                severity="warning",
            ),
            check_range(
                df=stories_df,
                column="observations_in_window",
                min_value=1,
                severity="warning",
            ),
            check_volume(
                df=stories_df,
                entity="stories_enriched",
                min_expected=1,
                severity="warning",
            ),
        ]

        return self._build_report(ingestion_date, results)

    def run_transformation_comment_checks(
        self, comments_df: pd.DataFrame, ingestion_date: str
    ) -> QualityReport:
        """
        Ejecuta checks sobre comments enriquecidos por la capa de transformación.

        Verifica que las columnas de sentiment estén presentes y sean válidas.

        Checks aplicados:
        - NOT NULL en sentiment_score, sentiment_label (critical)
        - sentiment_score en rango [-1, 1] (warning)
        - Volumen mínimo 1 registro (warning)

        Args:
            comments_df: DataFrame de comments enriquecidos
            ingestion_date: Fecha de ingesta evaluada

        Returns:
            QualityReport consolidado
        """
        results: List[CheckResult] = [
            check_not_null(
                df=comments_df,
                columns=["sentiment_score", "sentiment_label"],
                severity="critical",
            ),
            check_range(
                df=comments_df,
                column="sentiment_score",
                min_value=-1,
                max_value=1,
                severity="warning",
            ),
            check_volume(
                df=comments_df,
                entity="comments_enriched",
                min_expected=1,
                severity="warning",
            ),
        ]

        return self._build_report(ingestion_date, results)

    def _build_report(
        self, ingestion_date: str, results: List[CheckResult]
    ) -> QualityReport:
        """
        Construye reporte consolidado a partir de resultados individuales.

        Args:
            ingestion_date: Fecha de ingesta evaluada
            results: Lista de CheckResult producidos por funciones de checks.py

        Returns:
            QualityReport consolidado
        """
        total = len(results)
        passed = sum(1 for r in results if r.passed)
        failed = total - passed
        has_critical = any(not r.passed and r.severity == "critical" for r in results)

        # Resumen por entidad inferida desde details o nombre
        entities: Dict[str, Dict[str, int]] = {}
        for result in results:
            entity = result.details.get("entity", "unknown")
            if entity not in entities:
                entities[entity] = {"passed": 0, "failed": 0}
            if result.passed:
                entities[entity]["passed"] += 1
            else:
                entities[entity]["failed"] += 1

        report = QualityReport(
            ingestion_date=ingestion_date,
            generated_at=datetime.utcnow().isoformat(),
            total_checks=total,
            passed_checks=passed,
            failed_checks=failed,
            has_critical_failures=has_critical,
            entities=entities,
            checks=[r.to_dict() for r in results],
        )

        logger.info(
            f"Reporte de calidad generado: "
            f"total={report.total_checks}, passed={report.passed_checks}, "
            f"failed={report.failed_checks}, "
            f"critical_failures={report.has_critical_failures}"
        )

        return report
