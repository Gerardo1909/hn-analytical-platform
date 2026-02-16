"""
Checks reutilizables de calidad de datos para entidades de Hacker News.

Cada check retorna un resultado estructurado sin lanzar excepciones para
que una capa superior (runner/orquestador) tome decisiones de continuidad.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class CheckResult:
    """
    Resultado estructurado de un check de calidad.

    Attributes:
        name: Identificador del check ejecutado
        passed: True si el check pasó sin problemas
        severity: Nivel de severidad (critical, warning)
        description: Descripción legible del resultado
        affected_records: Cantidad de registros afectados
        details: Metadata adicional del check
        sample_ids: Muestra de IDs afectados para diagnóstico
    """

    name: str
    passed: bool
    severity: str
    description: str
    affected_records: int = 0
    details: Dict[str, Any] = field(default_factory=dict)
    sample_ids: List[int] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convierte el resultado a diccionario serializable."""
        return {
            "name": self.name,
            "passed": self.passed,
            "severity": self.severity,
            "description": self.description,
            "affected_records": self.affected_records,
            "details": self.details,
            "sample_ids": self.sample_ids,
        }


def _extract_sample_ids(
    df: pd.DataFrame, mask: pd.Series, sample_size: int = 10
) -> List[int]:
    """
    Extrae una muestra de IDs de registros afectados.

    Args:
        df: DataFrame fuente
        mask: Máscara booleana de registros afectados
        sample_size: Tamaño máximo de la muestra

    Returns:
        Lista de IDs (enteros) afectados, limitada a sample_size
    """
    if "id" not in df.columns or df.empty or mask.sum() == 0:
        return []

    return (
        pd.to_numeric(df.loc[mask, "id"], errors="coerce")
        .dropna()
        .astype(int)
        .head(sample_size)
        .tolist()
    )


def check_not_null(
    df: pd.DataFrame,
    columns: List[str],
    severity: str = "critical",
    sample_size: int = 10,
) -> CheckResult:
    """
    Verifica que las columnas indicadas no contengan nulos.

    Args:
        df: DataFrame a validar
        columns: Columnas que deben ser NOT NULL
        severity: Severidad del check (critical/warning)
        sample_size: Cantidad máxima de IDs de ejemplo a retornar
    """
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        return CheckResult(
            name="check_not_null",
            passed=False,
            severity=severity,
            description=f"Columnas requeridas ausentes: {missing_columns}",
            affected_records=len(df),
            details={"missing_columns": missing_columns, "columns_checked": columns},
        )

    null_mask = df[columns].isna().any(axis=1)
    affected = int(null_mask.sum())

    if affected == 0:
        return CheckResult(
            name="check_not_null",
            passed=True,
            severity=severity,
            description=f"Sin nulos en columnas requeridas: {columns}",
            affected_records=0,
            details={"columns_checked": columns},
        )

    return CheckResult(
        name="check_not_null",
        passed=False,
        severity=severity,
        description=f"Nulos detectados en columnas requeridas: {columns}",
        affected_records=affected,
        details={"columns_checked": columns},
        sample_ids=_extract_sample_ids(df, null_mask, sample_size),
    )


def check_unique(
    df: pd.DataFrame,
    columns: List[str],
    severity: str = "critical",
    sample_size: int = 10,
) -> CheckResult:
    """
    Verifica unicidad por combinación de columnas.

    Args:
        df: DataFrame a validar
        columns: Clave de unicidad
        severity: Severidad del check (critical/warning)
        sample_size: Cantidad máxima de IDs de ejemplo a retornar
    """
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        return CheckResult(
            name="check_unique",
            passed=False,
            severity=severity,
            description=f"Columnas de unicidad ausentes: {missing_columns}",
            affected_records=len(df),
            details={"missing_columns": missing_columns, "columns_checked": columns},
        )

    duplicated_mask = df.duplicated(subset=columns, keep=False)
    affected = int(duplicated_mask.sum())

    if affected == 0:
        return CheckResult(
            name="check_unique",
            passed=True,
            severity=severity,
            description=f"Unicidad válida para columnas: {columns}",
            affected_records=0,
            details={"columns_checked": columns},
        )

    return CheckResult(
        name="check_unique",
        passed=False,
        severity=severity,
        description=f"Duplicados detectados por columnas: {columns}",
        affected_records=affected,
        details={"columns_checked": columns},
        sample_ids=_extract_sample_ids(df, duplicated_mask, sample_size),
    )


def check_range(
    df: pd.DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    severity: str = "warning",
    sample_size: int = 10,
) -> CheckResult:
    """
    Verifica que una columna numérica esté dentro de un rango [min, max].

    Args:
        df: DataFrame a validar
        column: Columna objetivo
        min_value: Valor mínimo permitido (inclusive)
        max_value: Valor máximo permitido (inclusive)
        severity: Severidad del check (critical/warning)
        sample_size: Cantidad máxima de IDs de ejemplo a retornar
    """
    if column not in df.columns:
        return CheckResult(
            name="check_range",
            passed=False,
            severity=severity,
            description=f"Columna ausente para validación de rango: '{column}'",
            affected_records=len(df),
            details={"column": column, "min_value": min_value, "max_value": max_value},
        )

    numeric = pd.to_numeric(df[column], errors="coerce")
    invalid_mask = pd.Series(False, index=df.index)

    if min_value is not None:
        invalid_mask = invalid_mask | (numeric < min_value)
    if max_value is not None:
        invalid_mask = invalid_mask | (numeric > max_value)

    affected = int(invalid_mask.sum())

    if affected == 0:
        return CheckResult(
            name="check_range",
            passed=True,
            severity=severity,
            description=f"Rango válido para columna '{column}'",
            affected_records=0,
            details={"column": column, "min_value": min_value, "max_value": max_value},
        )

    return CheckResult(
        name="check_range",
        passed=False,
        severity=severity,
        description=f"Valores fuera de rango en '{column}'",
        affected_records=affected,
        details={"column": column, "min_value": min_value, "max_value": max_value},
        sample_ids=_extract_sample_ids(df, invalid_mask, sample_size),
    )


def check_referential_integrity(
    df_child: pd.DataFrame,
    df_parent: pd.DataFrame,
    child_key: str,
    parent_key: str,
    severity: str = "critical",
    sample_size: int = 10,
) -> CheckResult:
    """
    Verifica integridad referencial: cada valor de child_key debe existir
    en parent_key del DataFrame padre.

    Args:
        df_child: DataFrame hijo (ej: comments)
        df_parent: DataFrame padre (ej: stories + comments como universo válido)
        child_key: Columna FK en df_child
        parent_key: Columna PK en df_parent
        severity: Severidad del check (critical/warning)
        sample_size: Cantidad máxima de IDs de ejemplo a retornar
    """
    if child_key not in df_child.columns:
        return CheckResult(
            name="check_referential_integrity",
            passed=False,
            severity=severity,
            description=f"Clave hija ausente: '{child_key}'",
            affected_records=len(df_child),
            details={"child_key": child_key, "parent_key": parent_key},
        )

    if parent_key not in df_parent.columns:
        return CheckResult(
            name="check_referential_integrity",
            passed=False,
            severity=severity,
            description=f"Clave padre ausente: '{parent_key}'",
            affected_records=len(df_child),
            details={"child_key": child_key, "parent_key": parent_key},
        )

    valid_parent_values = set(
        pd.to_numeric(df_parent[parent_key], errors="coerce").dropna().tolist()
    )
    child_values = pd.to_numeric(df_child[child_key], errors="coerce")
    invalid_mask = ~child_values.isin(valid_parent_values)
    affected = int(invalid_mask.sum())

    if affected == 0:
        return CheckResult(
            name="check_referential_integrity",
            passed=True,
            severity=severity,
            description=f"Integridad referencial válida: '{child_key}' -> '{parent_key}'",
            affected_records=0,
            details={
                "child_key": child_key,
                "parent_key": parent_key,
                "valid_parent_count": len(valid_parent_values),
            },
        )

    return CheckResult(
        name="check_referential_integrity",
        passed=False,
        severity=severity,
        description=f"Referencias inválidas de '{child_key}' hacia '{parent_key}'",
        affected_records=affected,
        details={
            "child_key": child_key,
            "parent_key": parent_key,
            "valid_parent_count": len(valid_parent_values),
        },
        sample_ids=_extract_sample_ids(df_child, invalid_mask, sample_size),
    )


def check_volume(
    df: pd.DataFrame,
    entity: str,
    min_expected: int,
    severity: str = "warning",
) -> CheckResult:
    """
    Verifica volumen mínimo esperado por entidad.

    Args:
        df: DataFrame a validar
        entity: Nombre de la entidad (stories/comments)
        min_expected: Mínimo esperado de registros
        severity: Severidad del check (critical/warning)
    """
    actual = len(df)
    passed = actual >= min_expected

    return CheckResult(
        name="check_volume",
        passed=passed,
        severity=severity,
        description=(
            f"Volumen válido para '{entity}': {actual} >= {min_expected}"
            if passed
            else f"Volumen insuficiente para '{entity}': {actual} < {min_expected}"
        ),
        affected_records=0 if passed else (min_expected - actual),
        details={
            "entity": entity,
            "actual_count": actual,
            "min_expected": min_expected,
        },
    )
