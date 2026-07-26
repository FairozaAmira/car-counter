"""Regression tests for repository naming and diagnostic conventions."""

import ast
from pathlib import Path

from pydantic import BaseModel

from src.db.models import TrafficAnalysisResult
from src.schemas.kafka import KafkaAnalysisRequest, KafkaAnalysisResult, KafkaPublishItem
from src.schemas.traffic import (
    AnalysisDataResponse,
    AnalysisResult,
    BatchAnalysisItem,
    BatchAnalysisItemResponse,
    BatchAnalysisResponse,
    BatchAnalysisResult,
    DailyTotal,
    DailyTotalResponse,
    ErrorDetail,
    LeastCarsPeriod,
    LeastCarsPeriodResponse,
    ResponseMetadata,
    TrafficRecord,
    TrafficRecordResponse,
)

PROJECT_ROOT = Path(__file__).parents[3]
SCHEMA_MODELS: tuple[type[BaseModel], ...] = (
    TrafficRecord,
    DailyTotal,
    LeastCarsPeriod,
    AnalysisResult,
    TrafficRecordResponse,
    DailyTotalResponse,
    LeastCarsPeriodResponse,
    AnalysisDataResponse,
    ResponseMetadata,
    ErrorDetail,
    BatchAnalysisItem,
    BatchAnalysisResult,
    BatchAnalysisItemResponse,
    BatchAnalysisResponse,
    KafkaAnalysisRequest,
    KafkaAnalysisResult,
    KafkaPublishItem,
)


def hasRequiredParameter(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Return whether a function has a required caller-supplied parameter."""
    try:
        positional = [*node.args.posonlyargs, *node.args.args]
        if positional and positional[0].arg in {"self", "cls"}:
            positional = positional[1:]
        return len(positional) > len(node.args.defaults) or any(
            default is None for default in node.args.kw_defaults
        )
    except Exception as e:
        print(f"Error in hasRequiredParameter: {e}")
        raise


def hasDiagnosticBoundary(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Return whether a function uses the required outer exception boundary."""
    try:
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            body = body[1:]
        if len(body) != 1 or not isinstance(body[0], ast.Try):
            return False
        return any(
            isinstance(handler.type, ast.Name)
            and handler.type.id == "Exception"
            and handler.name == "e"
            for handler in body[0].handlers
        )
    except Exception as e:
        print(f"Error in hasDiagnosticBoundary: {e}")
        raise


def test_required_parameter_functions_have_diagnostic_boundaries() -> None:
    """Require the requested try/except boundary on applicable application functions."""
    missing: list[str] = []
    for path in (PROJECT_ROOT / "src").rglob("*.py"):
        if "tests" in path.parts or "versions" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and hasRequiredParameter(node)
                and not hasDiagnosticBoundary(node)
            ):
                missing.append(f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}:{node.name}")

    assert missing == []


def test_payload_and_database_fields_are_camel_case() -> None:
    """Require native camelCase schema fields and persisted column names."""
    invalidFields = {
        f"{model.__name__}.{fieldName}"
        for model in SCHEMA_MODELS
        for fieldName in model.model_fields
        if "_" in fieldName
    }
    invalidColumns = {
        column.name for column in TrafficAnalysisResult.__table__.columns if "_" in column.name
    }

    assert invalidFields == set()
    assert invalidColumns == set()
