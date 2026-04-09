"""
Pipeline Spec Validator — validates YAML specs before the workflow starts.

Catches missing fields, bad types, and structural issues BEFORE they reach
the AI, preventing confusing downstream failures.
"""

import logging
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger("spec_validator")


class SpecValidationError(Exception):
    """Raised when a pipeline spec fails validation."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        msg = f"{len(errors)} validation error(s):\n" + "\n".join(f"  - {e}" for e in errors)
        super().__init__(msg)


def validate_spec(spec_path: str) -> dict:
    """
    Load and validate a pipeline YAML spec. Returns the parsed dict.
    Raises SpecValidationError with all issues found.
    """
    path = Path(spec_path)
    errors: list[str] = []

    if not path.exists():
        raise SpecValidationError([f"Spec file not found: {spec_path}"])

    try:
        raw = path.read_text()
    except Exception as e:
        raise SpecValidationError([f"Cannot read spec file: {e}"])

    try:
        spec = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise SpecValidationError([f"Invalid YAML syntax: {e}"])

    if not isinstance(spec, dict):
        raise SpecValidationError(["Spec must be a YAML mapping (dict), got: " + type(spec).__name__])

    pipeline = spec.get("pipeline")
    if not pipeline:
        raise SpecValidationError(["Missing top-level 'pipeline' key"])
    if not isinstance(pipeline, dict):
        raise SpecValidationError(["'pipeline' must be a mapping (dict)"])

    _validate_pipeline_root(pipeline, errors)
    _validate_extract(pipeline.get("extract"), errors)
    _validate_transform(pipeline.get("transform"), errors)
    _validate_load(pipeline.get("load"), errors)
    _validate_quality_checks(pipeline.get("quality_checks"), errors)
    _validate_dependencies(pipeline.get("dependencies"), errors)

    if errors:
        raise SpecValidationError(errors)

    logger.info(f"Spec validated: {pipeline.get('name')} ({path.name}) — no issues")
    return spec


def _validate_pipeline_root(pipeline: dict, errors: list[str]):
    if not pipeline.get("name"):
        errors.append("pipeline.name is required")
    elif not isinstance(pipeline["name"], str):
        errors.append("pipeline.name must be a string")
    else:
        name = pipeline["name"]
        if " " in name:
            errors.append(f"pipeline.name must not contain spaces (got: '{name}')")
        if not name.replace("_", "").isalnum():
            errors.append(f"pipeline.name must be alphanumeric with underscores (got: '{name}')")

    if not pipeline.get("description"):
        errors.append("pipeline.description is recommended (missing)")


def _validate_extract(extract: Optional[dict], errors: list[str]):
    if extract is None:
        errors.append("pipeline.extract section is required")
        return
    if not isinstance(extract, dict):
        errors.append("pipeline.extract must be a mapping")
        return

    if not extract.get("source"):
        errors.append("extract.source is required (e.g. 'mysql', 'postgresql', 'csv')")

    source = extract.get("source", "").lower()
    db_sources = {"mysql", "postgresql", "postgres", "mssql", "oracle", "sqlite"}

    if source in db_sources:
        if not extract.get("database"):
            errors.append(f"extract.database is required for source '{source}'")
        if not extract.get("table") and not extract.get("query"):
            errors.append(f"extract.table or extract.query is required for source '{source}'")
        if source != "sqlite":
            if not extract.get("host"):
                errors.append(f"extract.host is required for source '{source}'")
            if not extract.get("username"):
                errors.append(f"extract.username is required for source '{source}'")

    if extract.get("row_limit") is not None:
        try:
            val = int(extract["row_limit"])
            if val <= 0:
                errors.append("extract.row_limit must be a positive integer")
        except (ValueError, TypeError):
            errors.append(f"extract.row_limit must be an integer (got: {extract['row_limit']})")


def _validate_transform(transform: Optional[dict], errors: list[str]):
    if transform is None:
        return

    if isinstance(transform, dict):
        steps = transform.get("steps")
    elif isinstance(transform, list):
        steps = transform
    else:
        errors.append("pipeline.transform must be a mapping with 'steps' or a list")
        return

    if steps is None:
        return
    if not isinstance(steps, list):
        errors.append("transform.steps must be a list")
        return

    for i, step in enumerate(steps):
        if not isinstance(step, dict):
            errors.append(f"transform.steps[{i}] must be a mapping (got: {type(step).__name__})")
            continue

        if "select_columns" in step:
            sel = step["select_columns"]
            cols = sel.get("columns") if isinstance(sel, dict) else sel
            if not cols or not isinstance(cols, list) or len(cols) == 0:
                errors.append(f"transform.steps[{i}].select_columns.columns must be a non-empty list")

        if "drop_columns" in step:
            dc = step["drop_columns"]
            cols = dc.get("columns") if isinstance(dc, dict) else dc
            if not cols or not isinstance(cols, list):
                errors.append(f"transform.steps[{i}].drop_columns.columns must be a non-empty list")


def _validate_load(load: Optional[dict], errors: list[str]):
    if load is None:
        errors.append("pipeline.load section is required")
        return
    if not isinstance(load, dict):
        errors.append("pipeline.load must be a mapping")
        return

    destinations = load.get("destinations")
    if not destinations:
        errors.append("load.destinations is required (list of destination configs)")
        return
    if not isinstance(destinations, list):
        errors.append("load.destinations must be a list")
        return

    for i, dest in enumerate(destinations):
        if not isinstance(dest, dict):
            errors.append(f"load.destinations[{i}] must be a mapping")
            continue
        if not dest.get("engine") and not dest.get("type"):
            errors.append(f"load.destinations[{i}].engine is required (e.g. 'mysql', 'postgresql')")
        if not dest.get("database") and not dest.get("path"):
            errors.append(f"load.destinations[{i}].database or .path is required")
        if not dest.get("table"):
            errors.append(f"load.destinations[{i}].table is required")


def _validate_quality_checks(qc: Optional[dict], errors: list[str]):
    if qc is None:
        return
    if not isinstance(qc, dict):
        errors.append("pipeline.quality_checks must be a mapping")
        return

    pre_load = qc.get("pre_load")
    if pre_load is not None and not isinstance(pre_load, list):
        errors.append("quality_checks.pre_load must be a list of checks")


def _validate_dependencies(deps: Optional[dict], errors: list[str]):
    if deps is None:
        return
    if not isinstance(deps, dict):
        errors.append("pipeline.dependencies must be a mapping")
        return

    packages = deps.get("packages")
    if packages is not None:
        if not isinstance(packages, list):
            errors.append("dependencies.packages must be a list")
        else:
            required = {"pandas", "sqlalchemy"}
            found = {p.lower() for p in packages if isinstance(p, str)}
            missing = required - found
            if missing:
                errors.append(f"dependencies.packages should include: {', '.join(sorted(missing))}")
