"""
Development Agent — Generates pipeline code from specifications.

When AI is enabled, this agent sends the pipeline spec to Claude/GPT and
lets the LLM use tools (read files, write files, run commands) to generate
code — exactly like Cursor AI Agent Mode.

When AI is disabled, falls back to template-based generation.
"""

import subprocess
from pathlib import Path

import yaml

from .base import BaseAgent
from orchestrator import HandoffEnvelope, WorkflowConfig

GENERATE_SYSTEM_PROMPT = """\
You are a senior data engineer working inside a Git repository.
Your job is to generate production-quality ETL pipeline code based on a YAML specification.

You have access to tools to read files, write files, edit files, list directories, search code, and run shell commands.

Rules:
- First, use list_directory and read_file to understand the existing repo structure and coding patterns.
- Generate a Python pipeline class in pipelines/<name>.py using pandas and sqlalchemy.
- Generate pytest tests in tests/test_<name>.py with real assertions.
- The pipeline class must have methods: extract, transform, validate, load, and a run() orchestrator method.
- Tests must be runnable with `pytest tests/` and must import from `pipelines.<name>`.
- After writing files, run `ruff check pipelines/ tests/` to verify no lint errors. Fix any issues.
- After ruff passes, run `pytest tests/ -v` to verify tests pass. Fix any failures.
- Do NOT use any test mocking for the transform and validate methods — test them with real DataFrames.
- Write clean, production-quality code. No placeholder comments like "TODO" or "pass".
- Do not explain your work. Just use the tools to create the files and verify they work.
"""

UPDATE_SYSTEM_PROMPT = """\
You are a senior data engineer working inside a Git repository.
Your job is to UPDATE existing ETL pipeline code to match an updated YAML specification.

You have access to tools to read files, write files, edit files, list directories, search code, and run shell commands.

CRITICAL: The pipeline and tests already exist. Do NOT rewrite them from scratch.
Instead, make the MINIMAL changes needed to align the code with the updated spec.

Rules:
- FIRST, read the existing pipeline code and test code with read_file.
- THEN, compare the existing code against the new spec to identify what needs to change.
- Use the edit_file tool to make surgical changes to specific methods or sections.
  Only use write_file if you need to create a new file or if the changes are so extensive
  that a full rewrite is genuinely simpler.
- Preserve all existing code structure, imports, variable names, and manual refinements
  that are not affected by the spec change.
- After edits, run `ruff check pipelines/ tests/` to verify no lint errors. Fix any issues.
- After ruff passes, run `pytest tests/ -v` to verify tests pass. Fix any failures.
- Do not explain your work. Just use the tools to read, edit, and verify.
"""


class DevelopmentAgent(BaseAgent):

    def execute(self, envelope: HandoffEnvelope, config: WorkflowConfig) -> HandoffEnvelope:
        repo = Path(config.repo_path)
        branch = envelope.branch

        self.logger.info(f"Starting development for {envelope.ticket_ref} on branch {branch}")

        try:
            self._create_branch(repo, branch, config.base_branch)
            spec = self._load_spec(config.pipeline_spec)
            spec_text = yaml.dump(spec, default_flow_style=False)
            pipeline_name = spec.get("pipeline", {}).get("name", "etl_pipeline")

            pipeline_file = repo / "pipelines" / f"{pipeline_name}.py"
            test_file = repo / "tests" / f"test_{pipeline_name}.py"
            existing_code = pipeline_file.exists()

            if self.ai_enabled:
                if existing_code:
                    self.logger.info("Existing pipeline found — using AI for incremental update")
                else:
                    self.logger.info("No existing pipeline — using AI for full generation")
                self._generate_with_ai(spec_text, pipeline_name, incremental=existing_code)
            else:
                self.logger.info("Using template-based generation (no AI configured)")
                self._generate_pipeline_template(repo, spec)
                self._generate_tests_template(repo, spec)

            files_to_commit = []
            for f in [pipeline_file, test_file]:
                if f.exists():
                    files_to_commit.append(f)

            commit_verb = "update" if existing_code else "generate"
            sha = self._commit_and_push(
                repo, branch, envelope.ticket_ref, files_to_commit, verb=commit_verb,
            )

            envelope.commit_sha = sha
            return self._success(
                envelope,
                files_created=[str(f.relative_to(repo)) for f in files_to_commit],
                pipeline_name=pipeline_name,
                ai_generated=self.ai_enabled,
                incremental=existing_code,
            )

        except Exception as e:
            return self._failure(envelope, str(e))

    def _generate_with_ai(self, spec_text: str, pipeline_name: str, incremental: bool = False):
        """Let the AI agent generate or update code using tools."""
        if incremental:
            system = UPDATE_SYSTEM_PROMPT
            user_prompt = f"""\
The pipeline specification has been updated. Update the existing pipeline code and tests \
to match the new spec. Only change what is necessary — do NOT rewrite files from scratch.

1. First, read the existing code:
   - `pipelines/{pipeline_name}.py`
   - `tests/test_{pipeline_name}.py`
2. Then review the updated spec below.
3. Use edit_file to make targeted changes to align the code with the new spec.
4. Run ruff and pytest to verify everything still works.

Updated pipeline specification:
```yaml
{spec_text}
```

Pipeline file: pipelines/{pipeline_name}.py
Test file: tests/test_{pipeline_name}.py
"""
        else:
            system = GENERATE_SYSTEM_PROMPT
            user_prompt = f"""\
Generate an ETL pipeline from this specification. Write the pipeline code and tests, \
then verify they pass linting and tests.

Pipeline specification:
```yaml
{spec_text}
```

Write the pipeline to: pipelines/{pipeline_name}.py
Write tests to: tests/test_{pipeline_name}.py
"""
        self.ai.run_agent(system, user_prompt)

    # --- Template fallback (used when AI is not configured) ---

    def _generate_pipeline_template(self, repo: Path, spec: dict) -> Path:
        pipeline_cfg = spec.get("pipeline", {})
        name = pipeline_cfg.get("name", "etl_pipeline")
        extract = pipeline_cfg.get("extract", {})
        load = pipeline_cfg.get("load", {})
        quality = pipeline_cfg.get("quality_checks", {})

        raw_transforms = pipeline_cfg.get("transform", [])
        if isinstance(raw_transforms, dict):
            raw_transforms = raw_transforms.get("steps", [])

        raw_quality = quality
        if isinstance(raw_quality, dict):
            raw_quality = raw_quality.get("pre_load", raw_quality.get("checks", []))
        if not isinstance(raw_quality, list):
            raw_quality = []

        if isinstance(load, dict) and "destinations" in load:
            load_entry = load["destinations"][0] if load["destinations"] else {}
        else:
            load_entry = load

        transform_lines = self._build_transform_chain(raw_transforms)
        quality_lines = self._build_quality_checks(raw_quality)

        indent = "        "
        transform_body = ("\n".join(f"{indent}{line}" for line in transform_lines)
                          if transform_lines else f"{indent}pass")
        quality_body = ("\n".join(f"{indent}{line}" for line in quality_lines)
                        if quality_lines else f"{indent}pass")

        extract_query = extract.get("query", "SELECT 1").strip()
        table_full = load_entry.get("table", "public.output")
        table_name = table_full.split(".")[-1]
        table_schema = load_entry.get("schema", table_full.split(".")[0] if "." in table_full else "public")
        load_mode = "append" if load_entry.get("mode") == "upsert" else load_entry.get("mode", "append")

        code = f'''\
"""
Auto-generated ETL Pipeline: {name}
Source: {extract.get("source", "unknown")}
Destination: {load.get("destination", "unknown")}.{table_full}
"""

import logging
from datetime import date

import pandas as pd
import sqlalchemy as sa

logger = logging.getLogger("{name}")


class {self._to_class_name(name)}:

    def __init__(self, source_engine: sa.Engine, dest_engine: sa.Engine):
        self.source_engine = source_engine
        self.dest_engine = dest_engine

    def run(self, execution_date: date) -> dict:
        logger.info(f"Running {name} for {{execution_date}}")

        df = self.extract(execution_date)
        logger.info(f"Extracted {{len(df)}} rows")

        df = self.transform(df)
        logger.info(f"Transformed to {{len(df)}} rows")

        self.validate(df)
        logger.info("Quality checks passed")

        rows_loaded = self.load(df)
        logger.info(f"Loaded {{rows_loaded}} rows")

        return {{"rows_extracted": len(df), "rows_loaded": rows_loaded}}

    def extract(self, execution_date: date) -> pd.DataFrame:
        query = """
{extract_query}
        """.strip().replace("{{{{execution_date}}}}", str(execution_date))
        return pd.read_sql(query, self.source_engine)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
{transform_body}
        return df

    def validate(self, df: pd.DataFrame):
{quality_body}

    def load(self, df: pd.DataFrame) -> int:
        df.to_sql(
            name="{table_name}",
            schema="{table_schema}",
            con=self.dest_engine,
            if_exists="{load_mode}",
            index=False,
        )
        return len(df)
'''

        pipeline_dir = repo / "pipelines"
        pipeline_dir.mkdir(exist_ok=True)
        out = pipeline_dir / f"{name}.py"
        out.write_text(code)
        self.logger.info(f"Generated pipeline: {out}")
        return out

    def _generate_tests_template(self, repo: Path, spec: dict) -> Path:
        pipeline_cfg = spec.get("pipeline", {})
        name = pipeline_cfg.get("name", "etl_pipeline")
        class_name = self._to_class_name(name)

        code = f'''\
"""Tests for {name} pipeline."""

import pandas as pd
import pytest
from unittest.mock import MagicMock

from pipelines.{name} import {class_name}


@pytest.fixture
def pipeline():
    source = MagicMock()
    dest = MagicMock()
    return {class_name}(source_engine=source, dest_engine=dest)


@pytest.fixture
def sample_data():
    return pd.DataFrame({{
        "id": [1, 2, 3],
        "value": [10.0, 20.0, 30.0],
        "name": ["a", "b", "c"],
    }})


class TestTransform:

    def test_returns_dataframe(self, pipeline, sample_data):
        result = pipeline.transform(sample_data)
        assert isinstance(result, pd.DataFrame)

    def test_preserves_rows_on_clean_data(self, pipeline, sample_data):
        result = pipeline.transform(sample_data)
        assert len(result) > 0

    def test_output_has_expected_columns(self, pipeline, sample_data):
        result = pipeline.transform(sample_data)
        for col in sample_data.columns:
            assert col in result.columns


class TestValidate:

    def test_passes_on_valid_data(self, pipeline, sample_data):
        pipeline.validate(sample_data)

    def test_fails_on_empty_data(self, pipeline):
        df = pd.DataFrame()
        with pytest.raises(ValueError):
            pipeline.validate(df)
'''

        test_dir = repo / "tests"
        test_dir.mkdir(exist_ok=True)
        out = test_dir / f"test_{name}.py"
        out.write_text(code)
        self.logger.info(f"Generated tests: {out}")
        return out

    def _build_transform_chain(self, transforms: list) -> list[str]:
        lines = []
        for t in transforms:
            if isinstance(t, dict):
                if "deduplicate" in t:
                    cols = t["deduplicate"].get("columns", [])
                    lines.append(f"df = df.drop_duplicates(subset={cols})")
                elif "filter" in t:
                    expr = t["filter"]
                    lines.append(f'df = df.query("{expr}")')
                elif "cast" in t:
                    for col, dtype in t["cast"].items():
                        if "date" in str(dtype):
                            lines.append(f'df["{col}"] = pd.to_datetime(df["{col}"]).dt.date')
                        elif "decimal" in str(dtype):
                            lines.append(f'df["{col}"] = df["{col}"].astype(float).round(2)')
            elif isinstance(t, str):
                if t.startswith("filter:"):
                    expr = t.split(":", 1)[1].strip().strip('"')
                    lines.append(f'df = df.query("{expr}")')
        return lines

    def _build_quality_checks(self, checks: list) -> list[str]:
        lines = [
            "if df.empty:",
            '    raise ValueError("DataFrame is empty — no data to load")',
        ]
        for check in checks:
            if isinstance(check, dict):
                if "row_count_gt" in check:
                    threshold = check["row_count_gt"]
                    lines.append(f"if len(df) <= {threshold}:")
                    lines.append(f'    raise ValueError(f"Row count {{len(df)}} not > {threshold}")')
                elif "column_not_null" in check:
                    cols = check["column_not_null"]
                    for col in cols:
                        lines.append(f'if df["{col}"].isnull().any():')
                        lines.append(f'    raise ValueError("Null values found in {col}")')
                elif "required_fields_not_null" in check:
                    fields = check["required_fields_not_null"]
                    if isinstance(fields, dict):
                        fields = fields.get("fields", [])
                    for col in fields:
                        lines.append(f'if "{col}" in df.columns and df["{col}"].isnull().any():')
                        lines.append(f'    raise ValueError("Null values found in {col}")')
        return lines

    # --- Git operations ---

    def _create_branch(self, repo: Path, branch: str, base: str):
        try:
            self._git(repo, "checkout", base)
        except subprocess.CalledProcessError:
            self.logger.warning("Normal checkout failed — forcing checkout to bypass parent-repo changes")
            self._git(repo, "checkout", "--force", base)
        try:
            self._git(repo, "pull", "origin", base)
        except subprocess.CalledProcessError:
            self.logger.warning("No remote origin — skipping pull (local-only repo)")
        try:
            self._git(repo, "checkout", "-b", branch)
        except subprocess.CalledProcessError:
            try:
                self._git(repo, "checkout", branch)
            except subprocess.CalledProcessError:
                self._git(repo, "checkout", "--force", branch)
        self.logger.info(f"On branch: {branch}")

    def _load_spec(self, spec_path: str) -> dict:
        with open(spec_path) as f:
            return yaml.safe_load(f)

    def _commit_and_push(self, repo: Path, branch: str, ticket: str, files: list[Path], verb: str = "generate") -> str:
        for f in files:
            self._git(repo, "add", str(f.relative_to(repo)))

        staged = self._git(repo, "diff", "--cached", "--name-only")
        if staged:
            self._git(repo, "commit", "-m", f"feat({ticket}): {verb} pipeline and tests")
        else:
            self.logger.info("No changes to commit (files unchanged)")

        sha = self._git(repo, "rev-parse", "HEAD")
        try:
            self._git(repo, "push", "-u", "origin", branch)
        except subprocess.CalledProcessError:
            self.logger.warning("Push failed (no remote configured — skipping for local prototype)")
        return sha

    @staticmethod
    def _to_class_name(snake: str) -> str:
        return "".join(word.capitalize() for word in snake.split("_"))

    @staticmethod
    def _git(repo: Path, *args) -> str:
        result = subprocess.run(
            ["git", *args],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
