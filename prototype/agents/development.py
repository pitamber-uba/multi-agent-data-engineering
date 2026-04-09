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
from spec_validator import validate_spec, SpecValidationError

GENERATE_SYSTEM_PROMPT = """\
You are a senior data engineer working inside a Git repository.
Your job: generate production-quality ETL pipeline code from a YAML specification.

Available tools: read_file, write_file, edit_file, run_command, list_directory, search_code.

═══════════════════════════════════════════════════════════════
STEP-BY-STEP WORKFLOW (follow this order exactly)
═══════════════════════════════════════════════════════════════

STEP 1 — EXPLORE THE REPO
  • list_directory(".") to see what already exists.
  • read_file("config/<spec_file>") to load the YAML spec.

STEP 2 — UNDERSTAND THE SPEC
  Parse these sections from the YAML and note every field:
  • pipeline.name          → class name + file name
  • pipeline.extract       → source DB, table, row_limit, connection details
  • pipeline.transform     → ordered list of transformation steps
  • pipeline.quality_checks → validation rules
  • pipeline.load          → target DB, table, mode, chunksize, DDL

STEP 3 — GENERATE pipelines/<name>.py
  Class with __init__, extract, transform, validate, load, run methods.

STEP 4 — GENERATE main.py
  Entry point that builds connection URLs and calls pipeline.run().

STEP 5 — GENERATE tests/test_<name>.py
  Pytest tests with real DataFrames (no mocking of transform/validate).

STEP 6 — VERIFY
  • run_command("ruff check pipelines/ tests/ main.py")  → fix any lint errors
  • run_command("pytest tests/ -v --tb=short")            → fix any test failures

═══════════════════════════════════════════════════════════════
HARD RULES (violating any of these is a critical error)
═══════════════════════════════════════════════════════════════

DATABASE CONNECTION:
  • Read the spec's extract section for: source, driver, host, port, username, database.
  • Build SQLAlchemy URL exactly as: {source}+{driver}://{username}:{password}@{host}:{port}/{database}
    Example: mysql+pymysql://root:pass@localhost:3306/MyFonts_Legacy
  • NEVER use sqlite:// unless the spec explicitly says source: sqlite.
  • If spec has password_env_var (e.g. SOURCE_DB_PASSWORD), the password MUST come from
    os.environ["{password_env_var}"] — NEVER hardcode a password.

ROW LIMIT:
  • If extract.row_limit exists (e.g. row_limit: 1000), the SQL query MUST be:
    SELECT * FROM {table} LIMIT {row_limit}
  • If row_limit is absent, use: SELECT * FROM {table} (no LIMIT).
  • NEVER ignore row_limit.

TRANSFORMATIONS — implement EVERY step in transform.steps in order:
  • drop_columns    → df.drop(columns=[...], errors='ignore')
  • derive_column / derive_domain → apply expression from spec to create new column
  • conditional_column → if/elif/else logic or np.select based on spec conditions
  • bucket_column   → pd.cut or manual if/elif mapping
  • clean_strings   → .str.strip().str.lower() on listed columns
  • parse_dates     → pd.to_datetime with format and errors from spec
  • cast_types      → .astype() for each mapping
  • fill_nulls      → .fillna() with spec's default values
  • deduplicate     → .sort_values(sort_by).drop_duplicates(subset, keep)
  • filter_rows     → .query(condition) or boolean mask
  • rename_columns  → .rename(columns={...})
  • sort_rows       → .sort_values(by, ascending)
  • select_columns  → df = df[columns_list] — add missing columns as None first
  If a step type is not in this list, implement it based on the step's description field.

QUALITY CHECKS — implement EVERY check from quality_checks.pre_load:
  • row_count_gt: N            → raise ValueError if len(df) <= N
  • required_fields_not_null   → raise ValueError if any listed field has nulls
  • column_not_null            → same as above
  • unique_check               → raise ValueError if duplicates found
  • value_range                → raise ValueError if values outside [min, max]
  • allowed_values             → raise ValueError if unexpected values found

LOAD:
  • Use the EXACT target engine/driver/host/port/database/table from load.destinations[0].
  • Use if_exists="{mode}" from the spec (append or replace).
  • Use chunksize from the spec if present.

LOGGING (mandatory in every method):
  • Constructor: self.logger = logging.getLogger(__name__)
  • extract():   log source table, row_limit, and row count after extraction
  • transform(): log EACH transformation step name and row count before/after
  • validate():  log each check with pass/fail and counts
  • load():      log target table, row count loaded, success/failure
  • run():       log start time, end time, total duration, overall status
  • Levels: INFO for normal, WARNING for non-fatal, ERROR for failures

main.py REQUIREMENTS:
  • Import the pipeline class from pipelines.<name>.
  • Read password env vars using os.environ (from spec's password_env_var fields).
  • Build source_url and target_url from spec's connection details + password.
  • Instantiate pipeline with those URLs and call run().
  • Use dotenv or os.environ — NEVER hardcode passwords.

TEST REQUIREMENTS:
  • File: tests/test_<name>.py
  • Import from pipelines.<name>
  • Create real pd.DataFrame fixtures with sample data matching the spec's columns.
  • Test transform() with real data — verify column drops, derived columns, renames.
  • Test validate() passes on valid data, raises ValueError on invalid data.
  • Do NOT mock transform or validate. Only mock database engines.
  • Tests must pass with: pytest tests/ -v

═══════════════════════════════════════════════════════════════
OUTPUT — do not explain your work. Just use tools to create files and verify.
═══════════════════════════════════════════════════════════════
"""

UPDATE_SYSTEM_PROMPT = """\
You are a senior data engineer working inside a Git repository.
Your job: UPDATE existing ETL pipeline code to match an updated YAML specification.

Available tools: read_file, write_file, edit_file, run_command, list_directory, search_code.

═══════════════════════════════════════════════════════════════
CRITICAL: The pipeline and tests already exist. Do NOT rewrite from scratch.
Make the MINIMAL changes needed to align the code with the updated spec.
═══════════════════════════════════════════════════════════════

STEP-BY-STEP WORKFLOW:

STEP 1 — READ EXISTING CODE
  • read_file("pipelines/<name>.py")
  • read_file("tests/test_<name>.py")
  • read_file("main.py") if it exists
  • read_file("config/<spec_file>") for the updated spec

STEP 2 — DIFF AGAINST THE NEW SPEC
  Compare the existing code against the updated YAML spec:
  • Are the database connection details still correct?
  • Has the row_limit changed?
  • Were transform steps added, removed, or reordered?
  • Did quality checks change?
  • Did the target table or load mode change?

STEP 3 — APPLY SURGICAL EDITS
  Use edit_file to change only the affected sections. Preserve:
  • Existing code structure, imports, variable names
  • Manual refinements not affected by the spec change
  • All existing logging statements
  Only use write_file if the changes are truly extensive.

STEP 4 — VERIFY
  • run_command("ruff check pipelines/ tests/ main.py") → fix lint errors
  • run_command("pytest tests/ -v --tb=short")           → fix test failures

HARD RULES (same as generation — apply to edits):
  • Database URLs: {source}+{driver}://{username}:{password}@{host}:{port}/{database}
  • NEVER use sqlite unless spec says source: sqlite.
  • If row_limit exists, SQL MUST have LIMIT {row_limit}.
  • Every method must have self.logger calls (add if missing, preserve if present).
  • Passwords from os.environ["{password_env_var}"] — never hardcoded.
  • Every transform step in the spec must be implemented in order.

Do not explain your work. Just use tools to read, edit, and verify.
"""


class DevelopmentAgent(BaseAgent):

    def execute(self, envelope: HandoffEnvelope, config: WorkflowConfig) -> HandoffEnvelope:
        repo = Path(config.repo_path)
        branch = envelope.branch

        self.logger.info(f"Starting development for {envelope.ticket_ref} on branch {branch}")

        try:
            self.logger.info("Validating pipeline spec...")
            try:
                validate_spec(config.pipeline_spec)
                self.logger.info("Spec validation passed")
            except SpecValidationError as e:
                self.logger.error(f"Spec validation failed: {e}")
                return self._failure(envelope, str(e))

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

            main_file = repo / "main.py"
            config_file = Path(config.pipeline_spec)

            files_to_commit = []
            for f in [pipeline_file, test_file, main_file, config_file]:
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
The pipeline YAML spec has been updated. Update the existing code to match.

FILES TO READ FIRST:
  • pipelines/{pipeline_name}.py   (existing pipeline)
  • tests/test_{pipeline_name}.py  (existing tests)
  • main.py                        (existing entry point, if any)

UPDATED SPEC (this is the source of truth — code must match this exactly):
```yaml
{spec_text}
```

WHAT TO DO:
  1. Read all three files above.
  2. Compare each method against the updated spec.
  3. Use edit_file to fix any mismatches (database details, transforms, row_limit, etc).
  4. Run: ruff check pipelines/ tests/ main.py
  5. Run: pytest tests/ -v --tb=short
  6. Fix any failures and re-run until both pass.
"""
        else:
            system = GENERATE_SYSTEM_PROMPT
            user_prompt = f"""\
Generate a complete ETL pipeline from this YAML specification.

SPEC (this is the source of truth — read every field carefully):
```yaml
{spec_text}
```

FILES TO CREATE:
  1. pipelines/{pipeline_name}.py  — Pipeline class with extract/transform/validate/load/run
  2. main.py                       — Entry point that builds DB URLs from env vars and calls run()
  3. tests/test_{pipeline_name}.py — Pytest tests with real DataFrame assertions

CRITICAL REMINDERS:
  • Database: use EXACT engine+driver from spec (e.g. mysql+pymysql). NEVER substitute sqlite.
  • Row limit: if extract.row_limit is set, SQL MUST include LIMIT {{row_limit}}.
  • Transforms: implement EVERY step from transform.steps in order. Skip nothing.
  • Passwords: read from os.environ["{{password_env_var}}"] — never hardcode.
  • Logging: every method must use self.logger with INFO/WARNING/ERROR levels.

After writing all files:
  1. Run: ruff check pipelines/ tests/ main.py
  2. Run: pytest tests/ -v --tb=short
  3. Fix any failures and re-run until both pass.
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
