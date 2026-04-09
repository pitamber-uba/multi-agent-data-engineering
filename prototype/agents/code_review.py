"""
Code Review Agent — Automated AI code review before PR creation.

Sits between Testing and PR Creation. Reviews the generated pipeline code
for correctness, security, spec compliance, and best practices.

When AI is enabled:
  The LLM reads the YAML spec AND the generated code, then produces a
  structured review. If it finds critical issues, it fixes them and
  returns RETRY (sending back to Testing for re-validation).

When AI is disabled:
  Runs a rule-based static check: validates that the pipeline file exists,
  has required methods, uses the correct database engine, respects row_limit,
  and includes logging.
"""

import re
import subprocess
from pathlib import Path

import yaml

from .base import BaseAgent
from orchestrator import HandoffEnvelope, WorkflowConfig

CODE_REVIEW_SYSTEM_PROMPT = """\
You are a senior data engineer performing a code review on an auto-generated ETL pipeline.

You have access to tools: read_file, write_file, edit_file, run_command, list_directory, search_code.

Your review MUST check ALL of the following against the YAML spec:

1. DATABASE CORRECTNESS
   - The pipeline uses the EXACT engine/driver from the spec (e.g. mysql+pymysql, NOT sqlite).
   - Connection URLs match the spec's host, port, database, username.
   - Passwords are read from os.environ using the spec's password_env_var fields.

2. EXTRACT CORRECTNESS
   - If the spec has row_limit, the SQL query MUST include LIMIT {row_limit}.
   - The source table name matches the spec exactly.

3. TRANSFORM CORRECTNESS
   - Every transformation step listed in the spec is implemented in the transform() method.
   - Column names match the spec exactly (case-sensitive).
   - No transformation steps are missing or invented.

4. VALIDATE CORRECTNESS
   - Every quality check from the spec is implemented in validate().

5. LOAD CORRECTNESS
   - Target database, table, and mode (append/replace) match the spec.
   - Chunksize matches if specified.

6. CODE QUALITY
   - Every method (extract, transform, validate, load, run) has logging via self.logger.
   - No hardcoded passwords or secrets.
   - No placeholder/TODO/pass statements.
   - Import statements are clean (no unused imports).

7. main.py CORRECTNESS
   - main.py exists, reads password env vars, builds correct connection URLs.
   - main.py imports and instantiates the correct pipeline class.

8. TEST CORRECTNESS
   - Tests exist and test real transformations with real DataFrames.
   - Tests don't mock transform/validate — they test with actual data.

WORKFLOW:
1. First, read the YAML spec file from config/.
2. Read the pipeline code, main.py, and test file.
3. Compare each point above against the spec.
4. If you find CRITICAL issues (wrong database, missing transforms, wrong table names):
   - Fix them using edit_file or write_file.
   - Run ruff and pytest to verify fixes.
   - End with a summary of what you fixed.
5. If you find only MINOR issues or the code is correct:
   - End with "REVIEW PASSED" and a brief summary.

Return a structured review in this format:
```
REVIEW STATUS: PASSED | FIXED | FAILED
ISSUES FOUND: <count>
ISSUES FIXED: <count>
SUMMARY: <one-line summary>
DETAILS:
- <finding 1>
- <finding 2>
```
"""


class CodeReviewAgent(BaseAgent):

    def execute(self, envelope: HandoffEnvelope, config: WorkflowConfig) -> HandoffEnvelope:
        repo = Path(config.repo_path)
        branch = envelope.branch

        self.logger.info(f"Code review starting for branch {branch}")

        try:
            self._checkout(repo, branch)

            spec = self._load_spec(config.pipeline_spec)
            pipeline_cfg = spec.get("pipeline", {})
            pipeline_name = pipeline_cfg.get("name", "etl_pipeline")

            if self.ai_enabled:
                self.logger.info("Running AI-powered code review")
                review_result = self._review_with_ai(config.pipeline_spec, pipeline_name)
                fixed = "FIXED" in review_result.upper()

                if fixed:
                    sha = self._commit_fixes(repo, branch, envelope.ticket_ref)
                    envelope.commit_sha = sha
                    envelope.metadata["code_review"] = review_result
                    envelope.metadata["code_review_status"] = "fixed"
                    return self._retry(envelope, "Code review found and fixed issues — re-running tests")

                envelope.metadata["code_review"] = review_result
                envelope.metadata["code_review_status"] = "passed"
                return self._success(envelope, code_review_passed=True)

            else:
                self.logger.info("Running rule-based code review")
                issues = self._static_review(repo, pipeline_name, pipeline_cfg)

                envelope.metadata["code_review_issues"] = issues
                if issues:
                    envelope.metadata["code_review_status"] = "issues_found"
                    self.logger.warning(f"Code review found {len(issues)} issue(s)")
                    for issue in issues:
                        self.logger.warning(f"  - {issue}")
                    return self._success(envelope, code_review_passed=True, review_warnings=issues)
                else:
                    envelope.metadata["code_review_status"] = "passed"
                    self.logger.info("Code review passed — no issues")
                    return self._success(envelope, code_review_passed=True)

        except Exception as e:
            self.logger.error(f"Code review failed: {e}")
            return self._failure(envelope, f"Code review error: {e}")

    def _review_with_ai(self, spec_path: str, pipeline_name: str) -> str:
        spec_file = Path(spec_path).name
        user_prompt = f"""\
Perform a thorough code review of the generated ETL pipeline.

1. Read the YAML spec: config/{spec_file}
2. Read the pipeline: pipelines/{pipeline_name}.py
3. Read the entry point: main.py
4. Read the tests: tests/test_{pipeline_name}.py
5. Compare every detail against the spec.
6. Fix critical issues. Report minor issues.
7. If you fix anything, run `ruff check pipelines/ tests/ main.py` and `pytest tests/ -v`.

Spec file: config/{spec_file}
Pipeline: pipelines/{pipeline_name}.py
Tests: tests/test_{pipeline_name}.py
"""
        return self.ai.run_agent(CODE_REVIEW_SYSTEM_PROMPT, user_prompt)

    def _static_review(self, repo: Path, pipeline_name: str, pipeline_cfg: dict) -> list[str]:
        """Rule-based review when AI is not available."""
        issues = []

        pipeline_file = repo / "pipelines" / f"{pipeline_name}.py"
        test_file = repo / "tests" / f"test_{pipeline_name}.py"
        main_file = repo / "main.py"

        if not pipeline_file.exists():
            issues.append(f"Pipeline file missing: pipelines/{pipeline_name}.py")
            return issues

        code = pipeline_file.read_text()

        required_methods = ["def extract", "def transform", "def validate", "def load", "def run"]
        for method in required_methods:
            if method not in code:
                issues.append(f"Missing method: {method}()")

        if "self.logger" not in code and "logging" not in code:
            issues.append("No logging found — every method should use self.logger")

        extract = pipeline_cfg.get("extract", {})
        source = extract.get("source", "").lower()
        if source == "mysql":
            if "sqlite" in code.lower() and "memory" not in code.lower():
                issues.append("Pipeline uses SQLite but spec requires MySQL")
            if "mysql+pymysql" not in code and "mysql" not in code.lower():
                issues.append("Pipeline doesn't appear to use mysql+pymysql connection")

        row_limit = extract.get("row_limit")
        if row_limit:
            if f"LIMIT" not in code.upper():
                issues.append(f"Spec has row_limit: {row_limit} but no LIMIT found in extract query")

        load_cfg = pipeline_cfg.get("load", {})
        destinations = load_cfg.get("destinations", [])
        if destinations:
            target_table = destinations[0].get("table", "")
            if target_table and target_table not in code:
                issues.append(f"Target table '{target_table}' not found in pipeline code")

        transform = pipeline_cfg.get("transform", {})
        steps = transform.get("steps", []) if isinstance(transform, dict) else transform
        if isinstance(steps, list):
            for step in steps:
                if isinstance(step, dict):
                    if "drop_columns" in step:
                        cols = step["drop_columns"].get("columns", [])
                        for col in cols:
                            if col not in code:
                                issues.append(f"drop_columns: '{col}' not found in pipeline code")

                    if "select_columns" in step:
                        sel = step["select_columns"]
                        cols = sel.get("columns", []) if isinstance(sel, dict) else []
                        missing_cols = [c for c in cols if c not in code]
                        if len(missing_cols) > 3:
                            issues.append(f"select_columns: {len(missing_cols)} columns not found in code")

        if not test_file.exists():
            issues.append(f"Test file missing: tests/test_{pipeline_name}.py")

        if not main_file.exists():
            issues.append("main.py entry point missing")
        else:
            main_code = main_file.read_text()
            for env_var_key in ["password_env_var"]:
                for section_key in ["extract", "load"]:
                    section = pipeline_cfg.get(section_key, {})
                    if isinstance(section, dict) and "destinations" in section:
                        for dest in section["destinations"]:
                            var = dest.get("password_env_var", "")
                            if var and var not in main_code:
                                issues.append(f"main.py doesn't reference env var: {var}")
                    elif isinstance(section, dict):
                        var = section.get("password_env_var", "")
                        if var and var not in main_code:
                            issues.append(f"main.py doesn't reference env var: {var}")

        return issues

    def _checkout(self, repo: Path, branch: str):
        subprocess.run(
            ["git", "checkout", branch],
            cwd=repo, capture_output=True, text=True, check=True,
        )

    def _load_spec(self, spec_path: str) -> dict:
        with open(spec_path) as f:
            return yaml.safe_load(f)

    def _commit_fixes(self, repo: Path, branch: str, ticket: str) -> str:
        subprocess.run(["git", "add", "-A"], cwd=repo, check=True, capture_output=True)
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo, capture_output=True, text=True, check=True,
        )
        if status.stdout.strip():
            subprocess.run(
                ["git", "commit", "-m", f"fix({ticket}): code review fixes"],
                cwd=repo, capture_output=True, text=True, check=True,
            )
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo, capture_output=True, text=True, check=True,
        )
        sha = result.stdout.strip()
        subprocess.run(
            ["git", "push", "origin", branch],
            cwd=repo, capture_output=True, text=True,
        )
        return sha
