"""
Testing Agent — Runs automated quality gates on pipeline code.

When AI is enabled, the agent uses Claude to analyze test failures, understand
the root cause, and generate fixes — like asking Cursor to "fix this test failure".

When AI is disabled, relies on ruff --fix for lint and reports failures without fixes.
"""

import os
import subprocess
from pathlib import Path

from .base import BaseAgent
from orchestrator import HandoffEnvelope, WorkflowConfig

FIX_PROMPT_SYSTEM = """\
You are a senior data engineer debugging test and lint failures in a Git repository.
You have access to tools to read files, write files, list directories, search code, and run commands.

Rules:
- Read the failing file(s) and understand the root cause.
- Fix the actual bug in the pipeline or test code — don't just silence the error.
- After fixing, run the failing command again to verify the fix works.
- If linting fails, fix the lint issues in the source files.
- If tests fail, read the test AND the pipeline code, understand why, and fix it.
- Do NOT explain. Just use tools to read, fix, and verify.
"""


class TestingAgent(BaseAgent):

    def execute(self, envelope: HandoffEnvelope, config: WorkflowConfig) -> HandoffEnvelope:
        repo = Path(config.repo_path)
        branch = envelope.branch

        self.logger.info(f"Testing branch {branch} at {envelope.commit_sha[:8]}")

        try:
            self._checkout(repo, branch)
        except Exception as e:
            return self._failure(envelope, f"Checkout failed: {e}")

        lint_ok, lint_output = self._run_linting(repo)
        test_ok, test_output, coverage = self._run_tests(repo)

        envelope.metadata["lint_passed"] = lint_ok
        envelope.metadata["lint_output"] = lint_output
        envelope.metadata["tests_passed"] = test_ok
        envelope.metadata["test_output"] = test_output
        envelope.metadata["coverage"] = coverage

        if lint_ok and test_ok:
            return self._success(envelope, all_checks_passed=True)

        if self.ai_enabled:
            self.logger.info("Using AI to analyze and fix failures")
            fixed = self._fix_with_ai(lint_ok, lint_output, test_ok, test_output)
            if fixed:
                sha = self._commit_fixes(repo, branch, envelope.ticket_ref)
                envelope.commit_sha = sha
                return self._retry(envelope, "AI analyzed failures and applied fixes")

        if not lint_ok:
            auto_fixed = self._attempt_autofix(repo)
            if auto_fixed:
                sha = self._commit_fixes(repo, branch, envelope.ticket_ref)
                envelope.commit_sha = sha
                return self._retry(envelope, "Lint issues auto-fixed, re-running tests")

        failures = []
        if not lint_ok:
            failures.append("linting")
        if not test_ok:
            failures.append("unit tests")

        return self._failure(envelope, f"Failed checks: {', '.join(failures)}")

    def _fix_with_ai(self, lint_ok: bool, lint_output: str, test_ok: bool, test_output: str) -> bool:
        """Let AI analyze failures and apply fixes — like Cursor debugging."""
        problems = []
        if not lint_ok:
            problems.append(f"LINT FAILURES:\n{lint_output[:3000]}")
        if not test_ok:
            problems.append(f"TEST FAILURES:\n{test_output[:3000]}")

        user_prompt = f"""\
The following checks failed. Read the relevant source files, understand the root cause, \
fix the issues, and verify the fixes pass.

{chr(10).join(problems)}

After fixing, run:
1. `ruff check pipelines/ tests/ --output-format=concise` to verify lint passes
2. `pytest tests/ -v --tb=short` to verify tests pass
"""
        try:
            self.ai.run_agent(FIX_PROMPT_SYSTEM, user_prompt)
            return True
        except Exception as e:
            self.logger.warning(f"AI fix attempt failed: {e}")
            return False

    def _checkout(self, repo: Path, branch: str):
        subprocess.run(
            ["git", "checkout", branch],
            cwd=repo, capture_output=True, text=True, check=True,
        )

    def _run_linting(self, repo: Path) -> tuple[bool, str]:
        self.logger.info("Running ruff linter...")
        result = subprocess.run(
            ["ruff", "check", "pipelines/", "tests/", "--output-format=concise"],
            cwd=repo, capture_output=True, text=True,
        )
        passed = result.returncode == 0
        output = result.stdout + result.stderr
        self.logger.info(f"Linting: {'PASS' if passed else 'FAIL'}")
        return passed, output

    def _test_env(self, repo: Path) -> dict:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(repo) + os.pathsep + env.get("PYTHONPATH", "")
        return env

    def _run_tests(self, repo: Path) -> tuple[bool, str, str]:
        self.logger.info("Running pytest...")
        env = self._test_env(repo)

        test_result = subprocess.run(
            ["pytest", "tests/", "-v", "--tb=short"],
            cwd=repo, capture_output=True, text=True, env=env,
        )

        passed = test_result.returncode == 0
        output = test_result.stdout + test_result.stderr

        coverage = "N/A"
        cov_result = subprocess.run(
            ["pytest", "tests/", "--cov=pipelines", "--cov-report=term-missing", "-q"],
            cwd=repo, capture_output=True, text=True, env=env,
        )
        if cov_result.returncode == 0:
            for line in cov_result.stdout.splitlines():
                if "TOTAL" in line:
                    coverage = line.split()[-1]

        self.logger.info(f"Tests: {'PASS' if passed else 'FAIL'} | Coverage: {coverage}")
        return passed, output, coverage

    def _attempt_autofix(self, repo: Path) -> bool:
        self.logger.info("Attempting auto-fix with ruff...")
        result = subprocess.run(
            ["ruff", "check", "pipelines/", "tests/", "--fix"],
            cwd=repo, capture_output=True, text=True,
        )
        return result.returncode == 0

    def _commit_fixes(self, repo: Path, branch: str, ticket: str) -> str:
        subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo, capture_output=True, text=True, check=True,
        )
        if status.stdout.strip():
            subprocess.run(
                ["git", "commit", "-m", f"fix({ticket}): auto-fix lint issues"],
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
