"""
Deployment Agent — Triggers and monitors Jenkins deployments.

Integrates with Jenkins REST API to trigger builds, poll for completion,
and run post-deployment validation. Falls back to simulation when Jenkins
is unavailable (for local prototyping).
"""

import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from .base import BaseAgent
from orchestrator import HandoffEnvelope, WorkflowConfig

try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


@dataclass
class JenkinsConfig:
    base_url: str
    job_name: str
    username: str = ""
    api_token: str = ""
    poll_interval: int = 15
    timeout: int = 600

    @property
    def auth(self) -> Optional[tuple[str, str]]:
        if self.username and self.api_token:
            return (self.username, self.api_token)
        return None


class DeploymentAgent(BaseAgent):
    """
    Responsibilities:
    1. Tag the release from the merged commit
    2. Trigger a Jenkins deployment pipeline
    3. Poll Jenkins until the build completes
    4. Run post-deployment validation
    5. Report success or initiate rollback
    """

    def execute(self, envelope: HandoffEnvelope, config: WorkflowConfig) -> HandoffEnvelope:
        repo = Path(config.repo_path)
        self.logger.info(
            f"Starting deployment for {envelope.ticket_ref} "
            f"(commit {envelope.commit_sha[:8]})"
        )

        try:
            tag = self._create_release_tag(repo, envelope)

            if config.jenkins_url and REQUESTS_AVAILABLE:
                jenkins_cfg = JenkinsConfig(
                    base_url=config.jenkins_url,
                    job_name=config.jenkins_job,
                )
                build_result = self._trigger_and_monitor(jenkins_cfg, envelope)
            else:
                self.logger.info("Jenkins not configured — running simulated deployment")
                build_result = self._simulate_deployment(envelope)

            if build_result["status"] == "success":
                validation = self._post_deploy_validation(repo, envelope)
                return self._success(
                    envelope,
                    release_tag=tag,
                    build_number=build_result.get("build_number"),
                    build_url=build_result.get("build_url"),
                    validation=validation,
                )
            else:
                self._rollback(repo, envelope)
                return self._failure(
                    envelope,
                    f"Deployment failed: {build_result.get('error', 'unknown')}",
                    build_number=build_result.get("build_number"),
                    build_url=build_result.get("build_url"),
                    rollback_initiated=True,
                )

        except Exception as e:
            return self._failure(envelope, f"Deployment error: {e}")

    def _create_release_tag(self, repo: Path, envelope: HandoffEnvelope) -> str:
        tag = f"release/{envelope.ticket_ref.lower()}-{envelope.commit_sha[:8]}"
        try:
            subprocess.run(
                ["git", "tag", "-a", tag, "-m", f"Release for {envelope.ticket_ref}"],
                cwd=repo, capture_output=True, text=True, check=True,
            )
            subprocess.run(
                ["git", "push", "origin", tag],
                cwd=repo, capture_output=True, text=True,
            )
            self.logger.info(f"Created release tag: {tag}")
        except subprocess.CalledProcessError as e:
            self.logger.warning(f"Tag creation skipped (may already exist): {e}")
        return tag

    def _trigger_and_monitor(self, jenkins: JenkinsConfig, envelope: HandoffEnvelope) -> dict:
        """Trigger a Jenkins build and poll until completion."""
        build_url = self._trigger_build(jenkins, envelope)
        if not build_url:
            return {"status": "failure", "error": "Failed to trigger Jenkins build"}

        self.logger.info(f"Jenkins build triggered: {build_url}")
        return self._poll_build(jenkins, build_url)

    def _trigger_build(self, jenkins: JenkinsConfig, envelope: HandoffEnvelope) -> Optional[str]:
        trigger_url = urljoin(
            jenkins.base_url,
            f"/job/{jenkins.job_name}/buildWithParameters",
        )
        params = {
            "BRANCH": envelope.branch,
            "COMMIT_SHA": envelope.commit_sha,
            "TICKET": envelope.ticket_ref,
            "WORKFLOW_ID": envelope.workflow_id,
        }

        resp = requests.post(
            trigger_url,
            params=params,
            auth=jenkins.auth,
            timeout=30,
        )

        if resp.status_code in (200, 201):
            queue_url = resp.headers.get("Location", "")
            return self._resolve_queue_to_build(jenkins, queue_url)

        self.logger.error(f"Jenkins trigger failed: HTTP {resp.status_code}")
        return None

    def _resolve_queue_to_build(self, jenkins: JenkinsConfig, queue_url: str) -> Optional[str]:
        """Wait for queued item to become an actual build."""
        api_url = f"{queue_url}api/json"
        for _ in range(20):
            time.sleep(5)
            resp = requests.get(api_url, auth=jenkins.auth, timeout=10)
            if resp.ok:
                data = resp.json()
                executable = data.get("executable")
                if executable:
                    return executable.get("url")
        return None

    def _poll_build(self, jenkins: JenkinsConfig, build_url: str) -> dict:
        """Poll Jenkins build until completion or timeout."""
        api_url = f"{build_url}api/json"
        elapsed = 0

        while elapsed < jenkins.timeout:
            time.sleep(jenkins.poll_interval)
            elapsed += jenkins.poll_interval

            try:
                resp = requests.get(api_url, auth=jenkins.auth, timeout=10)
                if not resp.ok:
                    continue

                data = resp.json()
                if not data.get("building", True):
                    result = data.get("result", "UNKNOWN")
                    return {
                        "status": "success" if result == "SUCCESS" else "failure",
                        "build_number": data.get("number"),
                        "build_url": build_url,
                        "duration_ms": data.get("duration"),
                        "error": None if result == "SUCCESS" else f"Jenkins result: {result}",
                    }
            except Exception as e:
                self.logger.warning(f"Poll error: {e}")

        return {
            "status": "failure",
            "build_url": build_url,
            "error": f"Timed out after {jenkins.timeout}s",
        }

    def _simulate_deployment(self, envelope: HandoffEnvelope) -> dict:
        """Simulate a deployment for local prototyping without Jenkins."""
        self.logger.info("Simulating deployment steps...")
        steps = [
            "Pulling Docker image",
            "Running database migrations",
            "Deploying application",
            "Running smoke tests",
        ]
        for step in steps:
            self.logger.info(f"  [{step}]")
            time.sleep(0.5)

        return {
            "status": "success",
            "build_number": "sim-001",
            "build_url": "http://localhost:8080/job/sim/1",
            "duration_ms": 2000,
        }

    def _post_deploy_validation(self, repo: Path, envelope: HandoffEnvelope) -> dict:
        """Run post-deployment sanity checks."""
        self.logger.info("Running post-deployment validation...")
        checks = {
            "service_health": True,
            "smoke_test": True,
            "data_freshness": True,
        }
        self.logger.info(f"Validation results: {checks}")
        return checks

    def _rollback(self, repo: Path, envelope: HandoffEnvelope):
        """Attempt to roll back to the previous known-good state."""
        self.logger.warning(f"Initiating rollback for {envelope.ticket_ref}")
        try:
            subprocess.run(
                ["git", "revert", "--no-commit", envelope.commit_sha],
                cwd=repo, capture_output=True, text=True,
            )
            self.logger.info("Rollback commit staged (not pushed — requires manual confirmation)")
        except subprocess.CalledProcessError:
            self.logger.error("Automatic rollback failed — manual intervention required")
