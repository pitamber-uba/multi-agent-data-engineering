"""
Multi-Agent Orchestrator for Data Engineering Pipelines

Coordinates the flow: Development → Testing → PR Creation → Deployment
Each stage is handled by a specialized agent with defined inputs/outputs.
"""

import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("orchestrator")


class Stage(str, Enum):
    DEVELOPMENT = "development"
    TESTING = "testing"
    CODE_REVIEW = "code_review"
    PR_CREATION = "pr_creation"
    DEPLOYMENT = "deployment"
    COMPLETED = "completed"
    FAILED = "failed"


class StageResult(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"


@dataclass
class HandoffEnvelope:
    workflow_id: str
    stage: Stage
    previous_stage: Optional[Stage]
    branch: str
    ticket_ref: str
    commit_sha: str
    timestamp: str
    metadata: dict = field(default_factory=dict)
    result: Optional[StageResult] = None
    error: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)

    @classmethod
    def from_json(cls, data: str) -> "HandoffEnvelope":
        return cls(**json.loads(data))


@dataclass
class WorkflowConfig:
    repo_path: str
    base_branch: str = "main"
    ticket_ref: str = ""
    pipeline_spec: str = ""
    jenkins_url: str = ""
    jenkins_job: str = ""
    max_retries: int = 2
    ai_provider: str = ""
    ai_model: str = ""


class Orchestrator:
    """
    Drives the multi-agent workflow through defined stages.
    Each stage delegates to a specialized agent and evaluates the result
    to determine the next transition.
    """

    TRANSITIONS = {
        (Stage.DEVELOPMENT, StageResult.SUCCESS): Stage.TESTING,
        (Stage.DEVELOPMENT, StageResult.FAILURE): Stage.FAILED,
        (Stage.TESTING, StageResult.SUCCESS): Stage.CODE_REVIEW,
        (Stage.TESTING, StageResult.FAILURE): Stage.DEVELOPMENT,
        (Stage.TESTING, StageResult.RETRY): Stage.TESTING,
        (Stage.CODE_REVIEW, StageResult.SUCCESS): Stage.PR_CREATION,
        (Stage.CODE_REVIEW, StageResult.FAILURE): Stage.FAILED,
        (Stage.CODE_REVIEW, StageResult.RETRY): Stage.TESTING,
        (Stage.PR_CREATION, StageResult.SUCCESS): Stage.COMPLETED,
        (Stage.PR_CREATION, StageResult.FAILURE): Stage.FAILED,
    }

    def __init__(self, config: WorkflowConfig, agents: dict):
        self.config = config
        self.agents = agents
        self.workflow_id = str(uuid.uuid4())
        self.history: list[HandoffEnvelope] = []
        self.retry_counts: dict[Stage, int] = {}

    def run(self) -> HandoffEnvelope:
        logger.info(f"Starting workflow {self.workflow_id} for {self.config.ticket_ref}")

        current_stage = Stage.DEVELOPMENT
        branch = f"feature/{self.config.ticket_ref.lower()}"
        commit_sha = ""
        accumulated_metadata: dict = {}

        while current_stage not in (Stage.COMPLETED, Stage.FAILED):
            envelope = HandoffEnvelope(
                workflow_id=self.workflow_id,
                stage=current_stage,
                previous_stage=self.history[-1].stage if self.history else None,
                branch=branch,
                ticket_ref=self.config.ticket_ref,
                commit_sha=commit_sha,
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata=dict(accumulated_metadata),
            )

            logger.info(f"Executing stage: {current_stage.value}")
            agent = self.agents.get(current_stage)

            if not agent:
                logger.error(f"No agent registered for stage: {current_stage.value}")
                envelope.result = StageResult.FAILURE
                envelope.error = f"No agent for {current_stage.value}"
                self.history.append(envelope)
                break

            try:
                result_envelope = agent.execute(envelope, self.config)
                self.history.append(result_envelope)

                commit_sha = result_envelope.commit_sha or commit_sha
                branch = result_envelope.branch or branch
                accumulated_metadata.update(result_envelope.metadata)

                next_key = (current_stage, result_envelope.result)
                next_stage = self.TRANSITIONS.get(next_key)

                if next_stage is None:
                    logger.error(f"No transition for {next_key}")
                    break

                if next_stage == current_stage:
                    self.retry_counts[current_stage] = self.retry_counts.get(current_stage, 0) + 1
                    if self.retry_counts[current_stage] > self.config.max_retries:
                        logger.error(f"Max retries exceeded for {current_stage.value}")
                        current_stage = Stage.FAILED
                        continue

                current_stage = next_stage
                logger.info(f"Transitioning to: {current_stage.value}")

            except Exception as e:
                logger.exception(f"Agent failure at {current_stage.value}")
                envelope.result = StageResult.FAILURE
                envelope.error = str(e)
                self.history.append(envelope)
                current_stage = Stage.FAILED

        final = self.history[-1] if self.history else envelope
        self._save_workflow_log()
        logger.info(f"Workflow {self.workflow_id} finished: {current_stage.value}")
        return final

    def _save_workflow_log(self):
        log_dir = Path(self.config.repo_path) / ".workflow_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{self.workflow_id}.json"
        log_file.write_text(json.dumps(
            [asdict(e) for e in self.history],
            indent=2,
            default=str,
        ))
        logger.info(f"Workflow log saved: {log_file}")
