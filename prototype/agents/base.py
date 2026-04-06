"""
Base agent interface that all specialized agents implement.
"""

from abc import ABC, abstractmethod
import logging

from orchestrator import HandoffEnvelope, WorkflowConfig, StageResult


class BaseAgent(ABC):

    def __init__(self, ai_provider=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.ai = ai_provider

    @property
    def ai_enabled(self) -> bool:
        return self.ai is not None

    @abstractmethod
    def execute(self, envelope: HandoffEnvelope, config: WorkflowConfig) -> HandoffEnvelope:
        ...

    def _success(self, envelope: HandoffEnvelope, **metadata) -> HandoffEnvelope:
        envelope.result = StageResult.SUCCESS
        envelope.metadata.update(metadata)
        self.logger.info(f"Stage {envelope.stage.value} completed successfully")
        return envelope

    def _failure(self, envelope: HandoffEnvelope, error: str, **metadata) -> HandoffEnvelope:
        envelope.result = StageResult.FAILURE
        envelope.error = error
        envelope.metadata.update(metadata)
        self.logger.error(f"Stage {envelope.stage.value} failed: {error}")
        return envelope

    def _retry(self, envelope: HandoffEnvelope, reason: str) -> HandoffEnvelope:
        envelope.result = StageResult.RETRY
        envelope.error = reason
        self.logger.warning(f"Stage {envelope.stage.value} requesting retry: {reason}")
        return envelope
