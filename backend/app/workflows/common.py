"""Safe workflow failure metadata; never includes upstream text or credentials."""

from app.dataops.orchestration import classify_failure


class WorkflowFailure(RuntimeError):
    def __init__(self, reason):
        self.policy = classify_failure(reason)
        super().__init__(self.policy.reason)


def workflow_failure(exc):
    return WorkflowFailure(str(exc))
