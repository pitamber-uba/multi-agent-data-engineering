# Agent Handoff Protocol

## Overview

This document establishes the formal protocol for agent-to-agent handoffs in the
multi-agent data engineering system. Each handoff carries a structured context
envelope that ensures traceability, reproducibility, and clean failure recovery.

---

## The HandoffEnvelope

Every inter-agent transition passes a `HandoffEnvelope` — a structured data object
that serves as the contract between agents.

```python
@dataclass
class HandoffEnvelope:
    workflow_id: str              # UUID for the entire workflow run
    stage: Stage                  # Current stage (development, testing, etc.)
    previous_stage: Optional[Stage]  # Where this envelope came from
    branch: str                   # Git branch being worked on
    ticket_ref: str               # Issue/ticket reference (e.g., TICKET-123)
    commit_sha: str               # Latest commit SHA on the branch
    timestamp: str                # ISO-8601 timestamp of envelope creation
    metadata: dict                # Stage-specific data (files, test results, etc.)
    result: Optional[StageResult] # SUCCESS, FAILURE, or RETRY
    error: Optional[str]          # Error message if result is FAILURE/RETRY
```

### Key Properties

- **Immutable per stage:** Once an agent completes, its envelope is appended to the
  workflow history. Subsequent agents receive a fresh envelope with updated fields.
- **Serializable:** Envelopes serialize to/from JSON for logging and persistence.
- **Self-describing:** Each envelope contains its origin (`previous_stage`) and
  destination (`stage`), making the workflow traceable without external state.

---

## Transition Rules

### State Machine

```
                     ┌──────────────┐
                     │              │
     ┌──────────────►│  DEVELOPMENT │◄─────────────────┐
     │               │              │                   │
     │               └──────┬───────┘                   │
     │                      │                           │
     │                 SUCCESS                     FAILURE
     │                      │                      (feedback)
     │                      ▼                           │
     │               ┌──────────────┐                   │
     │               │              │──── RETRY ────────┤
     │               │   TESTING    │                   │
     │               │              │──── FAILURE ──────┘
     │               └──────┬───────┘
     │                      │
     │                 SUCCESS
     │                      │
     │                      ▼
     │               ┌──────────────┐
     │               │              │
     │               │ PR CREATION  │──── FAILURE ──► FAILED
     │               │              │
     │               └──────┬───────┘
     │                      │
     │                 SUCCESS
     │                      │
     │                      ▼
     │               ┌──────────────┐
     │               │              │
     │               │  DEPLOYMENT  │──── FAILURE ──► FAILED
     │               │              │
     │               └──────┬───────┘
     │                      │
     │                 SUCCESS
     │                      │
     │                      ▼
     │               ┌──────────────┐
     │               │              │
     └───────────────│  COMPLETED   │
                     │              │
                     └──────────────┘
```

### Transition Table

| Current Stage | Result | Next Stage | Notes |
|---|---|---|---|
| Development | SUCCESS | Testing | Code committed, branch ready |
| Development | FAILURE | **FAILED** | Terminal — spec may be invalid |
| Testing | SUCCESS | PR Creation | All checks passed |
| Testing | FAILURE | Development | Feedback loop — tests failed |
| Testing | RETRY | Testing | Auto-fix applied, re-run checks |
| PR Creation | SUCCESS | Deployment | PR created (or simulated) |
| PR Creation | FAILURE | **FAILED** | Terminal — GitHub/Git error |
| Deployment | SUCCESS | **COMPLETED** | Pipeline deployed and validated |
| Deployment | FAILURE | **FAILED** | Rollback initiated |

---

## Handoff Details by Transition

### Development → Testing

**Trigger:** Code committed to feature branch

**Envelope contents:**
```json
{
  "stage": "testing",
  "previous_stage": "development",
  "branch": "feature/ticket-123",
  "commit_sha": "e5a73728574e",
  "metadata": {
    "files_created": ["pipelines/sales_daily_etl.py", "tests/test_sales_daily_etl.py"],
    "pipeline_name": "sales_daily_etl"
  },
  "result": "success"
}
```

**What Testing Agent receives:**
- Branch name to checkout
- Commit SHA to verify
- List of files to lint and test

**What Testing Agent does:**
1. Checks out the branch at the specified commit
2. Runs `ruff check` on pipelines/ and tests/
3. Runs `pytest` with coverage
4. If lint fails, attempts auto-fix → RETRY
5. If tests fail, returns FAILURE with diagnostic output
6. If all pass, returns SUCCESS with test metrics

---

### Testing → PR Creation

**Trigger:** All quality gates pass (lint + tests)

**Envelope contents:**
```json
{
  "stage": "pr_creation",
  "previous_stage": "testing",
  "metadata": {
    "lint_passed": true,
    "tests_passed": true,
    "coverage": "59%",
    "all_checks_passed": true,
    "pipeline_name": "sales_daily_etl"
  },
  "result": "success"
}
```

**What PR Agent receives:**
- Branch with passing tests
- Test result metadata for PR body
- Pipeline name for PR title

**What PR Agent does:**
1. Analyzes diff between feature branch and base
2. Reads commit log
3. Formats test results for the PR body
4. Generates structured PR title and description
5. Creates PR via `gh` CLI or writes locally

---

### Testing → Development (failure feedback)

**Trigger:** Unit tests fail

**Envelope contents:**
```json
{
  "stage": "testing",
  "previous_stage": "development",
  "metadata": {
    "lint_passed": true,
    "tests_passed": false,
    "test_output": "FAILED tests/test_sales_daily_etl.py::TestValidate::test_...",
    "coverage": "N/A"
  },
  "result": "failure",
  "error": "Failed checks: unit tests"
}
```

**What Development Agent receives on retry:**
- The same branch (still checked out)
- Error details from the Testing Agent
- Test output for diagnostic context

**What Development Agent does:**
- Re-generates code (potentially with fixes if integrated with AI feedback)
- Commits updated code
- Hands back to Testing

---

### PR Creation → Deployment

**Trigger:** PR successfully created

**Envelope contents:**
```json
{
  "stage": "deployment",
  "previous_stage": "pr_creation",
  "metadata": {
    "pr_url": "https://github.com/org/repo/pull/1",
    "pr_title": "feat(TICKET-123): add sales_daily_etl pipeline"
  },
  "result": "success"
}
```

**What Deployment Agent receives:**
- Branch and commit SHA for the release
- PR URL for reference
- Workflow context for Jenkins parameterization

**What Deployment Agent does:**
1. Creates a release tag (`release/ticket-123-<sha>`)
2. Triggers Jenkins build with parameters
3. Polls build until completion
4. Runs post-deployment validation
5. On failure, initiates rollback

---

## Retry and Error Recovery

### Retry Protocol

1. **Stage-level retries:** Controlled by `max_retries` in WorkflowConfig (default: 2)
2. **Retry counter:** The orchestrator tracks retries per stage independently
3. **Retry vs. failure:** Agents return `RETRY` for transient/auto-fixable issues
   and `FAILURE` for terminal conditions
4. **Max retry exceeded:** Workflow transitions to `FAILED`

### Error Propagation

Errors are carried in the envelope's `error` field and accumulated in the workflow
history. This provides:

- A complete error chain for post-mortem analysis
- Context for downstream agents to make informed decisions
- Data for alerting systems to categorize failure types

---

## Metadata Propagation

Metadata flows forward through the workflow, accumulating context:

| Stage | Metadata Added |
|---|---|
| Development | `files_created`, `pipeline_name` |
| Testing | `lint_passed`, `tests_passed`, `coverage`, `test_output` |
| PR Creation | `pr_url`, `pr_title` |
| Deployment | `release_tag`, `build_number`, `build_url`, `validation` |

The orchestrator preserves metadata across handoffs, ensuring that later stages
have access to information from earlier stages (e.g., the PR body includes test
results from the Testing stage).

---

## Workflow Persistence

Every workflow run is persisted as a JSON file in `.workflow_logs/`:

```
.workflow_logs/
  38dcb479-3b5f-438b-8444-307e442d6421.json
```

The log contains the complete sequence of HandoffEnvelopes, providing:
- Full audit trail of every agent action
- Reproducible context for debugging
- Data for workflow analytics and optimization
