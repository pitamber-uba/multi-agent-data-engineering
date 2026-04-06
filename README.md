# Multi-Agent Data Engineering

Research prototype exploring whether multiple AI agents can be orchestrated to handle
different stages of the data engineering lifecycle: development, testing, PR creation,
and deployment.

**The agents use the same AI technology as Cursor** — Anthropic Claude with tool-calling
(read files, write files, run commands). Each agent is an autonomous AI loop that
reasons about the task and uses tools to accomplish it.

## Quick Start

### Template Mode (no API key needed)

```bash
pip install -r requirements.txt

cd prototype
python run_workflow.py --demo
```

### AI Mode (agents use Claude — same as Cursor)

```bash
pip install -r requirements.txt

export ANTHROPIC_API_KEY=sk-ant-your-key-here

cd prototype
python run_workflow.py --demo --ai
```

With `--ai` enabled, each agent sends a prompt to Claude with tools attached. Claude
autonomously decides how to read the repo, generate code, run tests, and fix issues —
exactly like Cursor Agent Mode.

### OpenAI Alternative

```bash
export OPENAI_API_KEY=sk-your-key-here
python run_workflow.py --demo --ai --ai-provider openai
```

## How It Works

### The Agent Loop (same as Cursor)

Each agent runs this loop — this is what makes it an "AI agent" rather than a script:

```
1. Send system prompt + task prompt + tools to Claude
2. Claude reasons about the task
3. Claude calls a tool (e.g., write_file, run_command)
4. We execute the tool and return results to Claude
5. Claude reasons again, calls more tools
6. Repeat until Claude says "done"
```

The tools available to each agent:

| Tool | What it does | Cursor equivalent |
|---|---|---|
| `read_file` | Read any file in the repo | Reading files in editor |
| `write_file` | Create or overwrite files | Editing files |
| `run_command` | Run shell commands (git, pytest, ruff) | Terminal access |
| `list_directory` | Explore repo structure | File explorer |
| `search_code` | Grep for patterns | Cmd+Shift+F search |

### The Four Agents

```
Ticket/Spec
     |
     v
+-----------+    +-----------+    +-----------+    +-----------+
|   Dev     |--->| Testing   |--->|    PR     |--->|  Deploy   |
|  Agent    |<---|  Agent    |    |  Agent    |    |  Agent    |
+-----------+    +-----------+    +-----------+    +-----------+
  AI writes        AI analyzes      AI reads        Tags release
  pipeline +       failures and     the diff and    triggers Jenkins
  tests from       fixes code       writes smart    monitors deploy
  YAML spec                         PR summary
```

**Development Agent (AI):** Receives the YAML pipeline spec. Claude reads it,
understands the ETL requirements, generates a Python pipeline class with pandas/sqlalchemy,
generates pytest tests, runs linting to verify, and commits to a feature branch.

**Testing Agent (AI):** Runs ruff and pytest. If tests fail, Claude reads the error
output AND the source code, understands the root cause, applies a fix, and re-runs.
This is like asking Cursor "fix this test failure".

**PR Agent (AI):** Claude reads the actual git diff and source code, then writes a
PR description that explains *what the code does* — not just lists files changed.

**Deployment Agent:** Tags the release, triggers Jenkins via REST API, polls until
complete, runs post-deploy validation. Falls back to simulation without Jenkins.

## Architecture

```
prototype/
├── ai/
│   ├── provider.py          # AI agent loop (Claude/OpenAI + tool-calling)
│   └── tools.py             # Tools the AI can invoke (read, write, shell, search)
├── agents/
│   ├── base.py              # Base class with AI provider injection
│   ├── development.py       # AI generates pipeline code from spec
│   ├── testing.py           # AI analyzes and fixes test failures
│   ├── pull_request.py      # AI writes intelligent PR descriptions
│   └── deployment.py        # Jenkins integration + deployment
├── orchestrator.py           # State machine: Dev -> Test -> PR -> Deploy
├── run_workflow.py           # CLI entry point (--demo, --ai, --repo)
└── config/
    └── pipeline_spec.yaml    # Sample ETL specification
```

## Running Against a Real Repository

```bash
python run_workflow.py \
  --repo /path/to/your/repo \
  --ticket TICKET-456 \
  --spec /path/to/pipeline_spec.yaml \
  --ai \
  --base-branch main \
  --jenkins-url http://jenkins.internal:8080 \
  --jenkins-job data-pipeline-deploy
```

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/01-architecture.md) | Agent definitions, handoff mechanisms |
| [Integration Feasibility](docs/02-integration-feasibility.md) | Git, GitHub, Jenkins, pytest validation |
| [Risks & Limitations](docs/03-risks-limitations.md) | Risks, constraints, operational challenges |
| [Handoff Protocol](docs/04-handoff-protocol.md) | State machine, envelope contracts, retry logic |

## Key Findings

**Feasibility: Confirmed.** The prototype demonstrates that AI agents with tool-calling
can autonomously handle each stage of the data engineering lifecycle. The same technology
that powers Cursor AI (Claude + tools) works programmatically to generate code, fix
bugs, and write documentation.

**What makes this work:** Giving the LLM the same capabilities a human developer has —
reading files, writing files, running commands, searching code. The LLM decides *what*
to do; the tools let it *execute*.

**Critical requirement:** Human review before merge. AI generates the code and the PR,
but a human must approve and merge. This is the safety net.
