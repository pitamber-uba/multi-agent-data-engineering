#!/usr/bin/env python3
"""
End-to-end runner for the multi-agent data engineering workflow.

Demonstrates the full pipeline: Development -> Testing -> PR Creation -> Deployment

Usage:
    # Template mode (no AI, no API key needed)
    python run_workflow.py --demo

    # AI mode — agents use Claude to generate code, fix tests, write PRs
    export ANTHROPIC_API_KEY=sk-ant-...
    python run_workflow.py --demo --ai

    # AI mode with OpenAI instead
    export OPENAI_API_KEY=sk-...
    python run_workflow.py --demo --ai --ai-provider openai

    # AI mode via LangChain (Claude)
    export ANTHROPIC_API_KEY=sk-ant-...
    python run_workflow.py --demo --ai --ai-provider langchain-anthropic

    # AI mode via LangChain (OpenAI)
    export OPENAI_API_KEY=sk-...
    python run_workflow.py --demo --ai --ai-provider langchain-openai

    # AI mode with Google Gemini (direct)
    export GOOGLE_API_KEY=your-google-api-key
    python run_workflow.py --demo --ai --ai-provider gemini

    # AI mode via LangChain (Gemini)
    export GOOGLE_API_KEY=your-google-api-key
    python run_workflow.py --demo --ai --ai-provider langchain-gemini

    # Real repo mode
    python run_workflow.py --repo /path/to/repo --ticket TICKET-123 --spec spec.yaml --ai
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from orchestrator import Orchestrator, WorkflowConfig, Stage
from agents import DevelopmentAgent, TestingAgent, PullRequestAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("runner")


def create_ai_provider(config: WorkflowConfig):
    """Create an AI provider if configured."""
    if not config.ai_provider:
        return None

    from ai.provider import AIProvider

    provider = AIProvider(
        repo_path=config.repo_path,
        provider=config.ai_provider,
        model=config.ai_model or None,
    )
    logger.info(f"AI provider: {config.ai_provider} (model: {provider.model})")
    return provider


def build_agents(config: WorkflowConfig) -> dict:
    """Build agents with optional AI provider."""
    ai = create_ai_provider(config)

    if ai:
        logger.info("Agents will use AI (Cursor-style) for code generation and analysis")
    else:
        logger.info("Agents will use template-based generation (set --ai to enable AI)")

    return {
        Stage.DEVELOPMENT: DevelopmentAgent(ai_provider=ai),
        Stage.TESTING: TestingAgent(ai_provider=ai),
        Stage.PR_CREATION: PullRequestAgent(ai_provider=ai),
    }


def setup_demo_repo() -> Path:
    """Create a Git repo inside prototype/output/ for demonstration."""
    project_root = Path(__file__).parent
    tmp = project_root / "output"
    if tmp.exists():
        import shutil
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    logger.info(f"Demo repo created at: {tmp}")

    subprocess.run(["git", "init"], cwd=tmp, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "agent@demo.local"],
        cwd=tmp, capture_output=True, check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Multi-Agent Demo"],
        cwd=tmp, capture_output=True, check=True,
    )

    readme = tmp / "README.md"
    readme.write_text("# Demo Pipeline Repo\n")
    subprocess.run(["git", "add", "."], cwd=tmp, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "chore: initial commit"],
        cwd=tmp, capture_output=True, check=True,
    )

    return tmp


def copy_spec_to_repo(repo: Path, spec_source: Path) -> Path:
    config_dir = repo / "config"
    config_dir.mkdir(exist_ok=True)
    dest = config_dir / spec_source.name
    dest.write_text(spec_source.read_text())
    return dest


def run_demo(args):
    """Run a fully self-contained demo using a temp Git repo."""
    logger.info("=" * 60)
    logger.info("MULTI-AGENT DATA ENGINEERING — DEMO MODE")
    if args.ai:
        logger.info(f"  AI ENABLED: {args.ai_provider}")
    else:
        logger.info("  TEMPLATE MODE (no AI)")
    logger.info("=" * 60)

    repo = setup_demo_repo()
    spec_src = Path(__file__).parent / "config" / "pipeline_spec.yaml"
    spec_dest = copy_spec_to_repo(repo, spec_src)

    ai_provider = ""
    if args.ai:
        ai_provider = args.ai_provider
        from ai.provider import PROVIDER_ENV_KEYS
        key_var = PROVIDER_ENV_KEYS.get(ai_provider, "ANTHROPIC_API_KEY")
        if not os.environ.get(key_var):
            logger.error(f"AI mode with '{ai_provider}' requires {key_var} environment variable")
            logger.error(f"  export {key_var}=your-api-key-here")
            sys.exit(1)

    config = WorkflowConfig(
        repo_path=str(repo),
        base_branch="main",
        ticket_ref="TICKET-123",
        pipeline_spec=str(spec_dest),
        jenkins_url="",
        jenkins_job="",
        max_retries=2,
        ai_provider=ai_provider,
        ai_model=args.ai_model,
    )

    agents = build_agents(config)
    orch = Orchestrator(config, agents)

    logger.info("")
    logger.info("Starting orchestrated workflow...")
    logger.info(f"  Repo:    {repo}")
    logger.info(f"  Spec:    {spec_dest}")
    logger.info(f"  Ticket:  {config.ticket_ref}")
    logger.info(f"  AI:      {'ENABLED (' + ai_provider + ')' if ai_provider else 'disabled'}")
    logger.info("")

    result = orch.run()

    logger.info("")
    logger.info("=" * 60)
    logger.info("WORKFLOW RESULT")
    logger.info("=" * 60)
    logger.info(f"  Stage:   {result.stage.value}")
    logger.info(f"  Result:  {result.result.value if result.result else 'N/A'}")
    logger.info(f"  Branch:  {result.branch}")
    logger.info(f"  Commit:  {result.commit_sha[:12] if result.commit_sha else 'N/A'}")
    if result.error:
        logger.info(f"  Error:   {result.error}")
    logger.info("")

    logger.info("Workflow history:")
    for i, step in enumerate(orch.history, 1):
        status = step.result.value if step.result else "?"
        ai_flag = " [AI]" if step.metadata.get("ai_generated") else ""
        logger.info(f"  {i}. {step.stage.value:15s} -> {status}{ai_flag}")

    log_dir = repo / ".workflow_logs"
    if log_dir.exists():
        logs = list(log_dir.glob("*.json"))
        if logs:
            logger.info(f"\nFull workflow log: {logs[0]}")

    return result


def ensure_git_repo(repo_path: str) -> Path:
    """Ensure the repo directory exists and is inside a git repository."""
    repo = Path(repo_path)
    if not repo.is_absolute():
        repo = Path(__file__).parent / repo
    repo = repo.resolve()
    repo.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=repo, capture_output=True, text=True,
    )
    if result.returncode == 0:
        git_root = Path(result.stdout.strip())
        logger.info(f"Using existing git repo at: {git_root}")
        return repo

    logger.info(f"Initializing new git repo at: {repo}")
    subprocess.run(["git", "init"], cwd=repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "agent@pipeline.local"],
        cwd=repo, capture_output=True, check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Multi-Agent Pipeline"],
        cwd=repo, capture_output=True, check=True,
    )
    readme = repo / "README.md"
    if not readme.exists():
        readme.write_text("# Pipeline Repo\n")
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "chore: initial commit"],
        cwd=repo, capture_output=True, check=True,
    )
    return repo


def run_workflow(args):
    """Run the workflow against a real repository."""
    repo = ensure_git_repo(args.repo)
    ai_provider = args.ai_provider if args.ai else ""

    spec_path = args.spec
    if spec_path:
        spec_src = Path(spec_path)
        if not spec_src.is_absolute():
            spec_src = (Path(__file__).parent / spec_src).resolve()
        spec_dest = copy_spec_to_repo(repo, spec_src)
        spec_path = str(spec_dest)

    config = WorkflowConfig(
        repo_path=str(repo),
        base_branch=args.base_branch,
        ticket_ref=args.ticket,
        pipeline_spec=spec_path,
        jenkins_url=args.jenkins_url or os.environ.get("JENKINS_URL", ""),
        jenkins_job=args.jenkins_job or os.environ.get("JENKINS_JOB", ""),
        max_retries=args.max_retries,
        ai_provider=ai_provider,
        ai_model=args.ai_model,
    )

    agents = build_agents(config)
    orch = Orchestrator(config, agents)
    result = orch.run()
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Multi-Agent Data Engineering Workflow Runner",
    )
    parser.add_argument("--demo", action="store_true", help="Run self-contained demo")
    parser.add_argument("--repo", type=str, help="Path to the target Git repository")
    parser.add_argument("--ticket", type=str, default="TICKET-000", help="Ticket reference")
    parser.add_argument("--spec", type=str, help="Path to pipeline spec YAML")
    parser.add_argument("--base-branch", type=str, default="main")
    parser.add_argument("--jenkins-url", type=str, default="")
    parser.add_argument("--jenkins-job", type=str, default="")
    parser.add_argument("--max-retries", type=int, default=2)

    parser.add_argument(
        "--ai", action="store_true",
        help="Enable AI agents (requires ANTHROPIC_API_KEY or OPENAI_API_KEY)",
    )
    parser.add_argument(
        "--ai-provider", type=str, default="anthropic",
        choices=[
            "anthropic", "openai", "gemini",
            "langchain-anthropic", "langchain-openai", "langchain-gemini",
        ],
        help="AI provider: anthropic, openai, gemini (direct API), "
             "or langchain-anthropic, langchain-openai, langchain-gemini (via LangChain)",
    )
    parser.add_argument(
        "--ai-model", type=str, default="",
        help="Specific model name (default: claude-sonnet-4-20250514 or gpt-4o)",
    )

    args = parser.parse_args()

    if args.demo or (not args.repo):
        result = run_demo(args)
    elif args.repo:
        result = run_workflow(args)

    sys.exit(0 if result.result and result.result.value == "success" else 1)


if __name__ == "__main__":
    main()
