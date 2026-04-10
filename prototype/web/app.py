"""
Web UI for the Multi-Agent Data Engineering Pipeline.

Endpoints:
  GET  /                  — Main UI
  GET  /api/specs         — List all YAML specs in config/
  GET  /api/specs/{name}  — Read a single spec
  PUT  /api/specs/{name}  — Update a spec
  POST /api/specs/upload  — Upload a new YAML spec
  POST /api/run           — Launch a workflow (returns job_id)
  GET  /api/run/{job_id}/stream — SSE stream of workflow progress
  GET  /api/runs          — List completed / running jobs
"""

import asyncio
import json
import logging
import os
import subprocess
import sys

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
from fastapi import FastAPI, UploadFile, File, HTTPException, Request  # type: ignore[import-untyped]
from fastapi.responses import HTMLResponse, StreamingResponse  # type: ignore[import-untyped]

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator import Orchestrator, WorkflowConfig, Stage, StageResult, HandoffEnvelope
from agents import DevelopmentAgent, TestingAgent, PullRequestAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("web")

PROTOTYPE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROTOTYPE_DIR / "config"
OUTPUT_DIR = PROTOTYPE_DIR / "output"

app = FastAPI(title="Multi-Agent Pipeline Builder")

jobs: dict[str, dict] = {}


def _config_dir():
    CONFIG_DIR.mkdir(exist_ok=True)
    return CONFIG_DIR


# ─── API: Spec management ──────────────────────────────────────────────────

@app.get("/api/specs")
def list_specs():
    specs = []
    for f in sorted(_config_dir().glob("*.yaml")):
        specs.append({"name": f.name, "path": str(f), "size": f.stat().st_size})
    return specs


@app.get("/api/specs/{name}")
def read_spec(name: str):
    path = _config_dir() / name
    if not path.exists():
        raise HTTPException(404, f"Spec '{name}' not found")
    content = path.read_text()
    try:
        parsed = yaml.safe_load(content)
    except Exception:
        parsed = None
    return {"name": name, "content": content, "parsed": parsed}


@app.put("/api/specs/{name}")
async def update_spec(name: str, request: Request):
    body = await request.json()
    content = body.get("content", "")
    if not content.strip():
        raise HTTPException(400, "Empty content")
    try:
        yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise HTTPException(400, f"Invalid YAML: {e}")
    path = _config_dir() / name
    path.write_text(content)
    return {"status": "updated", "name": name}


@app.post("/api/specs/upload")
async def upload_spec(file: UploadFile = File(...)):
    if not file.filename.endswith((".yaml", ".yml")):
        raise HTTPException(400, "Only .yaml / .yml files accepted")
    content = (await file.read()).decode("utf-8")
    try:
        yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise HTTPException(400, f"Invalid YAML: {e}")
    dest = _config_dir() / file.filename
    dest.write_text(content)
    return {"status": "uploaded", "name": file.filename}


# ─── API: Workflow execution ────────────────────────────────────────────────

class WorkflowLogCapture(logging.Handler):
    """Captures log records into a shared list for SSE streaming."""

    def __init__(self, store: list):
        super().__init__()
        self.store = store

    def emit(self, record):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": self.format(record),
        }
        self.store.append(entry)


def _run_workflow_thread(job_id: str, spec_name: str, ai_provider: str, ai_model: str):
    """Runs the orchestrator in a background thread, pushing events to jobs[job_id]."""
    job = jobs[job_id]
    log_entries = job["logs"]

    handler = WorkflowLogCapture(log_entries)
    handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    try:
        spec_path = _config_dir() / spec_name
        if not spec_path.exists():
            job["status"] = "failed"
            job["error"] = f"Spec file not found: {spec_name}"
            return

        spec_content = yaml.safe_load(spec_path.read_text())
        pipeline_cfg = spec_content.get("pipeline", {})
        ticket = pipeline_cfg.get("ticket", "TICKET-WEB")

        repo = OUTPUT_DIR
        repo.mkdir(parents=True, exist_ok=True)

        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=repo, capture_output=True, text=True,
        )
        if result.returncode != 0:
            subprocess.run(["git", "init"], cwd=repo, capture_output=True, check=True)
            subprocess.run(["git", "config", "user.email", "agent@pipeline.local"],
                           cwd=repo, capture_output=True, check=True)
            subprocess.run(["git", "config", "user.name", "Multi-Agent Pipeline"],
                           cwd=repo, capture_output=True, check=True)
            readme = repo / "README.md"
            if not readme.exists():
                readme.write_text("# Pipeline Repo\n")
            subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
            subprocess.run(["git", "commit", "-m", "chore: initial commit"],
                           cwd=repo, capture_output=True, check=True)

        config_dest = repo / "config"
        config_dest.mkdir(exist_ok=True)
        (config_dest / spec_name).write_text(spec_path.read_text())

        config = WorkflowConfig(
            repo_path=str(repo),
            base_branch="main",
            ticket_ref=ticket,
            pipeline_spec=str(config_dest / spec_name),
            max_retries=2,
            ai_provider=ai_provider,
            ai_model=ai_model,
        )

        ai = None
        if ai_provider:
            from ai.provider import AIProvider
            ai = AIProvider(repo_path=config.repo_path, provider=ai_provider, model=ai_model or None)

        agents_map = {
            Stage.DEVELOPMENT: DevelopmentAgent(ai_provider=ai),
            Stage.TESTING: TestingAgent(ai_provider=ai),
            Stage.PR_CREATION: PullRequestAgent(ai_provider=ai),
        }

        job["status"] = "running"
        job["stage"] = "development"
        log_entries.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "logger": "web",
            "message": f"Workflow started — spec: {spec_name}, AI: {ai_provider or 'disabled'}",
            "event": "stage_change",
            "stage": "development",
        })

        orch = Orchestrator(config, agents_map)

        current_stage = Stage.DEVELOPMENT
        branch = f"feature/{ticket.lower()}"
        commit_sha = ""
        accumulated_metadata: dict = {}

        while current_stage not in (Stage.COMPLETED, Stage.FAILED):
            stage_name = current_stage.value
            job["stage"] = stage_name
            log_entries.append({
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": "INFO",
                "logger": "web",
                "message": f"▶ Stage: {stage_name.upper()}",
                "event": "stage_change",
                "stage": stage_name,
            })

            envelope = HandoffEnvelope(
                workflow_id=orch.workflow_id,
                stage=current_stage,
                previous_stage=orch.history[-1].stage if orch.history else None,
                branch=branch,
                ticket_ref=config.ticket_ref,
                commit_sha=commit_sha,
                timestamp=datetime.now(timezone.utc).isoformat(),
                metadata=dict(accumulated_metadata),
            )

            agent = agents_map.get(current_stage)
            if not agent:
                job["status"] = "failed"
                job["error"] = f"No agent for {stage_name}"
                break

            try:
                result_envelope = agent.execute(envelope, config)
                orch.history.append(result_envelope)
                commit_sha = result_envelope.commit_sha or commit_sha
                branch = result_envelope.branch or branch
                accumulated_metadata.update(result_envelope.metadata)

                result_str = result_envelope.result.value if result_envelope.result else "unknown"
                log_entries.append({
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "level": "INFO",
                    "logger": "web",
                    "message": f"✓ {stage_name} → {result_str}",
                    "event": "stage_result",
                    "stage": stage_name,
                    "result": result_str,
                })

                if result_envelope.result == StageResult.SUCCESS and current_stage == Stage.PR_CREATION:
                    pr_url = result_envelope.metadata.get("pr_url", "")
                    if pr_url:
                        job["pr_url"] = pr_url
                        log_entries.append({
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "level": "INFO",
                            "logger": "web",
                            "message": f"Pull Request: {pr_url}",
                            "event": "pr_created",
                            "pr_url": pr_url,
                        })

                next_key = (current_stage, result_envelope.result)
                next_stage = Orchestrator.TRANSITIONS.get(next_key)
                if next_stage is None:
                    job["status"] = "failed"
                    job["error"] = f"No transition for {next_key}"
                    break

                if next_stage == current_stage:
                    retries = orch.retry_counts.get(current_stage, 0) + 1
                    orch.retry_counts[current_stage] = retries
                    if retries > config.max_retries:
                        current_stage = Stage.FAILED
                        continue

                current_stage = next_stage

            except Exception as e:
                logger.exception(f"Agent failure at {stage_name}")
                job["status"] = "failed"
                job["error"] = str(e)
                log_entries.append({
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "level": "ERROR",
                    "logger": "web",
                    "message": f"✗ {stage_name} failed: {e}",
                    "event": "error",
                    "stage": stage_name,
                })
                current_stage = Stage.FAILED

        if current_stage == Stage.COMPLETED:
            job["status"] = "completed"
            log_entries.append({
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": "INFO",
                "logger": "web",
                "message": "Workflow completed successfully",
                "event": "workflow_done",
                "result": "success",
            })
        elif job["status"] != "failed":
            job["status"] = "failed"
            log_entries.append({
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": "ERROR",
                "logger": "web",
                "message": f"Workflow failed: {job.get('error', 'unknown')}",
                "event": "workflow_done",
                "result": "failure",
            })

        orch._save_workflow_log()

    except Exception as e:
        logger.exception("Workflow thread crashed")
        job["status"] = "failed"
        job["error"] = str(e)
        log_entries.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": "ERROR",
            "logger": "web",
            "message": f"Fatal error: {e}",
            "event": "workflow_done",
            "result": "failure",
        })
    finally:
        root_logger.removeHandler(handler)
        job["finished_at"] = datetime.now(timezone.utc).isoformat()


@app.post("/api/run")
async def start_run(request: Request):
    body = await request.json()
    spec_name = body.get("spec")
    ai_provider = body.get("ai_provider", "gemini")
    ai_model = body.get("ai_model", "")

    if not spec_name:
        raise HTTPException(400, "spec is required")

    spec_path = _config_dir() / spec_name
    if not spec_path.exists():
        raise HTTPException(404, f"Spec '{spec_name}' not found")

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {
        "id": job_id,
        "spec": spec_name,
        "ai_provider": ai_provider,
        "ai_model": ai_model,
        "status": "queued",
        "stage": None,
        "pr_url": None,
        "error": None,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "logs": [],
    }

    thread = threading.Thread(
        target=_run_workflow_thread,
        args=(job_id, spec_name, ai_provider, ai_model),
        daemon=True,
    )
    thread.start()

    return {"job_id": job_id, "status": "queued"}


@app.get("/api/run/{job_id}/stream")
async def stream_run(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")

    async def event_generator():
        job = jobs[job_id]
        sent = 0

        while True:
            logs = job["logs"]
            while sent < len(logs):
                entry = logs[sent]
                yield f"data: {json.dumps(entry)}\n\n"
                sent += 1

            if job["status"] in ("completed", "failed"):
                summary = {
                    "event": "done",
                    "status": job["status"],
                    "pr_url": job.get("pr_url"),
                    "error": job.get("error"),
                }
                yield f"data: {json.dumps(summary)}\n\n"
                break

            await asyncio.sleep(0.5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/runs")
def list_runs():
    return [
        {k: v for k, v in j.items() if k != "logs"}
        for j in sorted(jobs.values(), key=lambda x: x["started_at"], reverse=True)
    ]


# ─── Serve the frontend ────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def index():
    html_path = Path(__file__).parent / "templates" / "index.html"
    return HTMLResponse(html_path.read_text())
