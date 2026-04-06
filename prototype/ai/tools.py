"""
Repository tools that the AI agent can invoke via tool-calling.

These mirror the capabilities Cursor AI has in Agent Mode:
- Read files from the repo
- Write/create files in the repo
- Run shell commands (git, pytest, ruff, etc.)
- List directory contents
- Search for patterns in code

The AI decides WHICH tools to call and in what order — just like Cursor.
"""

import os
import subprocess
from pathlib import Path
from typing import Optional


TOOL_DEFINITIONS = [
    {
        "name": "read_file",
        "description": "Read the contents of a file in the repository. Use this to understand existing code, configs, or specs before writing new code.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path relative to the repository root",
                }
            },
            "required": ["path"],
        },
    },
    {
        "name": "write_file",
        "description": "Write content to a file in the repository. Creates parent directories if needed. Use this to create pipeline code, tests, configs, etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path relative to the repository root",
                },
                "content": {
                    "type": "string",
                    "description": "Full file content to write",
                },
            },
            "required": ["path", "content"],
        },
    },
    {
        "name": "run_command",
        "description": "Run a shell command in the repository directory. Use for git operations, running tests, linting, etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "Shell command to execute",
                },
            },
            "required": ["command"],
        },
    },
    {
        "name": "list_directory",
        "description": "List files and directories at a given path. Use to explore repo structure.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Directory path relative to repo root. Use '.' for root.",
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "search_code",
        "description": "Search for a pattern across files in the repository using grep. Use to find existing patterns, imports, or conventions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Regex pattern to search for",
                },
                "file_glob": {
                    "type": "string",
                    "description": "File glob to filter (e.g., '*.py'). Optional.",
                },
            },
            "required": ["pattern"],
        },
    },
]


class RepoTools:
    """Executes tool calls requested by the AI within a repository."""

    def __init__(self, repo_path: str):
        self.repo = Path(repo_path)

    def execute_tool(self, tool_name: str, tool_input: dict) -> str:
        handlers = {
            "read_file": self._read_file,
            "write_file": self._write_file,
            "run_command": self._run_command,
            "list_directory": self._list_directory,
            "search_code": self._search_code,
        }
        handler = handlers.get(tool_name)
        if not handler:
            return f"Error: Unknown tool '{tool_name}'"
        try:
            return handler(**tool_input)
        except Exception as e:
            return f"Error: {e}"

    def _read_file(self, path: str) -> str:
        target = self.repo / path
        if not target.exists():
            return f"Error: File not found: {path}"
        if not target.is_file():
            return f"Error: Not a file: {path}"
        content = target.read_text()
        if len(content) > 50_000:
            return content[:50_000] + "\n\n... (truncated)"
        return content

    def _write_file(self, path: str, content: str) -> str:
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        return f"Successfully wrote {len(content)} chars to {path}"

    def _run_command(self, command: str) -> str:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(self.repo) + os.pathsep + env.get("PYTHONPATH", "")

        result = subprocess.run(
            command,
            shell=True,
            cwd=self.repo,
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        output = ""
        if result.stdout:
            output += result.stdout
        if result.stderr:
            output += ("\n" if output else "") + result.stderr
        if not output:
            output = "(no output)"
        exit_info = f"\n[exit code: {result.returncode}]"

        if len(output) > 20_000:
            output = output[:20_000] + "\n... (truncated)"
        return output + exit_info

    def _list_directory(self, path: str) -> str:
        target = self.repo / path
        if not target.exists():
            return f"Error: Directory not found: {path}"
        if not target.is_dir():
            return f"Error: Not a directory: {path}"

        entries = sorted(target.iterdir())
        lines = []
        for entry in entries:
            if entry.name.startswith(".") and entry.name not in (".gitignore",):
                continue
            prefix = "d " if entry.is_dir() else "f "
            lines.append(f"{prefix}{entry.name}")
        return "\n".join(lines) if lines else "(empty directory)"

    def _search_code(self, pattern: str, file_glob: Optional[str] = None) -> str:
        cmd = ["grep", "-rn", pattern]
        if file_glob:
            cmd.extend(["--include", file_glob])
        cmd.append(".")

        result = subprocess.run(
            cmd, cwd=self.repo, capture_output=True, text=True, timeout=30,
        )
        output = result.stdout.strip()
        if not output:
            return f"No matches found for pattern: {pattern}"
        lines = output.splitlines()
        if len(lines) > 50:
            return "\n".join(lines[:50]) + f"\n\n... ({len(lines)} total matches)"
        return output
