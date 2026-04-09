"""
LangChain-based AI Provider.

Uses LangChain's agent framework (create_agent) to run the same agent loop
as the direct Anthropic/OpenAI providers, but through LangChain's abstraction.

Benefits of the LangChain approach:
- Unified interface across LLM providers
- Built-in agent memory and conversation history
- LangChain's callback system for observability (LangSmith tracing)
- Easy swapping between models without code changes
- Access to LangChain ecosystem (chains, retrievers, vector stores)

Usage:
    export ANTHROPIC_API_KEY=sk-ant-...
    python run_workflow.py --demo --ai --ai-provider langchain-anthropic

    export OPENAI_API_KEY=sk-...
    python run_workflow.py --demo --ai --ai-provider langchain-openai
"""

import logging
import os
from typing import Optional

from .tools import RepoTools

logger = logging.getLogger("ai.langchain")

try:
    from langchain_core.tools import StructuredTool
    from langchain_core.messages import HumanMessage
    from langchain.agents import create_agent

    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

try:
    from langchain_anthropic import ChatAnthropic

    LANGCHAIN_ANTHROPIC_AVAILABLE = True
except ImportError:
    LANGCHAIN_ANTHROPIC_AVAILABLE = False

try:
    from langchain_openai import ChatOpenAI

    LANGCHAIN_OPENAI_AVAILABLE = True
except ImportError:
    LANGCHAIN_OPENAI_AVAILABLE = False

try:
    from langchain_google_genai import ChatGoogleGenerativeAI

    LANGCHAIN_GEMINI_AVAILABLE = True
except ImportError:
    LANGCHAIN_GEMINI_AVAILABLE = False


def _build_langchain_tools(repo_tools: RepoTools) -> list:
    """Convert RepoTools into LangChain StructuredTool objects."""
    from pydantic import BaseModel, Field

    class ReadFileArgs(BaseModel):
        path: str = Field(description="Path relative to the repository root")

    class WriteFileArgs(BaseModel):
        path: str = Field(description="Path relative to the repository root")
        content: str = Field(description="Full file content to write")

    class RunCommandArgs(BaseModel):
        command: str = Field(description="Shell command to execute")

    class ListDirectoryArgs(BaseModel):
        path: str = Field(description="Directory path relative to repo root. Use '.' for root.")

    class EditFileArgs(BaseModel):
        path: str = Field(description="Path relative to the repository root")
        old_string: str = Field(description="Exact string to find (must be unique in the file)")
        new_string: str = Field(description="Replacement string")

    class SearchCodeArgs(BaseModel):
        pattern: str = Field(description="Regex pattern to search for")
        file_glob: str = Field(default="", description="File glob to filter (e.g., '*.py'). Optional.")

    return [
        StructuredTool.from_function(
            func=lambda path: repo_tools.execute_tool("read_file", {"path": path}),
            name="read_file",
            description=(
                "Read the contents of a file in the repository. "
                "Use this to understand existing code, configs, or specs."
            ),
            args_schema=ReadFileArgs,
        ),
        StructuredTool.from_function(
            func=lambda path, content: repo_tools.execute_tool(
                "write_file", {"path": path, "content": content}
            ),
            name="write_file",
            description=(
                "Write content to a file in the repository. Creates parent directories if needed."
            ),
            args_schema=WriteFileArgs,
        ),
        StructuredTool.from_function(
            func=lambda path, old_string, new_string: repo_tools.execute_tool(
                "edit_file", {"path": path, "old_string": old_string, "new_string": new_string}
            ),
            name="edit_file",
            description=(
                "Edit an existing file by replacing a specific string with new content. "
                "Use instead of write_file when you only need to change part of a file. "
                "The old_string must match exactly (including whitespace)."
            ),
            args_schema=EditFileArgs,
        ),
        StructuredTool.from_function(
            func=lambda command: repo_tools.execute_tool("run_command", {"command": command}),
            name="run_command",
            description="Run a shell command in the repository directory.",
            args_schema=RunCommandArgs,
        ),
        StructuredTool.from_function(
            func=lambda path: repo_tools.execute_tool("list_directory", {"path": path}),
            name="list_directory",
            description="List files and directories at a given path.",
            args_schema=ListDirectoryArgs,
        ),
        StructuredTool.from_function(
            func=lambda pattern, file_glob="": repo_tools.execute_tool(
                "search_code",
                {"pattern": pattern, **({"file_glob": file_glob} if file_glob else {})},
            ),
            name="search_code",
            description="Search for a pattern across files in the repository using grep.",
            args_schema=SearchCodeArgs,
        ),
    ]


class LangChainProvider:
    """
    Runs the agent loop through LangChain's create_agent.

    Supports:
    - langchain-anthropic: Claude via LangChain
    - langchain-openai: GPT-4o via LangChain
    - langchain-gemini: Gemini via LangChain
    """

    def __init__(
        self,
        repo_path: str,
        provider: str = "langchain-anthropic",
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        max_turns: int = 25,
        verbose: bool = True,
    ):
        if not LANGCHAIN_AVAILABLE:
            raise ImportError(
                "LangChain not installed. Run:\n"
                "  pip install langchain langchain-core langchain-anthropic langchain-openai"
            )

        self.repo_path = repo_path
        self.repo_tools = RepoTools(repo_path)
        self.max_turns = max_turns
        self.verbose = verbose

        if provider == "langchain-anthropic":
            if not LANGCHAIN_ANTHROPIC_AVAILABLE:
                raise ImportError("pip install langchain-anthropic")
            key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
            self.model = model or "claude-sonnet-4-20250514"
            self.llm = ChatAnthropic(model=self.model, api_key=key, max_tokens=8096)
            self.provider = provider

        elif provider == "langchain-openai":
            if not LANGCHAIN_OPENAI_AVAILABLE:
                raise ImportError("pip install langchain-openai")
            key = api_key or os.environ.get("OPENAI_API_KEY", "")
            self.model = model or "gpt-4o"
            self.llm = ChatOpenAI(model=self.model, api_key=key, temperature=0.2)
            self.provider = provider

        elif provider == "langchain-gemini":
            if not LANGCHAIN_GEMINI_AVAILABLE:
                raise ImportError("pip install langchain-google-genai")
            key = api_key or os.environ.get("GOOGLE_API_KEY", "")
            self.model = model or "gemini-3-flash-preview"
            self.llm = ChatGoogleGenerativeAI(model=self.model, google_api_key=key)
            self.provider = provider

        else:
            raise ValueError(f"Unsupported LangChain provider: {provider}")

        self.tools = _build_langchain_tools(self.repo_tools)

    def run_agent(self, system_prompt: str, user_prompt: str) -> str:
        """
        Run the LangChain agent loop.

        Uses create_agent which creates a LangGraph-based agent that
        calls tools in a loop until the task is complete.
        """
        agent = create_agent(
            model=self.llm,
            tools=self.tools,
            system_prompt=system_prompt,
        )

        logger.info(f"  LangChain agent starting ({self.provider}, model={self.model})")

        result = agent.invoke(
            {"messages": [HumanMessage(content=user_prompt)]},
            config={"recursion_limit": self.max_turns * 2},
        )

        messages = result.get("messages", [])
        tool_call_count = sum(
            1 for m in messages if hasattr(m, "type") and m.type == "tool"
        )
        logger.info(f"  LangChain agent completed ({tool_call_count} tool calls)")

        for m in messages:
            if hasattr(m, "type") and m.type == "tool":
                logger.info(f"    Tool: {m.name}")

        final_message = messages[-1] if messages else None
        if final_message and hasattr(final_message, "content"):
            return final_message.content if isinstance(final_message.content, str) else str(final_message.content)

        return ""
