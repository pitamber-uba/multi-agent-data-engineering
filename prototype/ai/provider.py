"""
AI Provider — Wraps LLM APIs with tool-calling for the agent loop.

Supports Anthropic Claude, OpenAI GPT, Google Gemini (direct and via LangChain).

The pattern is the same across all providers:
1. Send a system prompt + user prompt to the LLM
2. LLM decides which tools to call (read files, write files, run commands)
3. We execute those tool calls and return results
4. LLM continues reasoning and calling tools until the task is done
5. LLM returns its final answer
"""

import json
import logging
import os
import re
import time
from typing import Optional

from .tools import TOOL_DEFINITIONS, RepoTools

logger = logging.getLogger("ai.provider")

try:
    import anthropic

    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    import openai

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    from google import genai
    from google.genai import types as genai_types

    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False


class AIProvider:
    """
    Runs an AI agent loop: sends a prompt with tools, executes tool calls,
    and iterates until the AI completes the task.

    Supports:
    - anthropic: Direct Anthropic Claude API (matches Cursor)
    - openai: Direct OpenAI API
    - gemini: Direct Google Gemini API
    - langchain-anthropic: Claude via LangChain framework
    - langchain-openai: OpenAI via LangChain framework
    - langchain-gemini: Gemini via LangChain framework
    """

    LANGCHAIN_PROVIDERS = ("langchain-anthropic", "langchain-openai", "langchain-gemini")

    def __init__(
        self,
        repo_path: str,
        provider: str = "anthropic",
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        max_turns: int = 25,
    ):
        self.repo_path = repo_path
        self.provider = provider
        self.max_turns = max_turns

        if provider in self.LANGCHAIN_PROVIDERS:
            from .langchain_provider import LangChainProvider

            self._langchain = LangChainProvider(
                repo_path=repo_path,
                provider=provider,
                model=model,
                api_key=api_key,
                max_turns=max_turns,
            )
            self.model = self._langchain.model
            self.tools = None
            self.client = None
        else:
            self._langchain = None
            self.tools = RepoTools(repo_path)

            if provider == "anthropic":
                if not ANTHROPIC_AVAILABLE:
                    raise ImportError("pip install anthropic")
                self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
                self.model = model or "claude-sonnet-4-20250514"
                self.client = anthropic.Anthropic(api_key=self.api_key)
            elif provider == "openai":
                if not OPENAI_AVAILABLE:
                    raise ImportError("pip install openai")
                self.api_key = api_key or os.environ.get("OPENAI_API_KEY", "")
                self.model = model or "gpt-4o"
                self.client = openai.OpenAI(api_key=self.api_key)
            elif provider == "gemini":
                if not GEMINI_AVAILABLE:
                    raise ImportError("pip install google-genai")
                self.api_key = api_key or os.environ.get("GOOGLE_API_KEY", "")
                self.model = model or "gemini-2.5-flash"
                self.client = genai.Client(api_key=self.api_key)
            else:
                raise ValueError(f"Unsupported provider: {provider}")

    def run_agent(self, system_prompt: str, user_prompt: str) -> str:
        """
        Run the full agent loop: prompt → tool calls → iterate → final answer.
        This is the core of how Cursor AI works.
        """
        if self._langchain:
            return self._langchain.run_agent(system_prompt, user_prompt)
        elif self.provider == "anthropic":
            return self._run_anthropic(system_prompt, user_prompt)
        elif self.provider == "openai":
            return self._run_openai(system_prompt, user_prompt)
        elif self.provider == "gemini":
            return self._run_gemini(system_prompt, user_prompt)

    def _run_anthropic(self, system_prompt: str, user_prompt: str) -> str:
        messages = [{"role": "user", "content": user_prompt}]

        for turn in range(self.max_turns):
            logger.info(f"  AI turn {turn + 1}/{self.max_turns}")

            response = self.client.messages.create(
                model=self.model,
                max_tokens=8096,
                system=system_prompt,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            )

            if response.stop_reason == "end_turn":
                text_parts = [b.text for b in response.content if b.type == "text"]
                final = "\n".join(text_parts)
                logger.info(f"  AI completed in {turn + 1} turns")
                return final

            tool_calls = [b for b in response.content if b.type == "tool_use"]
            if not tool_calls:
                text_parts = [b.text for b in response.content if b.type == "text"]
                return "\n".join(text_parts)

            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for tc in tool_calls:
                logger.info(f"    Tool: {tc.name}({_summarize_input(tc.input)})")
                result = self.tools.execute_tool(tc.name, tc.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": result,
                })

            messages.append({"role": "user", "content": tool_results})

        return "Error: AI agent exceeded maximum turns"

    def _run_openai(self, system_prompt: str, user_prompt: str) -> str:
        openai_tools = [
            {
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t["description"],
                    "parameters": t["input_schema"],
                },
            }
            for t in TOOL_DEFINITIONS
        ]

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        for turn in range(self.max_turns):
            logger.info(f"  AI turn {turn + 1}/{self.max_turns}")

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=openai_tools,
                temperature=0.2,
            )

            choice = response.choices[0]

            if choice.finish_reason == "stop":
                logger.info(f"  AI completed in {turn + 1} turns")
                return choice.message.content or ""

            if choice.finish_reason == "tool_calls":
                messages.append(choice.message)

                for tc in choice.message.tool_calls:
                    args = json.loads(tc.function.arguments)
                    logger.info(f"    Tool: {tc.function.name}({_summarize_input(args)})")
                    result = self.tools.execute_tool(tc.function.name, args)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result,
                    })
            else:
                return choice.message.content or ""

        return "Error: AI agent exceeded maximum turns"

    def _run_gemini(self, system_prompt: str, user_prompt: str) -> str:
        gemini_tools = genai_types.Tool(function_declarations=[
            {
                "name": t["name"],
                "description": t["description"],
                "parameters": t["input_schema"],
            }
            for t in TOOL_DEFINITIONS
        ])

        config = genai_types.GenerateContentConfig(
            system_instruction=system_prompt,
            tools=[gemini_tools],
            temperature=0.2,
        )

        contents = [genai_types.Content(
            role="user",
            parts=[genai_types.Part.from_text(text=user_prompt)],
        )]

        for turn in range(self.max_turns):
            logger.info(f"  AI turn {turn + 1}/{self.max_turns}")

            response = self._gemini_call_with_retry(contents, config)

            candidate = response.candidates[0]
            parts = candidate.content.parts

            function_calls = [p for p in parts if p.function_call]
            if not function_calls:
                text_parts = [p.text for p in parts if p.text]
                logger.info(f"  AI completed in {turn + 1} turns")
                return "\n".join(text_parts)

            contents.append(candidate.content)

            tool_result_parts = []
            for p in function_calls:
                fc = p.function_call
                args = dict(fc.args) if fc.args else {}
                logger.info(f"    Tool: {fc.name}({_summarize_input(args)})")
                result = self.tools.execute_tool(fc.name, args)
                tool_result_parts.append(genai_types.Part.from_function_response(
                    name=fc.name,
                    response={"result": result},
                ))

            contents.append(genai_types.Content(
                role="user",
                parts=tool_result_parts,
            ))

        return "Error: AI agent exceeded maximum turns"

    def _gemini_call_with_retry(self, contents, config, max_retries: int = 8):
        """Call Gemini API with automatic retry on rate-limit and transient errors."""
        retryable = ("429", "503", "RESOURCE_EXHAUSTED", "UNAVAILABLE")
        for attempt in range(max_retries):
            try:
                return self.client.models.generate_content(
                    model=self.model,
                    contents=contents,
                    config=config,
                )
            except Exception as e:
                error_str = str(e)
                if not any(code in error_str for code in retryable):
                    raise

                retry_match = re.search(r"retry in (\d+\.?\d*)", error_str, re.IGNORECASE)
                if retry_match:
                    wait_secs = float(retry_match.group(1)) + 2
                else:
                    wait_secs = min(15 * (2 ** attempt), 120)

                logger.warning(
                    f"  Gemini transient error. Waiting {wait_secs:.0f}s before retry "
                    f"({attempt + 1}/{max_retries})..."
                )
                time.sleep(wait_secs)

        raise RuntimeError("Gemini API: max retries exhausted")


def is_ai_available() -> bool:
    """Check if any AI provider is configured and available."""
    if ANTHROPIC_AVAILABLE and os.environ.get("ANTHROPIC_API_KEY"):
        return True
    if OPENAI_AVAILABLE and os.environ.get("OPENAI_API_KEY"):
        return True
    return False


def get_default_provider() -> Optional[str]:
    """Determine which AI provider to use based on available keys."""
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.environ.get("OPENAI_API_KEY"):
        return "openai"
    return None


ALL_PROVIDERS = [
    "anthropic", "openai", "gemini",
    "langchain-anthropic", "langchain-openai", "langchain-gemini",
]

PROVIDER_ENV_KEYS = {
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "gemini": "GOOGLE_API_KEY",
    "langchain-anthropic": "ANTHROPIC_API_KEY",
    "langchain-openai": "OPENAI_API_KEY",
    "langchain-gemini": "GOOGLE_API_KEY",
}


def _summarize_input(inp: dict) -> str:
    """Short summary of tool input for logging."""
    if "path" in inp and "content" not in inp:
        return inp["path"]
    if "path" in inp and "content" in inp:
        return f"{inp['path']} ({len(inp['content'])} chars)"
    if "command" in inp:
        cmd = inp["command"]
        return cmd[:60] + "..." if len(cmd) > 60 else cmd
    if "pattern" in inp:
        return inp["pattern"]
    return str(inp)[:60]
