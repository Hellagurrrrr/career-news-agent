"""Pipeline-level timing + token logger.

A single ``pipeline_logger`` instance collects, per named stage:
    - how many times the stage ran
    - total / average wall-clock time
    - prompt / completion / total token counts (LLM stages only)

The logger is thread-safe so the same instance can be shared across the
ThreadPoolExecutor used by Firecrawl /map and /scrape and the asyncio
tasks used by the LangGraph agent.

LLM token usage is captured via a ``BaseCallbackHandler`` attached to
each LLM call (see ``NodeTokenCallback``). Non-LLM stages (``map``,
``scrape``) only contribute timing.

Typical usage:

    # sync, in a worker thread
    with pipeline_logger.time_block("scrape"):
        firecrawl.scrape(url=url)

    # async, inside a LangGraph node
    async with pipeline_logger.track_llm("extract") as cbs:
        result = await model.ainvoke(messages, config={**config, "callbacks": cbs})

    # at the end of the run
    pipeline_logger.print_summary()
    pipeline_logger.save(run_output_dir / "metrics.json")
"""

from __future__ import annotations

import json
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Iterator

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult


# --------------------- Stats container ---------------------
@dataclass
class StageStats:
    count: int = 0
    total_time_sec: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0


def _extract_usage(response: LLMResult) -> tuple[int, int, int]:
    """Pull (input, output, total) token counts out of an LLMResult.

    Different chat-model integrations populate token usage in different
    places. We check (in order):
        1. response.llm_output["token_usage"]      -- OpenAI-style
        2. response.llm_output["usage"]            -- some providers
        3. generation.message.usage_metadata       -- new LangChain standard
    """
    llm_output = response.llm_output or {}

    for key in ("token_usage", "usage"):
        usage = llm_output.get(key)
        if isinstance(usage, dict):
            inp = int(
                usage.get("prompt_tokens")
                or usage.get("input_tokens")
                or 0
            )
            out = int(
                usage.get("completion_tokens")
                or usage.get("output_tokens")
                or 0
            )
            total = int(usage.get("total_tokens") or (inp + out))
            if inp or out or total:
                return inp, out, total

    # Fallback: walk the generations.
    inp = out = total = 0
    for gen_list in response.generations or []:
        for gen in gen_list:
            msg = getattr(gen, "message", None)
            meta = getattr(msg, "usage_metadata", None) if msg else None
            if isinstance(meta, dict):
                inp += int(meta.get("input_tokens") or 0)
                out += int(meta.get("output_tokens") or 0)
                total += int(meta.get("total_tokens") or 0)
    return inp, out, total or (inp + out)


# --------------------- Callback handler ---------------------
class NodeTokenCallback(BaseCallbackHandler):
    """LangChain callback that attributes LLM token usage to a node name."""

    def __init__(self, node_name: str, logger: "PipelineLogger") -> None:
        self.node_name = node_name
        self.logger = logger

    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        inp, out, total = _extract_usage(response)
        if inp or out or total:
            self.logger.record_tokens(self.node_name, inp, out, total)


# --------------------- Pipeline logger ---------------------
class PipelineLogger:
    """Thread-safe accumulator for per-stage timing + token usage."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stages: dict[str, StageStats] = {}
        self._started_at = time.perf_counter()

    def _slot(self, stage: str) -> StageStats:
        s = self._stages.get(stage)
        if s is None:
            s = StageStats()
            self._stages[stage] = s
        return s

    def record_time(self, stage: str, duration_sec: float) -> None:
        with self._lock:
            s = self._slot(stage)
            s.count += 1
            s.total_time_sec += duration_sec

    def record_tokens(
        self,
        stage: str,
        input_tokens: int,
        output_tokens: int,
        total_tokens: int | None = None,
    ) -> None:
        with self._lock:
            s = self._slot(stage)
            s.input_tokens += input_tokens
            s.output_tokens += output_tokens
            s.total_tokens += (
                total_tokens if total_tokens is not None
                else input_tokens + output_tokens
            )

    @contextmanager
    def time_block(self, stage: str) -> Iterator[None]:
        """Time a synchronous block of code under ``stage``."""
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.record_time(stage, time.perf_counter() - t0)

    @asynccontextmanager
    async def track_llm(
        self, stage: str
    ) -> AsyncIterator[list[BaseCallbackHandler]]:
        """Time an async LLM call and yield a callback list to attach.

        Pass the yielded list as ``config={"callbacks": cbs}`` so that
        ``NodeTokenCallback`` can capture token usage from the model run.
        """
        t0 = time.perf_counter()
        cbs: list[BaseCallbackHandler] = [NodeTokenCallback(stage, self)]
        try:
            yield cbs
        finally:
            self.record_time(stage, time.perf_counter() - t0)

    # --------------------- Reporting ---------------------
    def summary(self) -> dict[str, Any]:
        """Return a JSON-serializable snapshot of the current stats."""
        with self._lock:
            wall = time.perf_counter() - self._started_at
            stages: dict[str, dict[str, Any]] = {}
            grand_in = grand_out = grand_total = 0
            for name, s in self._stages.items():
                avg = (s.total_time_sec / s.count) if s.count else 0.0
                stages[name] = {
                    **asdict(s),
                    "total_time_sec": round(s.total_time_sec, 3),
                    "avg_time_sec": round(avg, 3),
                }
                grand_in += s.input_tokens
                grand_out += s.output_tokens
                grand_total += s.total_tokens

            return {
                "wall_clock_sec": round(wall, 3),
                "tokens": {
                    "input": grand_in,
                    "output": grand_out,
                    "total": grand_total,
                },
                "stages": stages,
            }

    def print_summary(self) -> None:
        snap = self.summary()
        print("\n=========== pipeline metrics ===========")
        print(f"wall clock     : {snap['wall_clock_sec']:.3f}s")
        tok = snap["tokens"]
        print(
            f"tokens (total) : in={tok['input']} out={tok['output']} "
            f"total={tok['total']}"
        )
        print(f"{'stage':<12} {'count':>6} {'time(s)':>10} {'avg(s)':>8} "
              f"{'in_tok':>8} {'out_tok':>8} {'tot_tok':>8}")
        for name, s in snap["stages"].items():
            print(
                f"{name:<12} {s['count']:>6d} {s['total_time_sec']:>10.3f} "
                f"{s['avg_time_sec']:>8.3f} {s['input_tokens']:>8d} "
                f"{s['output_tokens']:>8d} {s['total_tokens']:>8d}"
            )
        print("========================================\n")

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.summary(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return path

    def reset(self) -> None:
        with self._lock:
            self._stages.clear()
            self._started_at = time.perf_counter()


# Module-level singleton -- import this from the rest of the pipeline.
pipeline_logger = PipelineLogger()
