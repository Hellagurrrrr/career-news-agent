import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from settings.config import (
    LLM_MAX_WORKERS,
    MAP_MAX_WORKERS,
    MAX_LINKS,
)
from crawler import map_sources_from_yaml, scrape_links_to_table
from logger import pipeline_logger
from raw_process_agent import raw_process_agent


def _scrape_all_sources(map_results: list[dict[str, Any]]) -> list[pd.DataFrame]:
    """Fan out scrape_links_to_table across sources, bounded by MAP_MAX_WORKERS.

    Total in-flight Firecrawl /scrape calls stay bounded at
    MAP_MAX_WORKERS * SCRAPE_MAX_WORKERS.
    """
    if not map_results:
        return []
    outer_workers = max(1, min(MAP_MAX_WORKERS, len(map_results)))
    with ThreadPoolExecutor(max_workers=outer_workers) as ex:
        return list(ex.map(scrape_links_to_table, map_results))


async def _process_one(
    row: dict[str, Any],
    sem: asyncio.Semaphore,
) -> dict[str, Any]:
    """Run raw_process_agent on one row; never raise.

    The returned record is the article's full AgentState plus the YAML-side
    metadata (source_url landing page) that the agent does not carry. This
    is the canonical row shape for the eventual DB table.
    """
    initial_state: dict[str, Any] = {
        "article_url": row["article_url"],
        "source_name": row.get("source_name", ""),
        "raw_markdown": row["markdown"],
        "scraped_at": datetime.now().isoformat(timespec="seconds"),
    }

    async with sem:
        try:
            final_state: dict[str, Any] = await raw_process_agent.ainvoke(initial_state)
        except Exception as exc:
            # Each node catches its own LLM errors into stage_errors. This
            # branch is the last-resort net for things like graph wiring bugs
            # or OOM: we still record the article so it isn't silently lost.
            final_state = {
                **initial_state,
                "stage_errors": [{"node": "ainvoke", "error": str(exc)}],
            }

    return {
        **final_state,
        "source_url": row.get("source_url", ""),
    }


async def _process_all(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Run the agent on every row in parallel, capped by LLM_MAX_WORKERS."""
    if not rows:
        return []

    sem = asyncio.Semaphore(max(1, LLM_MAX_WORKERS))
    tasks = [asyncio.create_task(_process_one(row, sem)) for row in rows]

    records: list[dict[str, Any]] = []
    total = len(tasks)
    for idx, fut in enumerate(asyncio.as_completed(tasks), start=1):
        record = await fut
        url = record.get("article_url", "<unknown>")
        errs = record.get("stage_errors") or []
        if errs:
            err_nodes = ",".join(e.get("node", "?") for e in errs)
            print(f"[done {idx}/{total}] {url} -> errors=[{err_nodes}]")
        else:
            overall = record.get("overall_score", "?")
            needs_rev = record.get("needs_revision", "?")
            print(
                f"[done {idx}/{total}] {url} -> overall={overall} "
                f"needs_revision={needs_rev}"
            )
        records.append(record)
    return records


async def main_async() -> None:
    run_output_dir = Path("output") / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_output_dir.mkdir(parents=True, exist_ok=True)

    # YAML -> map_sources_from_yaml -> scrape_links_to_table.
    # Each source fires up to `limit` /scrape calls -- mind the Firecrawl quota.
    map_results = map_sources_from_yaml(limit=MAX_LINKS)
    dfs = _scrape_all_sources(map_results)

    df = (
        pd.concat(dfs, ignore_index=True)
        if dfs
        else pd.DataFrame(columns=[
            "source_name",
            "source_url",
            "article_url",
            "markdown",
        ])
    )

    # Drop rows whose scrape failed or returned an empty body so the LLM
    # stage never sees a None/"" markdown.
    df = df.dropna(subset=["markdown"])
    df = df[df["markdown"].astype(str).str.len() > 0].reset_index(drop=True)

    print(f"\nfinal table shape: {df.shape}")
    print(df.head())

    rows = df.to_dict("records")
    records = await _process_all(rows)

    # Keep ALL articles -- no SCORE_THRESHOLD filtering -- so low-scoring
    # references can still be inspected and revisited downstream.
    articles_path = run_output_dir / "articles.json"
    articles_path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nSaved {len(records)} articles to: {articles_path}")

    # Per-stage timings + token usage for this run. Printed for quick
    # inspection and also persisted next to articles.json.
    pipeline_logger.print_summary()
    metrics_path = pipeline_logger.save(run_output_dir / "metrics.json")
    print(f"Saved pipeline metrics to: {metrics_path}")


if __name__ == "__main__":
    asyncio.run(main_async())
