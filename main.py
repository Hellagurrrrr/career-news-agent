import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

import db
import notion_sync
from crawler import map_sources_from_yaml, scrape_links_to_table
from logger import pipeline_logger
from raw_process_agent import raw_process_agent
from settings.config import (
    LLM_MAX_WORKERS,
    MAP_MAX_WORKERS,
    MAX_LINKS,
    NOTION_SYNC_ENABLED,
)


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


def _dedup_against_db(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Hash every scraped row and drop the ones already present in the DB.

    Mutates each row in-place to attach ``hash_code``; the agent stage
    later carries that hash through to the final record so we can persist
    it without recomputing.
    """
    if not rows:
        return []
    for row in rows:
        row["hash_code"] = db.compute_hash(row["article_url"])

    already = db.existing_hashes(row["hash_code"] for row in rows)
    new_rows = [r for r in rows if r["hash_code"] not in already]

    total = len(rows)
    skipped = total - len(new_rows)
    print(
        f"\n[dedup] {total} scraped | {skipped} already in DB | "
        f"{len(new_rows)} new -> agent"
    )
    return new_rows


async def _process_one(
    row: dict[str, Any],
    sem: asyncio.Semaphore,
) -> dict[str, Any]:
    """Run raw_process_agent on one row; never raise.

    The returned record is the article's full AgentState plus the
    pipeline-managed metadata that the agent itself does not carry
    (source_url landing page, hash_code). This is the canonical row
    shape consumed by db.insert_article().
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
        "hash_code": row["hash_code"],
    }


async def _process_all(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Run the agent on every row in parallel, capped by LLM_MAX_WORKERS.

    Each completed record is inserted into the DB immediately, so a crash
    later in the run never loses already-processed articles.
    """
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

        # Persist locally first -- this is the source of truth. We catch
        # broadly so one bad row never aborts the batch.
        try:
            unique_id = db.insert_article(record)
            record["unique_id"] = unique_id
            record["status"] = db.ArticleStatus.REVIEWING.value
            # Mirror the timestamp insert_article() just stamped onto the
            # row, so the Notion push below has a value to write into the
            # `created_at` column. Sub-second drift is harmless.
            record["created_at"] = datetime.now().isoformat(timespec="seconds")
            print(f"[db] inserted {unique_id} status=REVIEWING")
        except Exception as exc:
            print(f"[db insert failed] {url}: {exc}")
            records.append(record)
            continue

        # Mirror to Notion. push_article never raises -- it logs and
        # returns None on failure, so a Notion outage cannot break the
        # local pipeline. Pushed off the event loop because the SDK is
        # synchronous (~200-500ms per call).
        if NOTION_SYNC_ENABLED:
            await asyncio.to_thread(notion_sync.push_article, record)

        records.append(record)
    return records


async def main_async() -> None:
    run_output_dir = Path("output") / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_output_dir.mkdir(parents=True, exist_ok=True)

    # Idempotent: creates table+indexes on first run, no-op afterwards.
    db.init_db()

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

    # Hash + dedup BEFORE the agent so we never spend tokens on articles
    # we have already processed.
    rows = df.to_dict("records")
    new_rows = _dedup_against_db(rows)

    records = await _process_all(new_rows)

    # Per-run snapshot of newly-processed articles. The DB is the canonical
    # store; this JSON makes it easy to eyeball one run's output without
    # opening sqlite.
    articles_path = run_output_dir / "articles.json"
    articles_path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nSaved {len(records)} new articles to: {articles_path}")

    # Per-stage timings + token usage for this run. Printed for quick
    # inspection and also persisted next to articles.json.
    pipeline_logger.print_summary()
    metrics_path = pipeline_logger.save(run_output_dir / "metrics.json")
    print(f"Saved pipeline metrics to: {metrics_path}")


if __name__ == "__main__":
    asyncio.run(main_async())
