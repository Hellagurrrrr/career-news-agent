import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dotenv import load_dotenv
from firecrawl import Firecrawl

from logger import pipeline_logger
from settings.config import (
    FIRECRAWL_API_KEY,
    MAP_MAX_WORKERS,
    SCRAPE_MAX_WORKERS,
)

load_dotenv()

firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)

# Asset/file URLs we never want to send to /scrape regardless of source.
_NON_ARTICLE_EXT = re.compile(
    r"\.(pdf|jpe?g|png|gif|svg|webp|mp4|webm|mp3|zip)(?:\?.*)?$",
    re.IGNORECASE,
)


def _filter_article_urls(
    links: list,
    source_url: str,
    url_pattern: str | None,
) -> list[str]:
    """Apply per-source filters to a Firecrawl /map result.

    Drops:
        - the source landing page itself (e.g. .../insights)
        - common asset URLs (.pdf/.jpg/.mp4/...)
        - anything that does not match `url_pattern` (when provided)
    """
    pattern = re.compile(url_pattern) if url_pattern else None
    source_norm = source_url.rstrip("/")

    urls: list[str] = []
    for link in links:
        url = getattr(link, "url", None)
        if not url:
            continue
        if url.rstrip("/") == source_norm:
            continue
        if _NON_ARTICLE_EXT.search(url):
            continue
        if pattern and not pattern.search(url):
            continue
        urls.append(url)
    return urls


def map_sources_from_yaml(
    yaml_path: str | Path = "sources/source.yaml",
    limit: int = 5,
    max_workers: int = MAP_MAX_WORKERS,
) -> list[dict[str, Any]]:
    """Load every source from a YAML file and call firecrawl.map() on each url.

    Returns:
        A list of dicts, one per source, shaped like::

            {
                "source": {
                    "name":         str,
                    "url":          str,   # landing page from YAML
                    "url_pattern":  str | None,
                },
                "links": list[str],  # already filtered by url_pattern + ext
            }

        Sources missing a url or whose map() call fails are skipped.
        Each entry can be passed directly to ``scrape_links_to_table``.

        firecrawl.map() calls are dispatched concurrently across sources.
    """
    yaml_path = Path(yaml_path)
    with yaml_path.open("r", encoding="utf-8") as f:
        sources = yaml.safe_load(f) or []

    valid_sources: list[dict[str, Any]] = []
    for src in sources:
        url = src.get("url")
        if not url:
            print(f"[skip] source is missing the url field: {src}")
            continue
        valid_sources.append({
            "name": src.get("name", ""),
            "url": url,
            "url_pattern": src.get("url_pattern"),
        })

    if not valid_sources:
        return []

    def _map_one(src: dict[str, Any]) -> dict[str, Any] | None:
        name = src["name"]
        url = src["url"]
        url_pattern = src["url_pattern"]

        print(f"[mapping] {name}: {url}")
        try:
            with pipeline_logger.time_block("map"):
                r = firecrawl.map(url=url, limit=limit)
        except Exception as exc:
            print(f"[map failed] {url}: {exc}")
            return None

        links = _filter_article_urls(r.links, url, url_pattern)
        print(
            f"[mapped] {name}: discovered {len(r.links)} links, "
            f"kept {len(links)} after filter"
        )
        return {
            "source": {
                "name": name,
                "url": url,
                "url_pattern": url_pattern,
            },
            "links": links,
        }

    workers = max(1, min(max_workers, len(valid_sources)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return [r for r in ex.map(_map_one, valid_sources) if r is not None]


def scrape_links_to_table(
    map_entry: dict[str, Any],
    max_workers: int = SCRAPE_MAX_WORKERS,
) -> pd.DataFrame:
    """Scrape every filtered link for one source and return a tabular result.

    Args:
        map_entry: One element produced by ``map_sources_from_yaml``, shaped
            as ``{"source": {...}, "links": [url, ...]}``.
        max_workers: Size of the thread pool used to fan out /scrape calls.

    Returns:
        DataFrame with the columns:
            - source_name : Display name from YAML.
            - source_url  : Landing-page URL from YAML.
            - article_url : The scraped page URL.
            - markdown    : Markdown body from Firecrawl (None on failure).

        Per-link scrape failures do not abort the batch -- they are recorded
        with ``markdown=None`` and filtered out by the caller.
    """
    source = map_entry["source"]
    links = map_entry["links"]

    columns = [
        "source_name",
        "source_url",
        "article_url",
        "markdown",
    ]

    if not links:
        return pd.DataFrame([], columns=columns)

    def _scrape_one(url: str) -> dict[str, Any]:
        try:
            with pipeline_logger.time_block("scrape"):
                doc = firecrawl.scrape(url=url)
            markdown = getattr(doc, "markdown", None)
        except Exception as exc:
            print(f"[scrape failed] {url}: {exc}")
            markdown = None
        print(f"[scraped] {url} ({len(markdown) if markdown else 0} chars)")
        return {
            "source_name": source["name"],
            "source_url":  source["url"],
            "article_url": url,
            "markdown":    markdown,
        }

    workers = max(1, min(max_workers, len(links)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        rows = list(ex.map(_scrape_one, links))

    return pd.DataFrame(rows, columns=columns)
