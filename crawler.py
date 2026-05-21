import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dotenv import load_dotenv
from firecrawl import Firecrawl

from settings.config import FIRECRAWL_API_KEY

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
) -> list[dict[str, Any]]:
    """Load every source from a YAML file and call firecrawl.map() on each url.

    Returns:
        A list of dicts, one per source, shaped like::

            {
                "source": {
                    "name":         str,
                    "url":          str,   # landing page from YAML
                    "default_tag":  str,
                    "url_pattern":  str | None,
                },
                "links": list[str],  # already filtered by url_pattern + ext
            }

        Sources missing a url or whose map() call fails are skipped.
        Each entry can be passed directly to ``scrape_links_to_table``.
    """
    yaml_path = Path(yaml_path)
    with yaml_path.open("r", encoding="utf-8") as f:
        sources = yaml.safe_load(f) or []

    results: list[dict[str, Any]] = []
    for src in sources:
        name = src.get("name", "")
        url = src.get("url")
        default_tag = src.get("default_tag", "")
        url_pattern = src.get("url_pattern")
        if not url:
            print(f"[skip] source is missing the url field: {src}")
            continue

        print(f"[mapping] {name}: {url}")
        try:
            r = firecrawl.map(url=url, limit=limit)
        except Exception as exc:
            print(f"[map failed] {url}: {exc}")
            continue

        links = _filter_article_urls(r.links, url, url_pattern)
        print(
            f"  discovered {len(r.links)} links, kept {len(links)} after filter"
        )
        results.append({
            "source": {
                "name": name,
                "url": url,
                "default_tag": default_tag,
                "url_pattern": url_pattern,
            },
            "links": links,
        })

    return results


def scrape_links_to_table(map_entry: dict[str, Any]) -> pd.DataFrame:
    """Scrape every filtered link for one source and return a tabular result.

    Args:
        map_entry: One element produced by ``map_sources_from_yaml``, shaped
            as ``{"source": {...}, "links": [url, ...]}``.

    Returns:
        DataFrame with the columns:
            - source_name : Display name from YAML.
            - source_url  : Landing-page URL from YAML.
            - default_tag : Fallback tag from YAML (used by the LLM stage).
            - article_url : The scraped page URL.
            - markdown    : Markdown body from Firecrawl (None on failure).
    """
    source = map_entry["source"]
    links = map_entry["links"]

    rows: list[dict[str, Any]] = []
    for url in links:
        try:
            doc = firecrawl.scrape(url=url)
            markdown = getattr(doc, "markdown", None)
        except Exception as exc:
            # Per-link failures should not abort the batch; record None so
            # callers can filter the resulting DataFrame later.
            print(f"[scrape failed] {url}: {exc}")
            markdown = None
        rows.append({
            "source_name": source["name"],
            "source_url":  source["url"],
            "default_tag": source["default_tag"],
            "article_url": url,
            "markdown":    markdown,
        })
        print(f"[scraped] {url} ({len(markdown) if markdown else 0} chars)")

    return pd.DataFrame(
        rows,
        columns=[
            "source_name",
            "source_url",
            "default_tag",
            "article_url",
            "markdown",
        ],
    )
