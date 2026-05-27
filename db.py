"""SQLite persistence for processed articles.

The schema mirrors the AgentState produced by raw_process_agent, plus
three pipeline-managed columns:
    - unique_id : UUID4 hex, the row's primary key.
    - hash_code : stable SHA256 of the article URL; used to dedup re-scrapes
                  BEFORE we spend LLM tokens.
    - status    : workflow state. The pipeline always writes REVIEWING; an
                  admin moves the row to READY / PUBLISHED / DISCARD later.

Connection model
----------------
One process-wide sqlite3.Connection. SQLite is happy with this as long as
the calls happen from the same thread, which matches our asyncio main
thread. We pass ``check_same_thread=False`` so it stays safe if a future
caller pushes inserts onto ``asyncio.to_thread``.
"""

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterable

from settings.config import DB_PATH


class ArticleStatus(str, Enum):
    """Lifecycle states for an article row.

    REVIEWING: default after pipeline insert -- waiting for admin review.
    READY:     admin marked it as good; ready to publish.
    PUBLISHED: live on the community site.
    DISCARD:   admin marked it as unusable.
    """

    REVIEWING = "REVIEWING"
    READY = "READY"
    PUBLISHED = "PUBLISHED"
    DISCARD = "DISCARD"


class ArticleTag(str, Enum):
    """Content tag for an article row.

    The pipeline (raw_process_agent.generate_tag) chooses one of these per
    article; admins can later override the value via update_tag().

    MARKET: macro/meta-level industry trend observations on the recruitment
            market (e.g. tech giants' Q2 hiring pace, hedge-fund campus
            recruiting cadence).
    MENTOR: first-person know-how from current industry mentors (e.g. a
            Goldman IBD analyst's letter, a McKinsey EM's playbook).
    REPORT: in-house structured data reports emphasizing sample size and
            research dimensions (e.g. the 2026 International Student Job
            Seeking White Paper).
    VISA:   policy / compliance content (e.g. new H-1B lottery rules).
    CITY:   life and job-seeking guides centered on a single work city
            (e.g. London, New York), focused on lifestyle / local info.
    """

    MARKET = "MARKET"
    MENTOR = "MENTOR"
    REPORT = "REPORT"
    VISA = "VISA"
    CITY = "CITY"


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS articles (
    unique_id       TEXT PRIMARY KEY,
    hash_code       TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'REVIEWING'
                       CHECK(status IN ('REVIEWING','READY','PUBLISHED','DISCARD')),

    article_url     TEXT NOT NULL,
    source_name     TEXT,
    source_url      TEXT,
    scraped_at      TEXT,
    raw_markdown    TEXT,

    title_zh        TEXT,
    title_en        TEXT,
    excerpt_zh      TEXT,
    excerpt_en      TEXT,
    author          TEXT,
    country         TEXT,
    published_at    TEXT,
    read_minutes    INTEGER,
    notes           TEXT,

    tag             TEXT
                       CHECK(tag IS NULL OR tag IN ('MARKET','MENTOR','REPORT','VISA','CITY')),
    tag_reason      TEXT,

    relevance_score INTEGER,
    quality_score   INTEGER,
    overall_score   INTEGER,
    reason          TEXT,

    needs_revision  INTEGER,
    review_notes    TEXT,

    stage_errors    TEXT,

    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
"""

_CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_articles_hash   ON articles(hash_code);",
    "CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status);",
    "CREATE INDEX IF NOT EXISTS idx_articles_tag    ON articles(tag);",
    "CREATE INDEX IF NOT EXISTS idx_articles_url    ON articles(article_url);",
]

# Columns copied straight from a pipeline record into the row. Pipeline-
# managed columns (unique_id, hash_code, status, stage_errors, created_at,
# updated_at) are handled separately inside insert_article().
_COPY_COLUMNS: tuple[str, ...] = (
    "article_url",
    "source_name",
    "source_url",
    "scraped_at",
    "raw_markdown",
    "title_zh",
    "title_en",
    "excerpt_zh",
    "excerpt_en",
    "author",
    "country",
    "published_at",
    "read_minutes",
    "notes",
    "tag",
    "tag_reason",
    "relevance_score",
    "quality_score",
    "overall_score",
    "reason",
    "needs_revision",
    "review_notes",
)


_conn: sqlite3.Connection | None = None


def _get_conn() -> sqlite3.Connection:
    """Return the lazily-initialized process-wide SQLite connection."""
    global _conn
    if _conn is None:
        path = Path(DB_PATH)
        if path.parent and str(path.parent) not in ("", "."):
            path.parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(path, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        # WAL gives us concurrent reads even while a write is in flight.
        _conn.execute("PRAGMA journal_mode=WAL;")
        _conn.execute("PRAGMA foreign_keys=ON;")
    return _conn


def init_db() -> None:
    """Create the articles table + indexes if they don't exist (idempotent)."""
    conn = _get_conn()
    with conn:
        conn.execute(_CREATE_TABLE_SQL)
        for stmt in _CREATE_INDEX_SQL:
            conn.execute(stmt)


def compute_hash(article_url: str) -> str:
    """Stable SHA256 hex over a normalized article URL.

    URL is the cheapest, most reliable identity for v1: same article ->
    same URL -> same hash. If publisher-side URL churn becomes a real
    problem, swap this for a content-aware hash without altering the
    table schema (only the column value changes).
    """
    norm = (article_url or "").strip().rstrip("/")
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def existing_hashes(hashes: Iterable[str]) -> set[str]:
    """Return the subset of ``hashes`` that already exist in the DB.

    Chunks the query at 500 placeholders to stay well below SQLite's
    default 999-parameter limit, so the caller can pass arbitrarily large
    iterables without worrying.
    """
    hash_list = [h for h in hashes if h]
    if not hash_list:
        return set()

    conn = _get_conn()
    found: set[str] = set()
    chunk_size = 500
    for i in range(0, len(hash_list), chunk_size):
        sub = hash_list[i : i + chunk_size]
        placeholders = ",".join("?" for _ in sub)
        rows = conn.execute(
            f"SELECT hash_code FROM articles WHERE hash_code IN ({placeholders})",
            sub,
        ).fetchall()
        found.update(r["hash_code"] for r in rows)
    return found


def insert_article(record: dict[str, Any]) -> str:
    """Insert one pipeline record and return its freshly assigned unique_id.

    The record is expected to already carry:
        - hash_code   : produced by compute_hash() before the LLM stage
        - the agent's full final state (see raw_process_agent.AgentState)
        - source_url  : the YAML landing page URL (attached by main.py)

    Pipeline-managed columns are filled in here:
        unique_id    = uuid4 hex
        status       = REVIEWING (default; admin can move it later)
        created_at   = updated_at = ISO timestamp at insert time
        stage_errors = json.dumps(record.get("stage_errors") or [])
    """
    hash_code = record.get("hash_code")
    if not hash_code:
        raise ValueError("insert_article requires record['hash_code']")

    now = datetime.now().isoformat(timespec="seconds")
    unique_id = uuid.uuid4().hex

    values: dict[str, Any] = {
        "unique_id": unique_id,
        "hash_code": hash_code,
        "status": ArticleStatus.REVIEWING.value,
        "created_at": now,
        "updated_at": now,
    }
    for col in _COPY_COLUMNS:
        values[col] = record.get(col)

    # SQLite has no list/JSON column type; serialize stage_errors as text.
    stage_errors = record.get("stage_errors") or []
    values["stage_errors"] = json.dumps(stage_errors, ensure_ascii=False)

    columns_sql = ",".join(values.keys())
    placeholders_sql = ",".join(f":{c}" for c in values.keys())
    sql = f"INSERT INTO articles ({columns_sql}) VALUES ({placeholders_sql})"

    conn = _get_conn()
    with conn:
        conn.execute(sql, values)
    return unique_id


def update_status(unique_id: str, status: ArticleStatus | str) -> None:
    """Admin op: move an article through the workflow.

    Accepts either an ``ArticleStatus`` or the raw string; the latter is
    validated against the enum so callers cannot smuggle in arbitrary
    values that would still pass the column-level CHECK constraint by
    accident.
    """
    if isinstance(status, str):
        status = ArticleStatus(status)
    now = datetime.now().isoformat(timespec="seconds")
    conn = _get_conn()
    with conn:
        conn.execute(
            "UPDATE articles SET status = ?, updated_at = ? WHERE unique_id = ?",
            (status.value, now, unique_id),
        )


def update_tag(unique_id: str, tag: ArticleTag | str | None) -> None:
    """Admin op: re-tag an article (e.g. correcting the LLM's choice).

    Accepts an ``ArticleTag``, the raw string (validated against the enum
    so we don't bypass safety by hitting only the CHECK constraint), or
    ``None`` to clear the tag.
    """
    if tag is None:
        value: str | None = None
    else:
        if isinstance(tag, str):
            tag = ArticleTag(tag)
        value = tag.value
    now = datetime.now().isoformat(timespec="seconds")
    conn = _get_conn()
    with conn:
        conn.execute(
            "UPDATE articles SET tag = ?, updated_at = ? WHERE unique_id = ?",
            (value, now, unique_id),
        )
