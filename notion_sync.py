"""Notion as a cloud admin UI for the articles pipeline.

The local SQLite store (db.py) is always the source of truth. This module
gives the pipeline two cloud-facing capabilities:

    push_article(record)
        Upsert one local record into the Notion database, keyed by
        ``unique_id``. Called from main.py right after db.insert_article.

    pull_status_changes(local_statuses)
        Walk the Notion database and return rows whose ``status`` differs
        from what we have locally. Called from sync_back.py.

If NOTION_TOKEN or NOTION_DB_ID is unset, every function in here turns
into a graceful no-op so the rest of the pipeline keeps working.

The Notion database is expected to have these properties (names must
match EXACTLY -- snake_case, see the README of Step A). Run this module
directly (``python notion_sync.py``) to compare your DB schema against
this expected set.

Notion data model (2025-09-03+): a "database" is a container that owns
one or more "data sources". Rows, properties, and queries all live on a
data source, not on the database. For single-source DBs (our case), we
auto-resolve the data source id from NOTION_DB_ID on first use; set
NOTION_DS_ID explicitly only if your DB has more than one source.
"""

import json
import time
from typing import Any

from notion_client import Client
from notion_client.errors import APIResponseError

from db import ArticleStatus
from settings.config import (
    NOTION_DB_ID,
    NOTION_DS_ID,
    NOTION_SYNC_ENABLED,
    NOTION_TOKEN,
)

# Notion limits each rich_text property value to 2000 chars. We leave a
# 100-char margin for the "..." ellipsis we append on truncation.
_RICH_TEXT_MAX = 1900

# Notion API throttles at ~3 req/sec. We pace inserts conservatively to
# avoid sporadic 429s when several articles finish back-to-back.
_API_PAUSE_S = 0.35

# Allowed values for the two Notion Select properties. We do client-side
# validation so an LLM hallucination cannot pollute the Notion option
# list (Notion auto-creates new select options on first sight).
_STATUS_WHITELIST: set[str] = {s.value for s in ArticleStatus}
_TAG_WHITELIST: set[str] = {"MARKET", "MENTOR", "REPORT", "VISA", "CITY"}

# Target the current stable Notion API. 2026-03-11 renamed `archived` to
# `in_trash` and tweaked block insertion semantics -- neither of which we
# touch -- and 2025-09-03 introduced the data_source split that the rest
# of this module is written against. See:
#   https://developers.notion.com/guides/get-started/upgrade-guide-2026-03-11
#   https://developers.notion.com/guides/get-started/upgrade-guide-2025-09-03
_NOTION_API_VERSION = "2026-03-11"

# The exact property names the Notion DB must expose. Used by the schema
# checker (run ``python notion_sync.py``) and is the canonical mapping
# document for the integration.
_EXPECTED_PROPERTIES: dict[str, str] = {
    "title_zh": "title",
    "title_en": "rich_text",
    "excerpt_zh": "rich_text",
    "excerpt_en": "rich_text",
    "unique_id": "rich_text",
    "hash_code": "rich_text",
    "status": "select",
    "tag": "select",
    "tag_reason": "rich_text",
    "article_url": "url",
    "source_url": "url",
    "source_name": "rich_text",
    "author": "rich_text",
    "country": "rich_text",
    "published_at": "date",
    "scraped_at": "date",
    "created_at": "date",
    "read_minutes": "number",
    "relevance_score": "number",
    "quality_score": "number",
    "overall_score": "number",
    "reason": "rich_text",
    "needs_revision": "checkbox",
    "review_notes": "rich_text",
    "notes": "rich_text",
    "stage_errors": "rich_text",
}

# Properties we tolerate in Notion but never write to. ``last_edited_time``
# is auto-managed by Notion and will reject any API write. Anything listed
# here is hidden from the "Extra properties" warning of the schema check.
_SYSTEM_MANAGED_PROPERTIES: set[str] = {"updated_at"}

_client: Client | None = None
_data_source_id: str | None = None


def _get_client() -> Client:
    """Lazily build a Notion client; raises if credentials are missing."""
    global _client
    if _client is None:
        if not NOTION_TOKEN:
            raise RuntimeError("NOTION_TOKEN is not set; cannot reach Notion API")
        _client = Client(auth=NOTION_TOKEN, notion_version=_NOTION_API_VERSION)
    return _client


def _get_data_source_id() -> str:
    """Resolve and cache the data_source_id backing NOTION_DB_ID.

    Resolution order:
        1. If NOTION_DS_ID is set in the environment, use it as-is. This
           is the escape hatch for multi-source databases where there's
           no single "right" data source to pick.
        2. Otherwise call ``databases.retrieve`` and use the first (and,
           for our DBs, only) child ``data_sources`` entry. We cache the
           result in-process so the lookup runs at most once per run.

    Raises:
        RuntimeError: if NOTION_DB_ID is unset, or the database has zero
            data sources (which should never happen for a real Notion
            database).
    """
    global _data_source_id
    if _data_source_id is not None:
        return _data_source_id

    if NOTION_DS_ID:
        _data_source_id = NOTION_DS_ID
        return _data_source_id

    if not NOTION_DB_ID:
        raise RuntimeError("NOTION_DB_ID is not set; cannot resolve data_source_id")

    client = _get_client()
    db_info = client.databases.retrieve(database_id=NOTION_DB_ID)
    sources = db_info.get("data_sources") or []
    if not sources:
        raise RuntimeError(
            f"Notion database {NOTION_DB_ID} has no data_sources; cannot proceed"
        )
    if len(sources) > 1:
        names = ", ".join(f"{s.get('name')!r} ({s.get('id')})" for s in sources)
        print(
            f"[notion] WARN: NOTION_DB_ID {NOTION_DB_ID} has {len(sources)} data "
            f"sources [{names}]. Using the first one. Set NOTION_DS_ID to pin."
        )
    _data_source_id = sources[0]["id"]
    return _data_source_id


# --------------------- Property builders ---------------------
def _truncate(s: str | None) -> str:
    if not s:
        return ""
    if len(s) <= _RICH_TEXT_MAX:
        return s
    return s[: _RICH_TEXT_MAX - 3].rstrip() + "..."


def _rich(text: str | None) -> dict[str, Any]:
    if not text:
        return {"rich_text": []}
    return {"rich_text": [{"text": {"content": _truncate(text)}}]}


def _title(text: str | None) -> dict[str, Any]:
    if not text:
        return {"title": []}
    return {"title": [{"text": {"content": _truncate(text)}}]}


def _number(v: Any) -> dict[str, Any]:
    if v is None or v == "":
        return {"number": None}
    try:
        return {"number": float(v)}
    except (TypeError, ValueError):
        return {"number": None}


def _select(v: str | None, whitelist: set[str]) -> dict[str, Any]:
    if not v or v not in whitelist:
        return {"select": None}
    return {"select": {"name": v}}


def _url(v: str | None) -> dict[str, Any]:
    return {"url": v or None}


def _checkbox(v: Any) -> dict[str, Any]:
    return {"checkbox": bool(v)}


def _date(v: str | None) -> dict[str, Any]:
    # Notion accepts ISO-8601 directly. Our records use
    # datetime.isoformat(timespec="seconds"), which is exactly this.
    if not v:
        return {"date": None}
    return {"date": {"start": v}}


def _to_props(record: dict[str, Any]) -> dict[str, Any]:
    """Map a pipeline record to a Notion ``properties`` payload.

    Title fallback: if title_zh is empty (e.g. the extract node failed),
    fall back to article_url so the page is still identifiable in the
    Notion UI instead of showing "Untitled".
    """
    title_text = record.get("title_zh") or record.get("article_url") or "Untitled"
    stage_errors = record.get("stage_errors") or []

    return {
        "title_zh": _title(title_text),
        "title_en": _rich(record.get("title_en")),
        "excerpt_zh": _rich(record.get("excerpt_zh")),
        "excerpt_en": _rich(record.get("excerpt_en")),
        "unique_id": _rich(record.get("unique_id")),
        "hash_code": _rich(record.get("hash_code")),
        "status": _select(record.get("status"), _STATUS_WHITELIST),
        "tag": _select(record.get("tag"), _TAG_WHITELIST),
        "tag_reason": _rich(record.get("tag_reason")),
        "article_url": _url(record.get("article_url")),
        "source_url": _url(record.get("source_url")),
        "source_name": _rich(record.get("source_name")),
        "author": _rich(record.get("author")),
        "country": _rich(record.get("country")),
        "published_at": _date(record.get("published_at")),
        "scraped_at": _date(record.get("scraped_at")),
        "created_at": _date(record.get("created_at")),
        "read_minutes": _number(record.get("read_minutes")),
        "relevance_score": _number(record.get("relevance_score")),
        "quality_score": _number(record.get("quality_score")),
        "overall_score": _number(record.get("overall_score")),
        "reason": _rich(record.get("reason")),
        "needs_revision": _checkbox(record.get("needs_revision")),
        "review_notes": _rich(record.get("review_notes")),
        "notes": _rich(record.get("notes")),
        "stage_errors": _rich(json.dumps(stage_errors, ensure_ascii=False)),
    }


# --------------------- Push (pipeline -> Notion) ---------------------
def _find_page_by_unique_id(unique_id: str) -> str | None:
    """Return the Notion page id whose ``unique_id`` property equals the arg."""
    client = _get_client()
    resp = client.data_sources.query(
        data_source_id=_get_data_source_id(),
        filter={
            "property": "unique_id",
            "rich_text": {"equals": unique_id},
        },
        page_size=1,
    )
    results = resp.get("results", [])
    return results[0]["id"] if results else None


def push_article(record: dict[str, Any]) -> str | None:
    """Upsert one record into Notion. Returns the Notion page id, or None.

    Never raises -- Notion sync must not break the local pipeline. On any
    failure (missing creds, 429s, schema mismatch) we log and return None,
    leaving the local DB row authoritative.
    """
    if not NOTION_SYNC_ENABLED:
        return None

    unique_id = record.get("unique_id")
    if not unique_id:
        print("[notion] skip: record missing unique_id")
        return None

    props = _to_props(record)

    try:
        existing_page_id = _find_page_by_unique_id(unique_id)
        time.sleep(_API_PAUSE_S)
        client = _get_client()
        if existing_page_id:
            resp = client.pages.update(page_id=existing_page_id, properties=props)
            action = "updated"
        else:
            resp = client.pages.create(
                parent={
                    "type": "data_source_id",
                    "data_source_id": _get_data_source_id(),
                },
                properties=props,
            )
            action = "created"
        page_id = resp["id"]
        print(f"[notion] {action} {page_id[:8]}... unique_id={unique_id[:8]}...")
        return page_id
    except APIResponseError as exc:
        print(f"[notion failed] unique_id={unique_id[:8]}... {exc.code}: {exc}")
        return None
    except Exception as exc:
        print(f"[notion failed] unique_id={unique_id[:8]}... {exc}")
        return None


# --------------------- Pull (Notion -> pipeline) ---------------------
def _iter_all_pages():
    """Yield every page in the configured Notion data source, paginated."""
    client = _get_client()
    data_source_id = _get_data_source_id()
    cursor: str | None = None
    while True:
        kwargs: dict[str, Any] = {
            "data_source_id": data_source_id,
            "page_size": 100,
        }
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = client.data_sources.query(**kwargs)
        for page in resp.get("results", []):
            yield page
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")


def _extract_rich_text(prop: dict | None) -> str:
    if not prop:
        return ""
    arr = prop.get("rich_text") or []
    return "".join(t.get("plain_text", "") for t in arr)


def _extract_select(prop: dict | None) -> str:
    if not prop:
        return ""
    sel = prop.get("select")
    return sel.get("name", "") if sel else ""


def pull_status_changes(local_statuses: dict[str, str]) -> list[dict[str, str]]:
    """Compare Notion's ``status`` column against the local DB.

    Args:
        local_statuses: ``{unique_id: current_local_status}`` for every
            row already in the local DB.

    Returns:
        A list of ``{"unique_id", "old_status", "new_status"}`` entries
        where Notion differs from local. Notion rows that we do not know
        locally are ignored (we never auto-create local rows from Notion).
    """
    if not NOTION_SYNC_ENABLED:
        return []

    diffs: list[dict[str, str]] = []
    for page in _iter_all_pages():
        props = page.get("properties", {})
        unique_id = _extract_rich_text(props.get("unique_id"))
        notion_status = _extract_select(props.get("status"))
        if not unique_id or not notion_status:
            continue
        if notion_status not in _STATUS_WHITELIST:
            print(f"[notion] WARN: invalid status '{notion_status}' on {unique_id[:8]}...")
            continue
        local = local_statuses.get(unique_id)
        if local is None:
            continue
        if local != notion_status:
            diffs.append({
                "unique_id": unique_id,
                "old_status": local,
                "new_status": notion_status,
            })
    return diffs


# --------------------- Schema sanity check (dev tool) ---------------------
def _fetch_data_source_properties() -> dict[str, dict[str, Any]]:
    """Fetch the properties (column schema) of our configured data source.

    Under 2025-09-03+ properties live on the data source object, not on
    the database. ``databases.retrieve`` returns only the database
    container (title, parent, child data_sources list); the actual
    schema requires ``data_sources.retrieve``.
    """
    client = _get_client()
    ds_info = client.data_sources.retrieve(data_source_id=_get_data_source_id())
    return ds_info.get("properties", {}) or {}


def check_schema() -> tuple[list[str], list[str], list[str]]:
    """Compare expected vs. actual Notion data source schema.

    Returns ``(missing, extra, type_mismatches)``. Each list contains
    human-readable lines suitable for printing.
    """
    if not NOTION_SYNC_ENABLED:
        raise RuntimeError(
            "NOTION_TOKEN / NOTION_DB_ID must be set to run schema check"
        )
    actual = _fetch_data_source_properties()
    actual_types = {name: meta.get("type") for name, meta in actual.items()}

    missing: list[str] = []
    extra: list[str] = []
    type_mismatches: list[str] = []

    for name, expected_type in _EXPECTED_PROPERTIES.items():
        if name not in actual_types:
            missing.append(f"  - {name}   (expected type: {expected_type})")
        elif actual_types[name] != expected_type:
            type_mismatches.append(
                f"  - {name}   expected={expected_type}, actual={actual_types[name]}"
            )

    for name in actual_types:
        if name in _EXPECTED_PROPERTIES or name in _SYSTEM_MANAGED_PROPERTIES:
            continue
        extra.append(f"  - {name}   (type: {actual_types[name]})")

    return missing, extra, type_mismatches


def _print_schema_report() -> None:
    """Print a diagnostic report for the configured Notion database.

    Prints (in order):
        1. The database container info (object type, title, child
           data_sources list). This is the part that tells you whether
           you're hitting the right database at all.
        2. The data source info (which one we picked, how many columns).
        3. The comparison against ``_EXPECTED_PROPERTIES`` (missing /
           type-mismatched / extra columns).
    """
    if not NOTION_SYNC_ENABLED:
        print(
            "NOTION_TOKEN or NOTION_DB_ID is not set in .env; "
            "schema check cannot run."
        )
        return

    print(f"Checking Notion DB schema (db_id={NOTION_DB_ID})...\n")

    client = _get_client()
    try:
        db_info = client.databases.retrieve(database_id=NOTION_DB_ID)
    except APIResponseError as exc:
        print(f"FAILED to retrieve database: {exc.code}: {exc}")
        print("\nLikely causes:")
        print("  - NOTION_DB_ID is wrong (must be the 32-char hex of the DB itself,")
        print("    NOT a parent page that contains an inline database).")
        print("  - The DB is not shared with your integration: open the DB,")
        print("    click '...' (top-right) -> Connections -> add your integration.")
        print("  - NOTION_TOKEN belongs to a different workspace than the DB.")
        return
    except Exception as exc:
        print(f"FAILED to retrieve database: {exc}")
        return

    # --- Phase 1: dump the database container info ------------------------
    obj_type = db_info.get("object")
    title_arr = db_info.get("title") or []
    title = "".join(t.get("plain_text", "") for t in title_arr) or "(untitled)"
    sources = db_info.get("data_sources") or []

    print(f"  object type  : {obj_type}")
    print(f"  title        : {title}")
    print(f"  data_sources : {len(sources)}")
    for src in sources:
        print(f"    - {src.get('name')!r}  id={src.get('id')}")
    print()

    if obj_type and obj_type != "database":
        print(
            f"WARNING: object type is '{obj_type}', not 'database'. The ID in "
            "your NOTION_DB_ID is probably a page, not a database."
        )
        print()

    if not sources:
        print(
            "WARNING: Notion returned 0 data_sources for this database. "
            "Cannot run schema check."
        )
        return

    # --- Phase 2: fetch the chosen data source's properties --------------
    try:
        ds_id = _get_data_source_id()
        ds_info = client.data_sources.retrieve(data_source_id=ds_id)
    except APIResponseError as exc:
        print(f"FAILED to retrieve data source: {exc.code}: {exc}")
        return
    except Exception as exc:
        print(f"FAILED to retrieve data source: {exc}")
        return

    actual = ds_info.get("properties", {}) or {}
    actual_types = {name: meta.get("type") for name, meta in actual.items()}

    ds_title_arr = ds_info.get("title") or []
    ds_title = (
        "".join(t.get("plain_text", "") for t in ds_title_arr) or "(untitled)"
    )
    print(f"Inspecting data_source '{ds_title}' (id={ds_id})")
    print(f"  properties : {len(actual_types)} column(s)\n")

    if actual_types:
        print("Actual properties found in Notion:")
        for name in sorted(actual_types.keys()):
            print(f"  - {name!r:<32}  type: {actual_types[name]}")
        print()
    else:
        print(
            "WARNING: Notion returned 0 properties on this data source. "
            "Double-check that NOTION_DB_ID / NOTION_DS_ID point at the "
            "right object.\n"
        )

    # --- Phase 3: compare against expected schema -------------------------
    missing, extra, mismatches = check_schema()
    if not missing and not extra and not mismatches:
        print("OK: every expected property is present with the right type.")
        return

    if missing:
        print("MISSING properties (pipeline pushes will fail):")
        for line in missing:
            print(line)
        print()
    if mismatches:
        print("TYPE MISMATCHES (pipeline pushes will likely fail):")
        for line in mismatches:
            print(line)
        print()
    if extra:
        print("Extra properties in Notion (harmless, just noted):")
        for line in extra:
            print(line)


if __name__ == "__main__":
    _print_schema_report()
