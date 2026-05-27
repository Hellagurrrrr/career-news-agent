"""Feishu (Lark) Bitable as a cloud admin UI for the articles pipeline.

The local SQLite store (db.py) is always the source of truth. This module
gives the pipeline two cloud-facing capabilities:

    push_article(record)
        Upsert one local record into the Feishu Bitable, keyed by
        ``unique_id``. Called from main.py right after db.insert_article.

    pull_status_changes(local_statuses)
        Walk the Bitable and return rows whose ``status`` differs from
        what we have locally. Called from sync_back.py.

If any of FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_APP_TOKEN /
FEISHU_TABLE_ID is unset, every function in here turns into a graceful
no-op so the rest of the pipeline keeps working.

The Bitable is expected to expose the fields listed in
``_EXPECTED_FIELDS`` with matching types (names must match EXACTLY --
snake_case). Run this module directly (``python feishu_sync.py``) to
compare your Bitable schema against this expected set.

Feishu Bitable field types used here (see open.feishu.cn docs at
https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/bitable-v1/app-table-field):

    1  = text       (rich text; auto-promoted to "primary field" for col 0)
    2  = number
    3  = single_select
    5  = datetime   (millisecond epoch int)
    7  = checkbox
    15 = url        ({"link": ..., "text": ...})
"""

import json
import time
from datetime import datetime
from typing import Any

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import (
    AppTableRecord,
    CreateAppTableRecordRequest,
    ListAppTableFieldRequest,
    SearchAppTableRecordRequest,
    SearchAppTableRecordRequestBody,
    UpdateAppTableRecordRequest,
)

from db import ArticleStatus
from settings.config import (
    FEISHU_APP_ID,
    FEISHU_APP_SECRET,
    FEISHU_APP_TOKEN,
    FEISHU_SYNC_ENABLED,
    FEISHU_TABLE_ID,
)

# Bitable text cells accept ~100k chars, but a smaller ceiling keeps
# payloads light and grid views readable. 1900 is chosen to leave a small
# margin under common "show 2000 chars in column preview" UI behaviours.
_TEXT_MAX = 1900

# Feishu Bitable per-tenant write quota is ~50 QPS for record APIs. A
# tiny pause between calls protects us from local burst patterns
# (e.g. one finished article triggering search + update back-to-back).
_API_PAUSE_S = 0.1

# Allowed values for the two Bitable single-select fields. We do
# client-side validation so an LLM hallucination cannot pollute the
# Bitable option list (Bitable auto-creates new options on first sight).
_STATUS_WHITELIST: set[str] = {s.value for s in ArticleStatus}
_TAG_WHITELIST: set[str] = {"MARKET", "MENTOR", "REPORT", "VISA", "CITY"}

# Feishu Bitable field type ids.
_TYPE_TEXT = 1
_TYPE_NUMBER = 2
_TYPE_SELECT = 3
_TYPE_DATETIME = 5
_TYPE_CHECKBOX = 7
_TYPE_URL = 15

_TYPE_NAMES: dict[int, str] = {
    _TYPE_TEXT: "text",
    _TYPE_NUMBER: "number",
    _TYPE_SELECT: "single_select",
    _TYPE_DATETIME: "datetime",
    _TYPE_CHECKBOX: "checkbox",
    _TYPE_URL: "url",
}

# The exact field names + type ids the Bitable must expose. Used by the
# schema checker (run ``python feishu_sync.py``) and is the canonical
# mapping document for the integration.
#
# NOTE: Bitable picks the FIRST text column you create as the table's
# primary field (the bold, mandatory first column). Create ``title_zh``
# first so it claims that slot.
_EXPECTED_FIELDS: dict[str, int] = {
    "title_zh": _TYPE_TEXT,
    "title_en": _TYPE_TEXT,
    "excerpt_zh": _TYPE_TEXT,
    "excerpt_en": _TYPE_TEXT,
    "unique_id": _TYPE_TEXT,
    "hash_code": _TYPE_TEXT,
    "status": _TYPE_SELECT,
    "tag": _TYPE_SELECT,
    "tag_reason": _TYPE_TEXT,
    "article_url": _TYPE_URL,
    "source_url": _TYPE_URL,
    "source_name": _TYPE_TEXT,
    "author": _TYPE_TEXT,
    "country": _TYPE_TEXT,
    "published_at": _TYPE_DATETIME,
    "scraped_at": _TYPE_DATETIME,
    "created_at": _TYPE_DATETIME,
    "read_minutes": _TYPE_NUMBER,
    "relevance_score": _TYPE_NUMBER,
    "quality_score": _TYPE_NUMBER,
    "overall_score": _TYPE_NUMBER,
    "reason": _TYPE_TEXT,
    "needs_revision": _TYPE_CHECKBOX,
    "review_notes": _TYPE_TEXT,
    "notes": _TYPE_TEXT,
    "stage_errors": _TYPE_TEXT,
}

# Fields we tolerate in Bitable but never write to. Bitable's per-row
# metadata (created_time / last_modified_time / created_by / modified_by)
# only appears when the user explicitly adds those system field columns,
# so this set is empty by default.
_SYSTEM_MANAGED_FIELDS: set[str] = set()


_client: lark.Client | None = None


def _get_client() -> lark.Client:
    """Lazily build a Lark client; raises if credentials are missing."""
    global _client
    if _client is None:
        if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
            raise RuntimeError(
                "FEISHU_APP_ID / FEISHU_APP_SECRET are not set; "
                "cannot reach Lark API"
            )
        _client = (
            lark.Client.builder()
            .app_id(FEISHU_APP_ID)
            .app_secret(FEISHU_APP_SECRET)
            .log_level(lark.LogLevel.WARNING)
            .build()
        )
    return _client


# --------------------- Field value builders ---------------------
def _truncate(s: str | None) -> str:
    if not s:
        return ""
    if len(s) <= _TEXT_MAX:
        return s
    return s[: _TEXT_MAX - 3].rstrip() + "..."


def _to_epoch_ms(v: str | None) -> int | None:
    """Convert an ISO-8601 timestamp into a millisecond epoch int.

    Bitable's datetime field type stores millisecond epochs natively.
    Returns None for empty / unparseable inputs, which the caller then
    drops from the payload (writing 0 would show 1970-01-01 in the grid).
    """
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    for shape in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(s, shape).timestamp() * 1000)
        except ValueError:
            continue
    # Last resort: let fromisoformat handle timezone-aware variants.
    try:
        normalized = s.replace("Z", "+00:00")
        return int(datetime.fromisoformat(normalized).timestamp() * 1000)
    except ValueError:
        return None


def _select_value(v: str | None, whitelist: set[str]) -> str | None:
    if not v or v not in whitelist:
        return None
    return v


def _url_value(v: str | None) -> dict[str, str] | None:
    """Bitable URL fields take a ``{"link", "text"}`` pair.

    We set both to the same value so the cell renders the URL itself as
    the clickable display text.
    """
    if not v:
        return None
    return {"link": v, "text": v}


def _number_value(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_fields(record: dict[str, Any]) -> dict[str, Any]:
    """Map a pipeline record to a Bitable ``fields`` payload.

    Empty / None values are deliberately omitted: Bitable treats a
    missing key as "leave this cell unchanged" on update, which is
    exactly what we want for partial extracts. For first-time creates
    Bitable falls back to per-type defaults (number=null, text=empty,
    checkbox=false, ...).

    Title fallback: if title_zh is empty (e.g. the extract node failed),
    fall back to article_url so the row is still identifiable in the
    Bitable grid view instead of showing an empty primary cell.
    """
    fields: dict[str, Any] = {}

    title_text = record.get("title_zh") or record.get("article_url") or "Untitled"
    fields["title_zh"] = _truncate(title_text)

    text_keys = (
        "title_en", "excerpt_zh", "excerpt_en", "unique_id", "hash_code",
        "tag_reason", "source_name", "author", "country", "reason",
        "review_notes", "notes",
    )
    for key in text_keys:
        val = record.get(key)
        if val:
            fields[key] = _truncate(str(val))

    status = _select_value(record.get("status"), _STATUS_WHITELIST)
    if status:
        fields["status"] = status
    tag = _select_value(record.get("tag"), _TAG_WHITELIST)
    if tag:
        fields["tag"] = tag

    article_url = _url_value(record.get("article_url"))
    if article_url:
        fields["article_url"] = article_url
    source_url = _url_value(record.get("source_url"))
    if source_url:
        fields["source_url"] = source_url

    for date_key in ("published_at", "scraped_at", "created_at"):
        ms = _to_epoch_ms(record.get(date_key))
        if ms is not None:
            fields[date_key] = ms

    for num_key in ("read_minutes", "relevance_score",
                    "quality_score", "overall_score"):
        num = _number_value(record.get(num_key))
        if num is not None:
            fields[num_key] = num

    fields["needs_revision"] = bool(record.get("needs_revision"))

    stage_errors = record.get("stage_errors") or []
    fields["stage_errors"] = _truncate(
        json.dumps(stage_errors, ensure_ascii=False)
    )

    return fields


# --------------------- Push (pipeline -> Bitable) ---------------------
def _find_record_id(unique_id: str) -> str | None:
    """Return the Bitable record_id whose ``unique_id`` field equals the arg."""
    client = _get_client()
    body = (
        SearchAppTableRecordRequestBody.builder()
        .filter({
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": "unique_id",
                    "operator": "is",
                    "value": [unique_id],
                }
            ],
        })
        .build()
    )
    request = (
        SearchAppTableRecordRequest.builder()
        .app_token(FEISHU_APP_TOKEN)
        .table_id(FEISHU_TABLE_ID)
        .page_size(1)
        .request_body(body)
        .build()
    )
    resp = client.bitable.v1.app_table_record.search(request)
    if not resp.success():
        # Deliberately don't raise -- the caller has its own broad
        # try/except and will fall through to ``create`` if we return
        # None. The downside is duplicate rows on transient errors;
        # the upside is no "Feishu outage breaks pipeline" failure mode.
        print(
            f"[feishu] search failed for unique_id={unique_id[:8]}...: "
            f"code={resp.code} msg={resp.msg}"
        )
        return None
    items = resp.data.items or []
    return items[0].record_id if items else None


def push_article(record: dict[str, Any]) -> str | None:
    """Upsert one record into the Bitable. Returns the record_id or None.

    Never raises -- Feishu sync must not break the local pipeline. On
    any failure (missing creds, rate limits, schema mismatch) we log and
    return None, leaving the local DB row authoritative.
    """
    if not FEISHU_SYNC_ENABLED:
        return None

    unique_id = record.get("unique_id")
    if not unique_id:
        print("[feishu] skip: record missing unique_id")
        return None

    fields = _to_fields(record)
    table_record = AppTableRecord.builder().fields(fields).build()

    try:
        existing_record_id = _find_record_id(unique_id)
        time.sleep(_API_PAUSE_S)
        client = _get_client()
        if existing_record_id:
            req = (
                UpdateAppTableRecordRequest.builder()
                .app_token(FEISHU_APP_TOKEN)
                .table_id(FEISHU_TABLE_ID)
                .record_id(existing_record_id)
                .request_body(table_record)
                .build()
            )
            resp = client.bitable.v1.app_table_record.update(req)
            action = "updated"
        else:
            req = (
                CreateAppTableRecordRequest.builder()
                .app_token(FEISHU_APP_TOKEN)
                .table_id(FEISHU_TABLE_ID)
                .request_body(table_record)
                .build()
            )
            resp = client.bitable.v1.app_table_record.create(req)
            action = "created"
        if not resp.success():
            print(
                f"[feishu failed] unique_id={unique_id[:8]}... "
                f"code={resp.code}: {resp.msg}"
            )
            return None
        record_id = resp.data.record.record_id
        print(f"[feishu] {action} {record_id} unique_id={unique_id[:8]}...")
        return record_id
    except Exception as exc:
        print(f"[feishu failed] unique_id={unique_id[:8]}... {exc}")
        return None


# --------------------- Pull (Bitable -> pipeline) ---------------------
def _iter_all_records():
    """Yield every record in the configured Bitable, paginated.

    Only ``unique_id`` and ``status`` are requested per page, which
    keeps the payload small even on large tables.
    """
    client = _get_client()
    page_token: str | None = None
    while True:
        body = (
            SearchAppTableRecordRequestBody.builder()
            .field_names(["unique_id", "status"])
            .build()
        )
        req_builder = (
            SearchAppTableRecordRequest.builder()
            .app_token(FEISHU_APP_TOKEN)
            .table_id(FEISHU_TABLE_ID)
            .page_size(100)
            .request_body(body)
        )
        if page_token:
            req_builder = req_builder.page_token(page_token)
        resp = client.bitable.v1.app_table_record.search(req_builder.build())
        if not resp.success():
            print(
                f"[feishu] list failed: code={resp.code} msg={resp.msg}; "
                "aborting pull."
            )
            return
        for item in resp.data.items or []:
            yield item
        if not resp.data.has_more:
            return
        page_token = resp.data.page_token


def _extract_text(field_val: Any) -> str:
    """Extract a plain string from a Bitable text-field value.

    Bitable returns rich text as a list of segments
    ``[{"type": "text", "text": "..."}]`` for non-trivial values; short
    plain strings sometimes come back as a raw string instead. Tolerate
    both.
    """
    if field_val is None:
        return ""
    if isinstance(field_val, str):
        return field_val
    if isinstance(field_val, list):
        return "".join(
            seg.get("text", "") for seg in field_val if isinstance(seg, dict)
        )
    if isinstance(field_val, dict):
        return field_val.get("text", "") or ""
    return str(field_val)


def pull_status_changes(local_statuses: dict[str, str]) -> list[dict[str, str]]:
    """Compare Bitable's ``status`` field against the local DB.

    Args:
        local_statuses: ``{unique_id: current_local_status}`` for every
            row already in the local DB.

    Returns:
        A list of ``{"unique_id", "old_status", "new_status"}`` entries
        where Bitable differs from local. Bitable rows we don't know
        locally are ignored (we never auto-create local rows from
        Bitable).
    """
    if not FEISHU_SYNC_ENABLED:
        return []

    diffs: list[dict[str, str]] = []
    for item in _iter_all_records():
        fields = item.fields or {}
        unique_id = _extract_text(fields.get("unique_id"))
        feishu_status_raw = fields.get("status")
        # Single-select usually returns a plain string, but on some
        # Bitable versions it's wrapped as {"text": "..."}; handle both.
        if isinstance(feishu_status_raw, dict):
            feishu_status = feishu_status_raw.get("text", "") or ""
        else:
            feishu_status = str(feishu_status_raw or "").strip()
        if not unique_id or not feishu_status:
            continue
        if feishu_status not in _STATUS_WHITELIST:
            print(
                f"[feishu] WARN: invalid status '{feishu_status}' on "
                f"{unique_id[:8]}..."
            )
            continue
        local = local_statuses.get(unique_id)
        if local is None:
            continue
        if local != feishu_status:
            diffs.append({
                "unique_id": unique_id,
                "old_status": local,
                "new_status": feishu_status,
            })
    return diffs


# --------------------- Schema sanity check (dev tool) ---------------------
def _fetch_fields() -> dict[str, int]:
    """Fetch ``{field_name: field_type_id}`` for the configured Bitable."""
    client = _get_client()
    out: dict[str, int] = {}
    page_token: str | None = None
    while True:
        req_builder = (
            ListAppTableFieldRequest.builder()
            .app_token(FEISHU_APP_TOKEN)
            .table_id(FEISHU_TABLE_ID)
            .page_size(100)
        )
        if page_token:
            req_builder = req_builder.page_token(page_token)
        resp = client.bitable.v1.app_table_field.list(req_builder.build())
        if not resp.success():
            raise RuntimeError(
                f"Bitable list fields failed: "
                f"code={resp.code} msg={resp.msg}"
            )
        for item in resp.data.items or []:
            out[item.field_name] = item.type
        if not resp.data.has_more:
            break
        page_token = resp.data.page_token
    return out


def check_schema() -> tuple[list[str], list[str], list[str]]:
    """Compare expected vs. actual Bitable field schema.

    Returns ``(missing, extra, type_mismatches)``. Each list contains
    human-readable lines suitable for printing.
    """
    if not FEISHU_SYNC_ENABLED:
        raise RuntimeError(
            "FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_APP_TOKEN / "
            "FEISHU_TABLE_ID must all be set to run schema check"
        )
    actual = _fetch_fields()

    missing: list[str] = []
    extra: list[str] = []
    type_mismatches: list[str] = []

    for name, expected_type in _EXPECTED_FIELDS.items():
        if name not in actual:
            type_label = _TYPE_NAMES.get(expected_type, str(expected_type))
            missing.append(f"  - {name}   (expected type: {type_label})")
        elif actual[name] != expected_type:
            type_mismatches.append(
                f"  - {name}   "
                f"expected={_TYPE_NAMES.get(expected_type, expected_type)}, "
                f"actual={_TYPE_NAMES.get(actual[name], actual[name])}"
            )

    for name, type_id in actual.items():
        if name in _EXPECTED_FIELDS or name in _SYSTEM_MANAGED_FIELDS:
            continue
        extra.append(
            f"  - {name}   (type: {_TYPE_NAMES.get(type_id, type_id)})"
        )

    return missing, extra, type_mismatches


def _print_schema_report() -> None:
    """Print a diagnostic report for the configured Bitable.

    Prints (in order):
        1. The full list of fields we found in the Bitable.
        2. The comparison against ``_EXPECTED_FIELDS`` (missing /
           type-mismatched / extra columns).
    """
    if not FEISHU_SYNC_ENABLED:
        print(
            "FEISHU_* env vars are not all set; schema check cannot run."
        )
        return

    print(
        f"Checking Feishu Bitable schema "
        f"(app_token={FEISHU_APP_TOKEN}, table_id={FEISHU_TABLE_ID})...\n"
    )

    try:
        actual = _fetch_fields()
    except Exception as exc:
        print(f"FAILED to list fields: {exc}")
        print("\nLikely causes:")
        print("  - FEISHU_APP_TOKEN is wrong (must be the app_token from")
        print("    the Bitable URL: feishu.cn/base/<app_token>?table=<table_id>).")
        print("  - The Bitable is not shared with your application: open the")
        print("    Bitable, click the '...' menu -> Share -> add your app as")
        print("    a collaborator with edit permission.")
        print("  - FEISHU_APP_ID / FEISHU_APP_SECRET belong to a different")
        print("    tenant than the Bitable.")
        return

    print(f"  fields : {len(actual)} column(s)\n")
    if actual:
        print("Actual fields found in Bitable:")
        for name in sorted(actual.keys()):
            type_name = _TYPE_NAMES.get(actual[name], f"type_{actual[name]}")
            print(f"  - {name!r:<32}  type: {type_name}")
        print()
    else:
        print(
            "WARNING: Bitable returned 0 fields. Double-check that "
            "FEISHU_APP_TOKEN / FEISHU_TABLE_ID point at the right "
            "table.\n"
        )

    missing, extra, mismatches = check_schema()
    if not missing and not extra and not mismatches:
        print("OK: every expected field is present with the right type.")
        return

    if missing:
        print("MISSING fields (pipeline pushes will fail):")
        for line in missing:
            print(line)
        print()
    if mismatches:
        print("TYPE MISMATCHES (pipeline pushes will likely fail):")
        for line in mismatches:
            print(line)
        print()
    if extra:
        print("Extra fields in Bitable (harmless, just noted):")
        for line in extra:
            print(line)


if __name__ == "__main__":
    _print_schema_report()
