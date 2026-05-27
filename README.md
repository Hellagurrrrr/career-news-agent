# career-news-agent

> An editorial-grade content sourcing pipeline for [community web name](webLink) —
> a curated career community for international students.

This is a standalone tool that discovers, scrapes, scores, tags and queues
high-signal articles for the **Insights** section of the [name to be defined]
Careers website. It is **not** a generic web crawler. It is designed as a
semi-automated *editorial assistant* that respects publishers, prefers
quality over volume, and always keeps a human in the loop.

---

## Why this exists

[name to be defined]'s audience is international students targeting roles at
top-tier banks, consultancies and tech firms. The Insights board therefore
needs content that is:

- **Timely** — hiring signals, comp data, visa policy shifts
- **Specific** — written for graduates pursuing global careers
- **Trustworthy** — sourced from named publications, never scraped wholesale

Hand-curating this every week is expensive. This agent automates the
*sourcing and triage* layer so the editorial team only spends time on the
last 20% — review, voice, and publishing.

---

## Architecture

```
┌──────────────┐   ┌───────────────┐   ┌──────────────────────────────┐   ┌──────────────┐   ┌────────┐
│ sources/     │──▶│   Firecrawl   │──▶│  SQLite dedupe (hash_code)   │──▶│  LangGraph   │──▶│ SQLite │
│ source.yaml  │   │  /map +/scrape│   │  drop rows already processed │   │  4-node agent│   │ (SoT)  │
└──────────────┘   └───────────────┘   └──────────────────────────────┘   └──────┬───────┘   └───┬────┘
                                                                                 │               │
                                                                                 ▼               ▼
                                                                          ┌─────────────┐  ┌──────────────┐
                                                                          │ output/<ts>/│  │ Feishu       │
                                                                          │ articles+   │  │ Bitable      │
                                                                          │ metrics.json│  │ (mirror &    │
                                                                          │             │  │  review UI)  │
                                                                          └─────────────┘  └──────┬───────┘
                                                                                                  │
                                                                                                  ▼
                                                                                          ┌───────────────┐
                                                                                          │  sync_back.py │
                                                                                          │ pulls status  │
                                                                                          │ changes back  │
                                                                                          └───────────────┘
```

**Stage 1 — Discovery (`crawler.map_sources_from_yaml`).** Sources are
declared in `sources/source.yaml`. Each entry uses Firecrawl `/map` against
a landing page (e.g. `https://www.jpmorgan.com/insights`). The discovered
URLs are filtered per-source by a regex `url_pattern`, with asset URLs
(`.pdf`, `.jpg`, `.mp4`, …) and the landing page itself dropped.

**Stage 2 — Fetch (`crawler.scrape_links_to_table`).** Filtered URLs are
fetched via Firecrawl `/scrape` and returned as clean Markdown, fanned out
across `SCRAPE_MAX_WORKERS` threads per source.

**Stage 3 — Dedupe (`db.compute_hash` + `db.existing_hashes`).** Each row
gets a SHA-256 of its normalized `article_url`. Anything already in
`articles.db` is dropped *before* the LLM stage, so we never spend tokens
on a re-scrape.

**Stage 4 — LLM agent (`raw_process_agent`, LangGraph, DeepSeek).** Every
surviving row runs through a 4-node graph:

1. **`extract`** — pulls `title_zh/en`, `excerpt_zh/en`, `author`,
   `country`, `published_at`, `read_minutes`, `notes` from the raw
   markdown (structured output via Pydantic).
2. **`score`** — independent `relevance_score` and `quality_score` on a
   0–10 rubric (see `settings/config.py::SCORE_CRITERIA`). `overall_score`
   is computed deterministically in Python as their rounded mean.
3. **`generate_tag`** — picks exactly one of `MARKET` / `MENTOR` /
   `REPORT` / `VISA` / `CITY`, plus a short `tag_reason`.
4. **`review`** — re-reads the source markdown and cross-checks every
   extracted field, producing a corrected record plus `needs_revision`
   and `review_notes`.

Each node catches its own LLM errors into `stage_errors` so one bad call
never aborts the article.

**Stage 5 — Persist + mirror.** Every completed record is:

- Inserted into the local **SQLite** store (`articles.db`) with
  `status = REVIEWING`. SQLite is the **single source of truth**.
- Optionally pushed to **Feishu Bitable** (`feishu_sync.push_article`)
  as a cloud admin UI for editors. Feishu failures are logged and
  swallowed — they never break the local pipeline.
- Dumped to `output/<timestamp>/articles.json` plus
  `output/<timestamp>/metrics.json` for that run.

**Stage 6 — Reverse sync (`sync_back.py`).** Editors flip rows in the
Feishu Bitable between `REVIEWING → READY → PUBLISHED` (or `DISCARD`).
Running `python sync_back.py` walks the Bitable, diffs against the local
DB, and promotes any status changes back into SQLite.

---

## Quick start

### 1. Install dependencies

```bash
pip install firecrawl-py langchain-deepseek langgraph langchain-core \
            lark-oapi pandas pyyaml python-dotenv pydantic
```

### 2. Configure environment

Create a `.env` in the repo root and fill in:

```env
# Required
FIRECRAWL_API_KEY=fc-...
DEEPSEEK_API_KEY=sk-...

# Feishu (Lark) Bitable mirror -- all four required to enable sync.
# Leave any one empty to run pipeline + local SQLite only.
FEISHU_APP_ID=cli_...
FEISHU_APP_SECRET=...
FEISHU_APP_TOKEN=...   # from the URL: feishu.cn/base/<APP_TOKEN>?table=...
FEISHU_TABLE_ID=tbl... # the `table=` query param of the Bitable URL

# Optional pipeline tuning (defaults shown)
MAX_LINKS=5            # max URLs kept per source after /map filter
MAP_MAX_WORKERS=4      # parallel Firecrawl /map calls
SCRAPE_MAX_WORKERS=8   # parallel /scrape calls per source
LLM_MAX_WORKERS=6      # parallel DeepSeek calls in the agent stage
DB_PATH=articles.db    # SQLite file path (relative to repo root)
```

How to get the Feishu credentials:

1. Create a **custom app** on <https://open.feishu.cn/app>.
2. Under *Permissions & Scopes*, grant the `bitable:app` scope
   (read + write). Publish a version of the app so the scope takes effect.
3. Copy `App ID` + `App Secret` from the app's *Credentials & Basic Info*.
4. Open the target Bitable; copy the `<APP_TOKEN>` and `<TABLE_ID>` out
   of the URL (`feishu.cn/base/<APP_TOKEN>?table=<TABLE_ID>`).
5. In the Bitable, click `...` → *Share* → add the custom app as a
   collaborator with **Can edit** permission.

> Bitable fields are validated lazily. To verify your Bitable has the
> right columns before the first real run, do `python feishu_sync.py` —
> it prints the actual schema and lists any missing / mismatched fields.

### 3. (Optional) Edit the source whitelist

Add or remove publishers in `sources/source.yaml`:

```yaml
- name: Goldman Sachs Careers Blog
  type: map
  url: https://www.goldmansachs.com/careers/blog
  url_pattern: ^https?://(?:www\.)?goldmansachs\.com/careers/blog/.+
```

`url_pattern` is a Python regex applied to every link `/map` returns. Use
it to drop nav pages, tag indexes, social embeds, etc.

### 4. Run the pipeline

```bash
python main.py
```

This will, in one pass: map → scrape → dedupe → run the 4-node agent →
insert into SQLite → mirror to Feishu Bitable → write the per-run
snapshot to `output/<YYYYMMDD_HHMMSS>/`.

### 5. Sync editor decisions back

After editors change `status` in the Feishu Bitable:

```bash
python sync_back.py
```

---

## Local SQLite schema (`articles` table)

The local DB is the source of truth. See `db.py` for the canonical DDL.

| Column            | Type    | Notes                                                                                |
| ----------------- | ------- | ------------------------------------------------------------------------------------ |
| `unique_id`       | TEXT PK | UUID4 hex, assigned by `db.insert_article`                                           |
| `hash_code`       | TEXT    | SHA-256 of normalized `article_url`; UNIQUE; the dedupe key                          |
| `status`          | TEXT    | `REVIEWING` / `READY` / `PUBLISHED` / `DISCARD` (enforced by CHECK)                  |
| `article_url`     | TEXT    | Canonical URL of the scraped article                                                 |
| `source_name`     | TEXT    | Display name from `source.yaml`                                                      |
| `source_url`      | TEXT    | Landing-page URL from `source.yaml`                                                  |
| `scraped_at`      | TEXT    | ISO-8601 timestamp when Firecrawl returned the markdown                              |
| `raw_markdown`    | TEXT    | Original Firecrawl markdown (kept so the agent is replayable)                        |
| `title_zh/en`     | TEXT    | Bilingual title from the `extract` node                                              |
| `excerpt_zh/en`   | TEXT    | Bilingual excerpt (~500 chars target)                                                |
| `author`          | TEXT    | Original byline                                                                      |
| `country`         | TEXT    | Country the article is about / from                                                  |
| `published_at`    | TEXT    | Original publication time (string, as extracted)                                     |
| `read_minutes`    | INTEGER | Estimated reading time                                                               |
| `notes`           | TEXT    | Free-form notes from `extract`                                                       |
| `tag`             | TEXT    | One of `MARKET` / `MENTOR` / `REPORT` / `VISA` / `CITY` (CHECK)                      |
| `tag_reason`      | TEXT    | Short rationale for the chosen tag                                                   |
| `relevance_score` | INTEGER | 0–10, from `score` node                                                              |
| `quality_score`   | INTEGER | 0–10, from `score` node                                                              |
| `overall_score`   | INTEGER | `round((relevance + quality) / 2)`; computed in Python, not by the LLM               |
| `reason`          | TEXT    | Rationale for the scores                                                             |
| `needs_revision`  | INTEGER | 0/1 — whether the `review` node corrected any field                                  |
| `review_notes`    | TEXT    | Short Chinese note describing review changes                                         |
| `stage_errors`    | TEXT    | JSON array of `{node, error}` for any node that failed                               |
| `created_at`      | TEXT    | ISO-8601, set on insert                                                              |
| `updated_at`      | TEXT    | ISO-8601, refreshed by admin ops (`update_status`, `update_tag`)                     |

### Lifecycle (`ArticleStatus`)

| Status      | Set by         | Meaning                                                          |
| ----------- | -------------- | ---------------------------------------------------------------- |
| `REVIEWING` | pipeline       | Default after insert; waiting for editor review in Feishu        |
| `READY`     | editor → sync  | Approved; ready to publish on the community site                 |
| `PUBLISHED` | editor → sync  | Live on the community site                                       |
| `DISCARD`   | editor → sync  | Unusable; kept for dedupe history but never surfaced             |

### Tags (`ArticleTag`)

- **`MARKET`** — macro/meta-level industry trend observations on the
  recruitment market (e.g. tech giants' Q2 hiring pace, hedge-fund campus
  recruiting cadence).
- **`MENTOR`** — first-person know-how from current industry mentors
  (e.g. a Goldman IBD analyst's letter, a McKinsey EM's playbook).
- **`REPORT`** — in-house structured data reports emphasizing sample size
  and research dimensions (e.g. the 2026 International Student Job
  Seeking White Paper).
- **`VISA`** — policy / compliance content (e.g. new H-1B lottery rules).
- **`CITY`** — life and job-seeking guides centered on a single work city
  (London, New York, …), focused on lifestyle / local info.

---

## Feishu Bitable mirror schema

The Feishu Bitable is treated as a thin cloud admin UI on top of the
local SQLite store. Field names must match exactly (snake_case) so
`feishu_sync.push_article` can upsert without translation. The full
expected schema lives in `feishu_sync._EXPECTED_FIELDS`; running
`python feishu_sync.py` will diff your live Bitable against it.

> **Order matters when creating the table for the first time.** Bitable
> auto-promotes the first text column to the table's *primary field*
> (the bold, mandatory first column). Create `title_zh` first so it
> claims that slot.

| Field             | Bitable type    | Source field         |
| ----------------- | --------------- | -------------------- |
| `title_zh`        | text (primary)  | `title_zh`           |
| `title_en`        | text            | `title_en`           |
| `excerpt_zh`      | text            | `excerpt_zh`         |
| `excerpt_en`      | text            | `excerpt_en`         |
| `unique_id`       | text            | `unique_id`          |
| `hash_code`       | text            | `hash_code`          |
| `status`          | single_select   | `status` (one of 4)  |
| `tag`             | single_select   | `tag` (one of 5)     |
| `tag_reason`      | text            | `tag_reason`         |
| `article_url`     | url             | `article_url`        |
| `source_url`      | url             | `source_url`         |
| `source_name`     | text            | `source_name`        |
| `author`          | text            | `author`             |
| `country`         | text            | `country`            |
| `published_at`    | datetime        | `published_at`       |
| `scraped_at`      | datetime        | `scraped_at`         |
| `created_at`      | datetime        | `created_at`         |
| `read_minutes`    | number          | `read_minutes`       |
| `relevance_score` | number          | `relevance_score`    |
| `quality_score`   | number          | `quality_score`      |
| `overall_score`   | number          | `overall_score`      |
| `reason`          | text            | `reason`             |
| `needs_revision`  | checkbox        | `needs_revision`     |
| `review_notes`    | text            | `review_notes`       |
| `notes`           | text            | `notes`              |
| `stage_errors`    | text            | JSON-encoded         |

Notes:

- Uses the `lark-oapi` Python SDK targeting Bitable open API `v1`
  (`client.bitable.v1.app_table_record.{search,create,update}` and
  `client.bitable.v1.app_table_field.list`).
- Each text value is truncated to ~1900 chars to keep payloads light
  and grid views readable, even though Bitable accepts ~100k chars per
  cell.
- All three `*_at` columns are written as **millisecond epoch
  integers**, the native Bitable datetime format. ISO-8601 strings from
  the pipeline are converted in `_to_epoch_ms` before being pushed.
- `status` and `tag` are validated against a client-side whitelist so
  an LLM hallucination cannot pollute Bitable's auto-grown select
  options.

---

## Per-run output

Each invocation of `python main.py` creates `output/<YYYYMMDD_HHMMSS>/`
containing:

- **`articles.json`** — every article processed in that run, in the same
  shape that was inserted into SQLite. Handy for eyeballing one run
  without opening sqlite.
- **`metrics.json`** — per-stage timing and DeepSeek token usage
  (see `logger.PipelineLogger`). Stages tracked: `map`, `scrape`,
  `extract`, `score`, `generate_tag`, `review`.

A human-readable version of `metrics.json` is also printed at the end of
every run.

---

## Project layout

```
career-news-agent/
├── main.py                # async pipeline orchestrator
├── crawler.py             # Firecrawl /map + /scrape
├── raw_process_agent.py   # 4-node LangGraph agent (DeepSeek)
├── db.py                  # SQLite persistence (source of truth)
├── feishu_sync.py         # push to Feishu Bitable + schema checker
├── sync_back.py           # pull editor status changes from Feishu
├── llm.py                 # DeepSeek client factory
├── logger.py              # per-stage timing + token logger
├── settings/
│   └── config.py          # env-var loading + scoring rubric
├── sources/
│   └── source.yaml        # publisher whitelist
├── output/<run-ts>/       # per-run articles.json + metrics.json (gitignored)
├── articles.db            # local SQLite store (gitignored)
├── .env
└── README.md
```

---

## Compliance & ethics

This project is built around a hard rule: **we summarize, we never
republish**.

- `robots.txt` is always respected (Firecrawl enforces this by default).
- Whitelist-only — no opportunistic crawling of arbitrary domains.
- Excerpts are LLM-rewritten; the full article is never stored on the
  public website (the raw markdown is kept locally only for replayability
  of the agent stage).
- Every published card links back to the original source with attribution.
- Paywalled publications (WSJ, FT, Bloomberg, etc.) are limited to
  metadata + outbound links — no paywall circumvention, ever.
- The crawler identifies itself with Firecrawl's default User-Agent and a
  contact email so publishers can reach us.

If you are a publisher and would like us to stop ingesting your content,
email `editorial@meridian.careers` and we will remove the source within
one business day.

---

## Roadmap

**v0.1 — MVP (current)**

- Firecrawl-based whitelist crawler
- 4-node DeepSeek agent (extract / score / generate_tag / review)
- Local SQLite as source of truth + JSON snapshot per run
- Feishu Bitable mirror with two-way status sync
- Per-stage timing and token metrics

**v0.2 — Hardening**

- Semantic deduplication via embeddings (in addition to URL-hash)
- Per-source success/failure dashboards on top of `metrics.json`
- Quarterly Bitable archival job
- Retry/backoff on Firecrawl 429s and DeepSeek transient errors

**v0.3 — Website integration**

- Postgres mirror as the new shared source of truth
- Read-only `GET /api/news` endpoint consumed by the main site
- Auto-publish flow for `READY` items

**v1.0 — Editorial agent**

- Background-research agent that synthesizes 2–3 related articles into a
  Meridian-original brief
- Image suggestion via Unsplash API based on extracted entities
- Weekly digest generator for the Editorial Weekly newsletter

---

## License

Internal tool — not currently licensed for external use.

---

## Maintainers

TBD
