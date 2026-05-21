# career-news-agent

> An editorial-grade content sourcing pipeline for [community web name](webLink) —
> a curated career community for international students.

This is a standalone tool that discovers, filters, summarizes and queues
high-signal articles for the **Insights** section of the [name to be defined] Careers
website. It is **not** a generic web crawler. It is designed as a
semi-automated *editorial assistant* that respects publishers, prefers
quality over volume, and always keeps a human in the loop.

---

## Why this exists

[name to be defined]'s audience is international students targeting roles at top-tier
banks, consultancies and tech firms. The Insights board therefore needs
content that is:

- **Timely** — hiring signals, comp data, visa policy shifts
- **Specific** — written for graduates pursuing global careers
- **Trustworthy** — sourced from named publications, never scraped wholesale

Hand-curating this every week is expensive. This agent automates the
*sourcing and triage* layer so the editorial team only spends time on the
last 20% — review, voice, and publishing.

---

## Architecture (MVP)

```
┌──────────────┐   ┌───────────┐   ┌─────────────────┐   ┌────────┐
│ sources.yaml │──▶│ Firecrawl │──▶│  LLM Pipeline   │──▶│ Notion │
│  (whitelist) │   │  /scrape  │   │ score → summary │   │   DB   │
└──────────────┘   └───────────┘   └─────────────────┘   └────────┘
                          │                                   │
                          ▼                                   ▼
                  ┌──────────────┐                     ┌─────────────┐
                  │ local SQLite │                     │  Editorial  │
                  │ (dedupe log) │                     │   review    │
                  └──────────────┘                     └─────────────┘
```

**Stage 1 — Discovery.** Sources are declared in `sources.yaml`. Each entry
is either an RSS feed, a sitemap URL, or a Firecrawl `/search` query.

**Stage 2 — Fetch.** New URLs are fetched via Firecrawl `/scrape` and
returned as clean Markdown. RSS-only sources skip Firecrawl entirely.

**Stage 3 — Dedupe.** A canonical-URL hash and a content fingerprint are
checked against a local SQLite log. Anything seen before is dropped.

**Stage 4 — LLM triage (two passes).**

1. **Relevance scoring** (`gpt-4o-mini`) — outputs a 0–10 score and a
   short rationale. Items below the threshold are discarded.
2. **Editorial pass** (`gpt-4o` or equivalent) — produces a tag
   (`MARKET` / `MENTOR` / `REPORT`), a bilingual headline (zh + en) and a
   bilingual excerpt of ≤ 200 characters each.

**Stage 5 — Publish to Notion.** Survivors are written to a Notion
database with `Status = Pending Review`. Editors approve or reject inside
Notion.

> The Notion database is currently the **single source of truth**. A future
> milestone introduces a Postgres mirror so that the main website can
> consume content via a dedicated API instead of polling Notion.

---

## Tech stack

| Concern        | Choice                                          |
| -------------- | ----------------------------------------------- |
| Scraping       | [Firecrawl](https://firecrawl.dev) `/scrape`, `/search` |
| RSS fallback   | `feedparser`                                    |
| LLM gateway    | `litellm` (OpenAI / Anthropic / DeepSeek interchangeable) |
| Storage (MVP)  | SQLite via `sqlite-utils`                       |
| Review UI      | Notion (managed via `notion-client`)            |
| Scheduling     | `APScheduler` (cron-compatible)                 |
| Validation     | `pydantic`                                      |
| Logging        | `loguru`                                        |
| Config         | `pyyaml` + `python-dotenv`                      |

Python ≥ 3.11 is required.

---

## Project layout

```
meridian-news-agent/
├── sources/
│   └── sources.yaml          # Whitelist of feeds, sitemaps, search queries
├── meridian_agent/
│   ├── crawler/              # Firecrawl + RSS adapters
│   ├── pipeline/
│   │   ├── dedupe.py
│   │   ├── score.py          # LLM relevance pass
│   │   ├── editorial.py      # Bilingual headline + excerpt + tag
│   │   └── runner.py         # Orchestration
│   ├── storage/
│   │   ├── sqlite_log.py     # Dedupe + run history
│   │   └── notion_client.py  # Notion writer (idempotent upsert)
│   ├── scheduler/
│   └── settings.py
├── scripts/
│   ├── run_once.py           # Manual one-shot run
│   └── seed_notion_db.py     # Bootstrap a new Notion database schema
├── tests/
├── .env.example
├── pyproject.toml
└── README.md
```

---

## Notion database schema

The schema is intentionally aligned with the card fields rendered on the
Meridian website (`card-title`, `card-excerpt`, `tag`, `card-meta`,
`card-footer`), so future website integration is a straight mapping.

| Property           | Type           | Purpose                                        |
| ------------------ | -------------- | ---------------------------------------------- |
| `Title`            | Title          | Chinese headline (rewritten by LLM)            |
| `Title EN`         | Rich text      | English headline                               |
| `Excerpt ZH`       | Rich text      | ≤ 200 chars                                    |
| `Excerpt EN`       | Rich text      | ≤ 200 chars                                    |
| `Tag`              | Select         | `MARKET` / `MENTOR` / `REPORT`                 |
| `Status`           | Status         | `Draft` → `Pending Review` → `Approved` → `Rejected` → `Published` |
| `Relevance Score`  | Number         | 0–10, used to sort the review queue            |
| `Source URL`       | URL            | Canonical link to the original article         |
| `Source Name`      | Select         | Publisher (e.g. *eFinancialCareers*)           |
| `Author`           | Rich text      | Original byline                                |
| `Published At`     | Date           | Original publication time                      |
| `Scraped At`       | Date           | Time this row was created                      |
| `Read Minutes`     | Number         | Estimated reading time                         |
| `Cover Image`      | Files & media  | External URL (no direct uploads)               |
| `Content Hash`     | Rich text      | SHA-1 fingerprint for dedupe (hidden)          |
| `Raw Markdown`     | Rich text      | Original scraped Markdown (hidden, replayable) |
| `Notes`            | Rich text      | Editorial annotations                          |

A bootstrap script (`scripts/seed_notion_db.py`) creates this schema in a
target workspace.

---

## Configuration

Copy `.env.example` to `.env` and fill in:

```
FIRECRAWL_API_KEY=...
OPENAI_API_KEY=...                 # or ANTHROPIC_API_KEY / DEEPSEEK_API_KEY
NOTION_API_KEY=...
NOTION_DATABASE_ID=...
LLM_MODEL_SCORE=gpt-4o-mini
LLM_MODEL_EDITORIAL=gpt-4o
RELEVANCE_THRESHOLD=6
RUN_INTERVAL_MINUTES=360
```

Sources are declared in `sources/sources.yaml`:

```yaml
- name: eFinancialCareers
  type: rss
  url: https://www.efinancialcareers.com/news/feed
  default_tag: MARKET

- name: Goldman Sachs Careers Blog
  type: scrape_index
  url: https://www.goldmansachs.com/careers/blog
  default_tag: MENTOR

- name: H1B & OPT policy watch
  type: search
  query: '"H1B" OR "OPT" policy update 2026'
  default_tag: REPORT
```

---

## Running

```bash
pip install -e .
python scripts/seed_notion_db.py        # one-time setup
python scripts/run_once.py              # manual run
python -m meridian_agent.scheduler      # long-running scheduled mode
```

A typical run prints:

```
[fetch]   sources=12  new_urls=43
[dedupe]  remaining=29
[score]   passed=11  dropped=18  threshold=6
[editorial] generated=11
[notion]  upserted=11  skipped=0
```

---

## Compliance & ethics

This project is built around a hard rule: **we summarize, we never
republish**.

- `robots.txt` is always respected (Firecrawl enforces this by default).
- Whitelist-only — no opportunistic crawling of arbitrary domains.
- Excerpts are LLM-rewritten and capped at ~200 characters; the full
  article is **never** stored on the public website.
- Every published card links to the original source with attribution.
- Paywalled publications (WSJ, FT, Bloomberg, etc.) are limited to
  metadata + outbound links — no paywall circumvention, ever.
- The crawler identifies itself with a descriptive `User-Agent` and a
  contact email so publishers can reach us.

If you are a publisher and would like us to stop ingesting your content,
email `editorial@meridian.careers` and we will remove the source within
one business day.

---

## Roadmap

**v0.1 — MVP (current)**
- [x] Whitelist + Firecrawl
- [x] Two-pass LLM pipeline
- [x] Notion as single source of truth
- [x] Manual review workflow

**v0.2 — Hardening**
- [ ] Semantic deduplication via embeddings (`text-embedding-3-small`)
- [ ] Per-source success/failure dashboards
- [ ] Quarterly Notion archival job

**v0.3 — Website integration**
- [ ] Postgres mirror as the new source of truth
- [ ] Read-only `GET /api/news` endpoint consumed by the main site
- [ ] Auto-publish flow for `Approved` items

**v1.0 — Editorial agent**
- [ ] Background-research agent that synthesizes 2–3 related articles
      into a Meridian-original brief
- [ ] Image suggestion via Unsplash API based on extracted entities
- [ ] Weekly digest generator for the Editorial Weekly newsletter

---

## License

Internal tool — not currently licensed for external use.

---

## Maintainers

Meridian Careers — Editorial Engineering
`editorial@meridian.careers`
```
