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

## Notion database schema

The schema is intentionally aligned with the card fields rendered on the
Meridian website (`card-title`, `card-excerpt`, `tag`, `card-meta`,
`card-footer`), so future website integration is a straight mapping.


| Property          | Type          | Purpose                                                                                                                                                                                                             |
| ----------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Title ZH`        | Title         | Chinese headline (rewritten by LLM)                                                                                                                                                                                 |
| `Title EN`        | Rich text     | English headline                                                                                                                                                                                                    |
| `Excerpt ZH`      | Rich text     | ≤ 200 chars                                                                                                                                                                                                         |
| `Excerpt EN`      | Rich text     | ≤ 200 chars                                                                                                                                                                                                         |
| `Tag`             | Select        | `MARKET` / `MENTOR` / `REPORT` / `VISA` / `CITY`                                                                                                                                                                    |
| `Status`          | Status        | `Pending Review` → `Approved` → `Rejected` → `Published`                                                                                                                                                            |
| `Relevance Score` | Number        | 0–10, used to sort the review queue                                                                                                                                                                                 |
| `Source URL`      | URL           | Canonical link to the original article                                                                                                                                                                              |
| `Source Name`     | Select        | Publisher (e.g. *eFinancialCareers*)                                                                                                                                                                                |
| `Author`          | Rich text     | Original byline                                                                                                                                                                                                     |
| `Country`         | Multi-select  | Countries this article is relevant to. Used for geo-based filtering of the Insights feed. Constrained to a curated enum (e.g. United States, United Kingdom, Hong Kong SAR, Singapore, Mainland China, Global).     |
| `City`            | Multi-select  | Cities or financial hubs this article is relevant to. Aligned with Meridian's rotation cities (New York, London, Hong Kong, Singapore, Shanghai) plus editorially-added hubs. Leave empty for country-level pieces. |
| `Published At`    | Date          | Original publication time                                                                                                                                                                                           |
| `Scraped At`      | Date          | Time this row was created                                                                                                                                                                                           |
| `Read Minutes`    | Number        | Estimated reading time                                                                                                                                                                                              |
| `Cover Image`     | Files & media | External URL (no direct uploads)                                                                                                                                                                                    |
| `Content Hash`    | Rich text     | SHA-1 fingerprint for dedupe (hidden)                                                                                                                                                                               |
| `Raw Markdown`    | Rich text     | Original scraped Markdown (hidden, replayable)                                                                                                                                                                      |
| `Notes`           | Rich text     | Editorial annotations                                                                                                                                                                                               |

### Tag Clarification:

- `MARKET`: The "market conditions" of the recruitment market. For example, the recruitment pace of tech giants in Q2 and the segmented recruitment of hedge funds on campus, which belong to macro/meta-level industry trend observations.
- `MENTOR`: First-person experience articles from current mentors in the industry, such as letters from Goldman Sachs IBD and McKinsey EM, which focus on personal know-how.
- `REPORT`: Structured data reports, such as the "2026 White Paper on Job Seeking for International Students" and the "Comprehensive Report on Careers for Female International Students," emphasize sample size and research dimensions, and are research content produced in-house by the community.
- `VISA`:  Policy and compliance related content, such as new H-1B lottery rules.
- `CITY`: Life/job-seeking guides centered around individual work cities, such as London and New York. Focus on lifestyle/local information.

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

- Whitelist + Firecrawl
- Two-pass LLM pipeline
- Notion as single source of truth
- Manual review workflow

**v0.2 — Hardening**

- Semantic deduplication via embeddings (`text-embedding-3-small`)
- Per-source success/failure dashboards
- Quarterly Notion archival job

**v0.3 — Website integration**

- Postgres mirror as the new source of truth
- Read-only `GET /api/news` endpoint consumed by the main site
- Auto-publish flow for `Approved` items

**v1.0 — Editorial agent**

- Background-research agent that synthesizes 2–3 related articles
into a Meridian-original brief
- Image suggestion via Unsplash API based on extracted entities
- Weekly digest generator for the Editorial Weekly newsletter

---

## License

Internal tool — not currently licensed for external use.

---

## Maintainers

TBD