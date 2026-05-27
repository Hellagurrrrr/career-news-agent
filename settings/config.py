import os
from dotenv import load_dotenv

load_dotenv()

# get api keys
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
if not FIRECRAWL_API_KEY:
    raise SystemExit("FIRECRAWL_API_KEY is not set")

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
if not DEEPSEEK_API_KEY:
    raise SystemExit("DEEPSEEK_API_KEY is not set")

# get parameters
MAX_LINKS = int(os.getenv("MAX_LINKS", "5"))

# Persistent SQLite store for processed articles. Path is relative to the
# project root unless overridden via env. We deliberately keep it OUTSIDE
# the per-run output/ directory because it accumulates across runs.
DB_PATH = os.getenv("DB_PATH", "articles.db")

# Notion sync (optional). The local SQLite store is always the source of
# truth; Notion is a mirror for human review + admin UI. When either of
# these is missing we just skip Notion calls everywhere -- the pipeline
# still runs end-to-end locally.
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
if not NOTION_TOKEN:
    raise SystemExit("NOTION_TOKEN is not set")
NOTION_DB_ID = os.getenv("NOTION_DB_ID")
# Notion API 2025-09-03 split each database into a container + one or more
# child "data sources". Reads/writes against rows now target the data
# source, not the database. For single-source DBs (the common case) we
# auto-resolve this from NOTION_DB_ID; set NOTION_DS_ID explicitly only if
# your DB has multiple data sources and you need to pin a specific one.
NOTION_DS_ID = os.getenv("NOTION_DS_ID")
NOTION_SYNC_ENABLED = bool(NOTION_TOKEN and NOTION_DB_ID)

# Concurrency knobs for the I/O-bound pipeline stages. All external calls
# (Firecrawl /map, Firecrawl /scrape, DeepSeek LLM) are network-bound, so a
# small thread pool gives a big speedup. Tune these via env vars if Firecrawl
# or DeepSeek start returning 429s.
MAP_MAX_WORKERS = int(os.getenv("MAP_MAX_WORKERS", "4"))
SCRAPE_MAX_WORKERS = int(os.getenv("SCRAPE_MAX_WORKERS", "8"))
LLM_MAX_WORKERS = int(os.getenv("LLM_MAX_WORKERS", "6"))

# score criteria for scoring agent node 2
SCORE_CRITERIA = """
relevance_score rubric (0-10):
- 9-10: Directly useful for the audience and focused on one or more core
  topics: career paths and success stories; internships, jobs, graduate
  programs, or recruiting at well-known organizations; salary, compensation,
  hiring, or labor market trends; industry trends with clear career
  implications; visa, work authorization, compliance, or immigration policy
  for international students and global talent.
- 7-8: Clearly career-related but slightly indirect, niche, or missing
  practical details. Includes company, education, technology, business, or
  policy news that has meaningful implications for career decisions.
- 5-6: Some career relevance, but the connection is broad, speculative, or
  only a minor part of the article.
- 3-4: Mostly general news, company updates, opinion, or lifestyle content
  with weak career implications.
- 0-2: Not relevant to careers, education, employment, compensation, industry
  direction, or visa/work policy.

quality_score rubric (0-10):
- 9-10: Substantive, well-sourced, current, and specific. Provides concrete
  facts, data, examples, quotes, or analysis; explains context and
  implications; is clearly written and not promotional.
- 7-8: Useful and credible with adequate detail, but may lack depth, multiple
  sources, original analysis, or actionable takeaways.
- 5-6: Understandable but thin, generic, lightly sourced, or mostly a summary
  of obvious information.
- 3-4: Low-detail, poorly structured, unclear, outdated, overly promotional,
  or missing important context.
- 0-2: Spam, duplicate boilerplate, broken scrape, non-article content, or
  too little meaningful content to evaluate.
  """