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
if not MAX_LINKS:
    raise SystemExit("MAX_LINKS is not set")