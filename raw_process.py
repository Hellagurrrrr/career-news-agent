from datetime import datetime

from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage

from pydantic import BaseModel, Field

from llm import get_base_model

SCORE_SNIPPET_CHARS = 6000

class Output(BaseModel):
    title_zh: str = Field(
            description="The title of the article in Chinese"
        )
    title_en: str = Field(
        description="The title of the article in English"
        )
    excerpt_zh: str = Field(
        description="The excerpt of the article in Chinese, around 500 characters"
        )
    excerpt_en: str = Field(
        description="The excerpt of the article in English, around 500 characters"
        )
    tag: str = Field(
        description="The tag of the article"
        )
    status: str = Field(
        description="The status of the article"
        )
    source_url: str = Field(
        description="The source url of the article"
        )
    author: str = Field(
        description="The author of the article"
        )
    country: str = Field(
        description="The country of the article"
        )
    published_at: str = Field(
        description="The published time of the article"
        )
    scraped_at: str = Field(
        description="The scraped time of the article"
        )
    read_minutes: int = Field(
        description="The read minutes of the article"
        )
    raw_markdown: str = Field(
        description="The raw markdown of the article"
        )
    notes: str = Field(
        description="The notes of the article"
        )

class Score(BaseModel):
    relevance_score: int = Field(
        ge=0, le=10,
        description="Relevance score, 0-10 inclusive"
        )
    quality_score: int = Field(
        ge=0, le=10,
        description="Quality score, 0-10 inclusive"
        )
    overall_score: int = Field(
        ge=0, le=10,
        description="Overall score, 0-10 inclusive"
        )
    reason: str = Field(
        min_length=100, max_length=800,
        description="The reason for the score, around 50-300 characters"
        )

    @property
    def overall_score(self) -> int:
        return int((self.relevance_score * 0.7 + self.quality_score * 0.3) / 2)

SCORE_SYSTEM_PROMPT = f"""You are an editorial scoring assistant for a career-news
curation pipeline. Score each article for relevance and quality, then return
integer scores from 0 to 10 plus a concise reason.

Audience:
- Chinese-speaking international students and early-career professionals, especially people
  interested in global careers, overseas study, internships, full-time jobs,
  career planning, compensation, industry trends, and work/visa policies.

Relevance Score rubric:
- 9-10: Directly useful for the audience and focused on one or more core topics:
  career paths and success stories; internships, jobs, graduate programs, or
  recruiting at well-known organizations; salary, compensation, hiring, or labor
  market trends; industry trends with clear career implications; visa, work
  authorization, compliance, or immigration policy for international students
  and global talent.
- 7-8: Clearly career-related but slightly indirect, niche, or missing practical
  details. Includes company, education, technology, business, or policy news
  that has meaningful implications for career decisions.
- 5-6: Some career relevance, but the connection is broad, speculative, or only
  a minor part of the article.
- 3-4: Mostly general news, company updates, opinion, or lifestyle content with
  weak career implications.
- 0-2: Not relevant to careers, education, employment, compensation, industry
  direction, or visa/work policy.

Quality Score rubric:
- 9-10: Substantive, well-sourced, current, and specific. Provides concrete
  facts, data, examples, quotes, or analysis; explains context and implications;
  is clearly written and not promotional.
- 7-8: Useful and credible with adequate detail, but may lack depth, multiple
  sources, original analysis, or actionable takeaways.
- 5-6: Understandable but thin, generic, lightly sourced, or mostly a summary of
  obvious information.
- 3-4: Low-detail, poorly structured, unclear, outdated, overly promotional, or
  missing important context.
- 0-2: Spam, duplicate boilerplate, broken scrape, non-article content, or too
  little meaningful content to evaluate.

Overall Score rubric:
- Calculate the weighted score as relevance_score * 0.7 + quality_score * 0.3.
- Round to the nearest integer from 0 to 10.
- Do not give an overall_score above 6 if relevance_score is 5 or lower.
- Do not give an overall_score above 7 if quality_score is 4 or lower.

Scoring rules:
- Use the full 0-10 range when justified; do not cluster all articles around 7.
- Reward practical, timely, decision-useful content over vague inspiration.
- Penalize articles that are merely announcements without career implications.
- Penalize clickbait, unsupported claims, obvious SEO filler, or scraped pages
  with missing main content.
- Base scores only on the provided article content and metadata. Do not assume
  facts that are not present.
- In the reason field, write 100-800 characters explaining the main factors
  behind the scores, including both relevance and quality, and the word count
  should NEVER exceed 800 characters.
"""


PROCESSOR_SYSTEM_PROMPT = """You are a helpful assistant that processes a raw scraped
article (provided as Markdown) and returns a structured output.

The user message uses XML-ish tags to separate pipeline-provided context
from the article body. The following fields MUST be copied verbatim into
the output -- do NOT re-derive them from the article body:
  - source_url : copy the value inside <SOURCE_URL>...</SOURCE_URL>
  - tag        : if a more specific tag cannot be confidently inferred
                 from the article, fall back to the value inside
                 <DEFAULT_TAG>...</DEFAULT_TAG>

The <SOURCE_NAME> tag tells you which publication the article is from --
use it as context when judging quality, country, and city, but never
fabricate information that is not in the article itself.

Rules:
- Do not invent information that is not in the article.
- Always translate the title and excerpt into BOTH English and Chinese.
- Leave fields blank ("" or 0) if you cannot infer them from the article.
"""


def _build_user_message(
    article_url: str,
    source_name: str,
    default_tag: str,
    raw_article: str,
) -> str:
    """Pack pipeline metadata and the raw markdown into one user message."""
    return (
        f"<SOURCE_URL>{article_url}</SOURCE_URL>\n"
        f"<SOURCE_NAME>{source_name}</SOURCE_NAME>\n"
        f"<DEFAULT_TAG>{default_tag}</DEFAULT_TAG>\n\n"
        f"<ARTICLE_MARKDOWN>\n{raw_article}\n</ARTICLE_MARKDOWN>"
    )


@tool
def get_current_timestamp() -> str:
    '''
    Get the current timestamp
    '''
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def score_raw_article(
    article_url: str,
    raw_article: str,
    source_name: str = "",
    default_tag: str = "",
) -> Score:
    """Score a raw scraped article

    Args:
        article_url:  The URL the markdown was scraped from. Echoed into
            Output.source_url so the LLM does not have to guess it.
        source_name:  Display name of the publication (from YAML). Used
            only as context in the prompt.
        default_tag:  Fallback tag (from YAML). Used when the LLM cannot
            confidently pick a more specific tag.
        raw_article:  The Markdown body returned by Firecrawl.
    """
    snippet = raw_article[:SCORE_SNIPPET_CHARS]

    model = get_base_model()
    wrapper_model = model.with_structured_output(Score)
    messages = [
        SystemMessage(content=SCORE_SYSTEM_PROMPT),
        HumanMessage(content=_build_user_message(
            article_url, source_name, default_tag, snippet,
        )),
    ]
    return wrapper_model.invoke(messages)


def process_raw_article(
    article_url: str,
    raw_article: str,
    source_name: str = "",
    default_tag: str = "",
) -> Output:
    """Run the LLM extraction stage on one scraped article.

    Args:
        article_url:  The URL the markdown was scraped from. Echoed into
            Output.source_url so the LLM does not have to guess it.
        raw_article:  The Markdown body returned by Firecrawl.
        source_name:  Display name of the publication (from YAML). Used
            only as context in the prompt.
        default_tag:  Fallback tag (from YAML). Used when the LLM cannot
            confidently pick a more specific tag.
    """
    model = get_base_model()
    wrapper_model = model.with_structured_output(Output)
    messages = [
        SystemMessage(content=PROCESSOR_SYSTEM_PROMPT),
        HumanMessage(content=_build_user_message(
            article_url, source_name, default_tag, raw_article,
        )),
    ]
    return wrapper_model.invoke(messages)