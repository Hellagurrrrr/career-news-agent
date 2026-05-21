from datetime import datetime

from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage

from pydantic import BaseModel, Field

from llm import get_base_model

class Output(BaseModel):
    title_zh: str = Field(
            description="The title of the article in Chinese"
        )
    title_en: str = Field(
        description="The title of the article in English"
        )
    excerpt_zh: str = Field(
        description="The excerpt of the article in Chinese, around 200 characters"
        )
    excerpt_en: str = Field(
        description="The excerpt of the article in English, around 200 characters"
        )
    tag: str = Field(
        description="The tag of the article"
        )
    status: str = Field(
        description="The status of the article"
        )
    quality_score: float = Field(
        description="The quality score of the article"
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
    city: str = Field(
        description="The city of the article"
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
    notes: str = Field(
        description="The notes of the article"
        )


SYSTEM_PROMPT = """You are a helpful assistant that processes a raw scraped
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
- The quality score is between 0 and 10 (10 is highest).
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
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=_build_user_message(
            article_url, source_name, default_tag, raw_article,
        )),
    ]
    return wrapper_model.invoke(messages)