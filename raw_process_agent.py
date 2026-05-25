import operator
from typing import Annotated, TypedDict

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from llm import get_base_model
from settings.config import SCORE_CRITERIA


# --------------------- Agent State ---------------------
class AgentState(TypedDict, total=False):
    """State for the raw-article processing agent."""

    # === input (passed by invoker) ===
    article_url: str
    source_name: str
    raw_markdown: str
    scraped_at: str

    # === extract node output ===
    title_zh: str
    title_en: str
    excerpt_zh: str
    excerpt_en: str
    author: str
    country: str
    published_at: str
    read_minutes: int
    notes: str

    # === score node output ===
    relevance_score: int
    quality_score: int
    overall_score: int
    reason: str

    # === review node output ===
    needs_revision: bool
    review_notes: str

    # === meta info ===
    # operator.add reducer lets each node append errors without overwriting
    # entries written by earlier nodes.
    stage_errors: Annotated[list[dict], operator.add]


# --------------------- Node 1: Extraction ---------------------
class Extraction(BaseModel):
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
    author: str = Field(
        description="The author of the article"
        )
    country: str = Field(
        description="The country of the article"
        )
    published_at: str = Field(
        description="The published time of the article"
        )
    read_minutes: int = Field(
        description="The estimated reading time of the article in minutes"
    )
    notes: str = Field(
        description="Any notes you want to add to the article"
        )


EXTRACT_SYSTEM_PROMPT = """You are an expert at extracting information from a given article.
You will be given an article (as Markdown) plus some pipeline-provided metadata
in XML-ish tags. Extract the following structured fields:
- Title in Chinese
- Title in English
- Excerpt in Chinese (around 500 characters)
- Excerpt in English (around 500 characters)
- Author: the author of the article
- Country: the country the article is about / from
- Published At: the published time of the article
- Read Minutes: the estimated reading time of the article in minutes
- Notes: any notes you want to add about the article

Rules:
- Do not invent information that is not in the article.
- Leave fields blank ("" or 0) if you cannot infer them from the article.
- Translate the title and excerpt into BOTH Chinese and English regardless of
  the source language.
"""


async def extract(state: AgentState, config: RunnableConfig) -> dict:
    """Pull structured fields out of the raw markdown."""
    user_msg = (
        f"<SOURCE_URL>{state['article_url']}</SOURCE_URL>\n"
        f"<SOURCE_NAME>{state.get('source_name', '')}</SOURCE_NAME>\n"
        f"<SCRAPED_AT>{state.get('scraped_at', '')}</SCRAPED_AT>\n\n"
        f"<ARTICLE_MARKDOWN>\n{state['raw_markdown']}\n</ARTICLE_MARKDOWN>"
    )

    model = get_base_model().with_structured_output(Extraction)
    try:
        result: Extraction = await model.ainvoke(
            [
                SystemMessage(content=EXTRACT_SYSTEM_PROMPT),
                HumanMessage(content=user_msg),
            ],
            config=config,
        )
    except Exception as exc:
        return {"stage_errors": [{"node": "extract", "error": str(exc)}]}

    return result.model_dump()


# --------------------- Node 2: Scoring ---------------------
class Score(BaseModel):
    relevance_score: int = Field(
        ge=0, le=10, description="Relevance score, 0-10 inclusive"
    )
    quality_score: int = Field(
        ge=0, le=10, description="Quality score, 0-10 inclusive"
    )
    reason: str = Field(
        description="The reason for the score, concise and to the point."
    )


SCORE_SYSTEM_PROMPT = f"""You are an expert at scoring an article based on the following criteria:

Audience:
- Chinese-speaking international students and early-career professionals,
  especially people interested in global careers, overseas study, internships,
  full-time jobs, career planning, compensation, industry trends, and
  work/visa policies.

Scoring criteria:
{SCORE_CRITERIA}

Scoring discipline:
- Score relevance_score and quality_score INDEPENDENTLY -- a high-quality
  article on an off-topic subject should still get a low relevance_score,
  and a thin article on a perfectly on-topic subject should still get a low
  quality_score.
- Use the full 0-10 range when justified; do not cluster every article
  around 7.
- Base scores only on the provided article content and metadata. Do not
  assume facts that are not present.
- The reason should be concise and to the point.
"""


async def score(state: AgentState, config: RunnableConfig) -> dict:
    """Score relevance + quality, then derive overall_score deterministically."""
    user_msg = (
        f"<SOURCE_URL>{state['article_url']}</SOURCE_URL>\n\n"
        f"<ARTICLE_MARKDOWN>\n{state['raw_markdown']}\n</ARTICLE_MARKDOWN>"
    )

    model = get_base_model().with_structured_output(Score)
    try:
        result: Score = await model.ainvoke(
            [
                SystemMessage(content=SCORE_SYSTEM_PROMPT),
                HumanMessage(content=user_msg),
            ],
            config=config,
        )
    except Exception as exc:
        return {"stage_errors": [{"node": "score", "error": str(exc)}]}

    # The LLM is unreliable at applying weights, so compute overall_score
    # deterministically in Python.
    overall_score = round((result.relevance_score + result.quality_score) / 2)

    return {
        "relevance_score": result.relevance_score,
        "quality_score": result.quality_score,
        "overall_score": overall_score,
        "reason": result.reason,
    }


# --------------------- Node 3: Review ---------------------
class ReviewedExtraction(BaseModel):
    title_zh: str = Field(
        description="Reviewed/corrected title in Chinese"
        )
    title_en: str = Field(
        description="Reviewed/corrected title in English"
        )
    excerpt_zh: str = Field(
        description="Reviewed/corrected excerpt in Chinese, around 500 characters"
    )
    excerpt_en: str = Field(
        description="Reviewed/corrected excerpt in English, around 500 characters"
    )
    author: str = Field(
        description="Reviewed/corrected author"
        )
    country: str = Field(
        description="Reviewed/corrected country"
        )
    published_at: str = Field(
        description="Reviewed/corrected published time"
        )
    read_minutes: int = Field(
        description="Reviewed/corrected estimated reading time in minutes"
    )
    notes: str = Field(
        description="Reviewed/corrected notes"
        )
    needs_revision: bool = Field(
        description=(
            "True if you changed any field during review; "
            "false if every field was already accurate."
        )
    )
    review_notes: str = Field(
        description=(
            "A short Chinese note (1-3 sentences) describing what was changed "
            "and why. Leave as an empty string if needs_revision is false."
        )
    )


REVIEW_SYSTEM_PROMPT = """You are an editorial reviewer for a career-news pipeline.
You will be given:
1. The original article markdown (the source of truth).
2. A previously-extracted set of structured fields about that article.

Your job is to verify each field against the original article and produce a
corrected version using the same schema. For each field:
- Keep the existing value if it is accurate and well-formed.
- Replace it with a corrected value if it misrepresents the article, drops
  important information, or contains hallucinated content.
- The two translations (title and excerpt, zh/en) must remain faithful to the
  article and consistent with each other.

In addition:
- Set needs_revision to true if you changed any field; otherwise false.
- Write review_notes as a short Chinese note (1-3 sentences) describing what
  you changed and why. If needs_revision is false, leave review_notes as an
  empty string.

Rules:
- Do not invent information that is not in the article.
- Leave fields blank ("" or 0) if the article does not provide them, even if
  the previous extraction guessed a value.
"""


async def review(state: AgentState, config: RunnableConfig) -> dict:
    """Cross-check the extracted fields against the raw markdown and fix mistakes."""
    extracted_block = (
        f"title_zh: {state.get('title_zh', '')}\n"
        f"title_en: {state.get('title_en', '')}\n"
        f"excerpt_zh: {state.get('excerpt_zh', '')}\n"
        f"excerpt_en: {state.get('excerpt_en', '')}\n"
        f"author: {state.get('author', '')}\n"
        f"country: {state.get('country', '')}\n"
        f"published_at: {state.get('published_at', '')}\n"
        f"read_minutes: {state.get('read_minutes', 0)}\n"
        f"notes: {state.get('notes', '')}"
    )

    user_msg = (
        f"<SOURCE_URL>{state['article_url']}</SOURCE_URL>\n\n"
        f"<ARTICLE_MARKDOWN>\n{state['raw_markdown']}\n</ARTICLE_MARKDOWN>\n\n"
        f"<EXTRACTED_FIELDS>\n{extracted_block}\n</EXTRACTED_FIELDS>"
    )

    model = get_base_model().with_structured_output(ReviewedExtraction)
    try:
        result: ReviewedExtraction = await model.ainvoke(
            [
                SystemMessage(content=REVIEW_SYSTEM_PROMPT),
                HumanMessage(content=user_msg),
            ],
            config=config,
        )
    except Exception as exc:
        return {"stage_errors": [{"node": "review", "error": str(exc)}]}

    return result.model_dump()


# --------------------- Main graph ---------------------
agent = StateGraph(AgentState)
agent.add_node("extract", extract)
agent.add_node("score", score)
agent.add_node("review", review)

agent.set_entry_point("extract")
agent.add_edge("extract", "score")
agent.add_edge("score", "review")
agent.add_edge("review", END)

raw_process_agent = agent.compile()
raw_process_agent.name = "raw-process-agent"
