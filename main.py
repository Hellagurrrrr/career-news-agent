import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from settings.config import MAX_LINKS, SCORE_THRESHOLD
from crawler import map_sources_from_yaml, scrape_links_to_table
from raw_process import process_raw_article, score_raw_article

if __name__ == "__main__":
    run_output_dir = Path("output") / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_output_dir.mkdir(parents=True, exist_ok=True)

    # Demo pipeline: yaml -> map_sources_from_yaml -> scrape_links_to_table.
    # Note: each source fires up to `limit` /scrape calls -- mind the Firecrawl quota.
    map_results = map_sources_from_yaml(limit=MAX_LINKS)

    dfs = [scrape_links_to_table(r) for r in map_results]
    df = (
        pd.concat(dfs, ignore_index=True)
        if dfs
        else pd.DataFrame(columns=[
            "source_name",
            "source_url",
            "default_tag",
            "article_url",
            "markdown",
        ])
    )

    # P1.9: drop rows whose scrape failed or returned an empty body so the
    # LLM stage never sees a None/"" markdown.
    df = df.dropna(subset=["markdown"])
    df = df[df["markdown"].astype(str).str.len() > 0].reset_index(drop=True)

    print(f"\nfinal table shape: {df.shape}")
    print(df.head())

    score_rows = []
    outputs = []

    for _, row in df.iterrows():
        print(f"Scoring article: {row['article_url']}")

        try:
            score = score_raw_article(
                article_url=row["article_url"],
                source_name=row["source_name"],
                default_tag=row["default_tag"],
                raw_article=row["markdown"],
            )
        except Exception as e:
            print(f"[score failed] {row['article_url']}: {e}")
            score_rows.append({
                "article_url":     row["article_url"],
                "relevance_score": None,
                "quality_score":   None,
                "overall_score":   None,
                "reason":          f"ERROR: {e}",
                "raw_markdown":    row["markdown"],
            })
            continue

        print("================================================")
        print(score.model_dump_json(indent=2))
        print("================================================")

        score_rows.append({
            "article_url": row["article_url"],
            "relevance_score": score.relevance_score,
            "quality_score": score.quality_score,
            "overall_score": score.overall_score,
            "reason": score.reason,
            "raw_markdown": row["markdown"],
        })

        if score.overall_score < SCORE_THRESHOLD:
            continue

        output = process_raw_article(
            article_url=row["article_url"],
            raw_article=row["markdown"],
            source_name=row["source_name"],
            default_tag=row["default_tag"],
        )
        print("================================================")
        print(output.model_dump_json(indent=2))
        print("================================================")

        outputs.append(output.model_dump(mode="json"))

    scores_path = run_output_dir / "score_rows.json"
    outputs_path = run_output_dir / "outputs.json"

    scores_path.write_text(
        json.dumps(score_rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    outputs_path.write_text(
        json.dumps(outputs, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Saved score rows to: {scores_path}")
    print(f"Saved processed outputs to: {outputs_path}")